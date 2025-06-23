import logging
from zoneinfo import ZoneInfo

from django.db import models, transaction, IntegrityError
from django.utils import timezone
import lz4.frame as lz4
import numpy as np
import pandas as pd

from hostore.utils.timeseries import ts_combine_first

logger = logging.getLogger(__name__)


class TimeseriesChunkStore(models.Model):
    # Partitionnement temporel (null si pas de chunk)
    chunk_year  = models.IntegerField(null=True)
    chunk_month = models.IntegerField(null=True)

    # Métadonnées obligatoires
    start_ts = models.DateTimeField()        # premier timestamp inclus
    length   = models.IntegerField()         # nb. d’éléments
    tz       = models.CharField(max_length=48, default='UTC')
    dtype    = models.CharField(max_length=16)  # ex. 'float64'

    # Données brutes
    data = models.BinaryField()

    # Paramètres haut niveau
    CHUNK_AXIS = ('year', 'month')   # config. par classe enfant. configs : ('year',) ('year', 'month')
    STORE_TZ   = 'Europe/Paris'
    STORE_FREQ   = '1h'
    ITER_CHUNK_SIZE = 500

    class Meta:
        # les classes héritant de TimeseriesChunkStore doivent rajouter ['chunk_year', 'chunk_month'] au unique together
        unique_together = ['chunk_year', 'chunk_month']
        abstract = True

    # ------------------ Sérialisation bas niveau ------------------

    @staticmethod
    def _compress(serie: pd.Series) -> (bytes, np.array):
        arr = serie.to_numpy()
        return lz4.compress(arr.tobytes()), arr

    @classmethod
    def _decompress(cls, row) -> pd.Series:
        blob = row.data
        dtype = row.dtype
        raw = lz4.decompress(blob)
        arr = np.frombuffer(raw, dtype=dtype)
        idx = cls._rebuild_index(row, len(arr))
        return pd.Series(arr, index=idx)

    # -- public --
    @classmethod
    def set_ts(cls, attrs: dict, serie: pd.Series,
               update=False, replace=False):
        """
        Insère ou met à jour une série dense.
        attrs contient uniquement les clés métier (version/kind/...).
        """
        if update and replace:
            raise ValueError('update and replace are mutuellement exclusifs.')

        serie = cls._normalize_index(serie)

        if replace:
            # we need to delete previous related chunks (previous serie may lay on a greater span than new one)
            cls.objects.filter(**attrs).delete()

        # Découpage éventuel
        for sub in cls._chunk(serie):
            cls._upsert_chunk(attrs, sub, update, replace)

    @classmethod
    def get_ts(cls, attrs: dict,
               start=None, end=None, flat=True) -> pd.Series | dict:
        """
        Récupère et recompose la série.
        """
        qs = cls.objects.filter(**attrs)
        if start or end:
            qs = cls._filter_interval(qs, start, end)

        pieces = []
        for row in qs.iterator(chunk_size=cls.ITER_CHUNK_SIZE):
            pieces.append(cls._decompress(row))

        if not pieces:
            raise ValueError('Série introuvable.')

        full = pd.concat(pieces).sort_index()
        full.index = pd.to_datetime(full.index, utc=True)
        full = full.tz_convert(cls.STORE_TZ)

        if start or end:
            full = full.loc[start:end]

        return full if flat else [{'data': full, **attrs}]

    @classmethod
    def set_many_ts(cls, mapping: dict[tuple, pd.Series],
                    keys: tuple[str, ...]):
        """
        mapping : {(k1,k2,...): serie}
        keys    : ('version','kind',...)
        """
        rows = []
        for ktuple, serie in mapping.items():
            attrs = dict(zip(keys, ktuple))
            serie = cls._normalize_index(serie)
            for sub in cls._chunk(serie):
                rows.append(cls._build_row(attrs, sub))
        cls._bulk_upsert(rows)

    @classmethod
    def yield_ts(cls, filters: dict | None = None):
        qs = cls.objects.filter(**(filters or {})).iterator(
            chunk_size=cls.ITER_CHUNK_SIZE)
        for row in qs:
            yield cls._decompress(row), row

    # -- private helpers --

    @classmethod
    def _normalize_index(cls, serie: pd.Series) -> pd.Series:
        if not isinstance(serie.index, pd.DatetimeIndex):
            raise ValueError('Index doit être DatetimeIndex.')
        serie = serie.sort_index()
        if serie.index.tz is None:
            logger.warning('Saving serie without tz may lead to inconsistent results')
            serie = serie.tz_localize(cls.STORE_TZ)
        else:
            serie = serie.tz_convert(cls.STORE_TZ)
        if serie.isnull().all():
            raise ValueError('Série vide.')
        return serie

    @classmethod
    def _chunk(cls, serie: pd.Series):
        if serie.isnull().any():
            raise ValueError('Trying to chunk with nulls in serie')

        if not cls.CHUNK_AXIS:
            yield serie
            return
        grouper = serie.groupby([
            getattr(serie.index, ax) for ax in cls.CHUNK_AXIS
        ])
        for _, sub in grouper:
            yield sub

    @classmethod
    def _year_month(cls, serie):
        first_ts = serie.index[0].tz_convert(cls.STORE_TZ)
        year = first_ts.year if 'year' in cls.CHUNK_AXIS else None
        month = first_ts.month if 'month' in cls.CHUNK_AXIS else None
        return first_ts, year, month

    @classmethod
    def _build_row(cls, attrs, serie):
        if serie.isnull().any():
            raise ValueError('Trying to build row with nulls in serie')
        compressed, arr = cls._compress(serie)
        first_ts, year, month = cls._year_month(serie)
        return cls(
            **attrs,
            chunk_year=year,
            chunk_month=month,
            start_ts=first_ts,
            length=len(arr),
            tz=str(serie.index.tz),
            dtype=str(arr.dtype),
            data=compressed
        )

    @classmethod
    def _upsert_chunk(cls, attrs, serie, update, replace):
        row = cls._build_row(attrs, serie)
        attributes_fields = [*attrs.keys(), 'chunk_year', 'chunk_month']
        attributes = {f: getattr(row, f) for f in attributes_fields}

        if update:
            # combine first with existing
            qs = cls.objects.filter(**attributes)
            if qs.count() == 0:
                pass
            elif qs.count() == 1:
                row = qs.first()
                ds_existing = cls._decompress(row)
                ds_new = ts_combine_first([serie, ds_existing])
                row = cls._build_row(attrs, ds_new)
            else:
                raise ValueError(f'Multiple chunks found for attributes {attributes} with update={update}')

        # dict pour defaults
        defaults = {
            'start_ts': row.start_ts,
            'length': row.length,
            'tz': row.tz,
            'dtype': row.dtype,
            'data': row.data,
        }

        if update or replace:
            cls.objects.update_or_create(
                defaults=defaults,
                **attributes
            )
        else:
            # insertion stricte
            try:
                with transaction.atomic(using=cls.objects.db):
                    row.save()
            except IntegrityError:
                # collision => conseiller d’utiliser update=True ou replace=True
                raise ValueError(
                    f'Chunk déjà présent pour clés {attrs} '
                    f'({row.chunk_year}-{row.chunk_month})'
                )

    @classmethod
    def _bulk_upsert(cls, rows):
        if not rows:
            return

        with transaction.atomic():
            cls.objects.bulk_create(
                rows,
                batch_size=1000,  # optionnel : tuning
            )

    @classmethod
    def _rebuild_index(cls, row, length):
        start = row.start_ts.astimezone(ZoneInfo(row.tz))
        return pd.date_range(start=start,
                             periods=length,
                             freq=cls.STORE_FREQ)

    # @classmethod
    # def _filter_interval(cls, qs, start, end):
    #     if start:
    #         qs = qs.filter(start_ts__lte=end or start)
    #     if end:
    #         qs = qs.filter(start_ts__lte=end)
    #     return qs

    @classmethod
    def _filter_interval(cls, qs, start, end):
        """
        Retourne les chunks qui chevauchent [start, end] (inclus).
        start / end sont des str, datetime, ou None (timezone naïve = STORE_TZ).
        """
        if isinstance(start, str):
            start = pd.Timestamp(start, tz=cls.STORE_TZ).to_pydatetime()
        if isinstance(end, str):
            end = pd.Timestamp(end, tz=cls.STORE_TZ).to_pydatetime()

        # borne UTC pour comparaison directe avec start_ts
        start_utc = start.astimezone(ZoneInfo('UTC')) if start else None
        end_utc = end.astimezone(ZoneInfo('UTC')) if end else None

        # 1) filtre rapide par année/mois si chunking activé
        if cls.CHUNK_AXIS:
            if start and end and start.year == end.year:
                qs = qs.filter(chunk_year=start.year)
            elif start and end:
                qs = qs.filter(chunk_year__gte=start.year,
                               chunk_year__lte=end.year)
            elif start:
                qs = qs.filter(chunk_year__gte=start.year)
            elif end:
                qs = qs.filter(chunk_year__lte=end.year)

            if 'month' in cls.CHUNK_AXIS and start and end and start.year == end.year:
                if start.month == end.month:
                    qs = qs.filter(chunk_month=start.month)
                else:
                    qs = qs.filter(chunk_month__gte=start.month,
                                   chunk_month__lte=end.month)

        # 2) filtre précis sur start_ts / fin_ts
        if start_utc:
            qs = qs.filter(start_ts__lte=end_utc or start_utc)  # chunk commence avant fin
        if end_utc:
            # Un chunk se termine à start_ts + (length-1)*freq
            qs = qs.filter(
                start_ts__gte=start_utc or end_utc - pd.Timedelta(days=365 * 200)
                              |
                              Q(start_ts__lt=end_utc)
            )
        return qs

class TestTimeseriesChunkStoreWithAttributes(TimeseriesChunkStore):
    # Clé fonctionnelle définie par l’utilisateur
    version = models.IntegerField()
    kind    = models.CharField(max_length=100)

    class Meta:
        abstract = False
        unique_together = (
            'version', 'kind', 'chunk_year', 'chunk_month'
        )
        indexes = [
            models.Index(fields=['version', 'kind']),
            models.Index(fields=['chunk_year', 'chunk_month']),
            models.Index(fields=['start_ts']),  # pour between
        ]
