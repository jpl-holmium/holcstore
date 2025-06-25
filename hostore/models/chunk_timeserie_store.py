import logging
from zoneinfo import ZoneInfo

from django.db import models, transaction, IntegrityError
from django.utils import timezone
import lz4.frame as lz4
import numpy as np
import pandas as pd

from hostore.utils.timeseries import ts_combine_first

logger = logging.getLogger(__name__)


# todo vérifier les options utilisateur (par ex validité de CHUNK_AXIS) ? ou ?

# todo gérer le stockage de df ? prévoir des méthodes dédiées au transfert api ? (transférer les binary)

class TimeseriesChunkStore(models.Model):
    # Partitionnement temporel (null si pas de chunk)
    chunk_index = models.IntegerField()

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
        # les classes héritant de TimeseriesChunkStore doivent rajouter ['chunk_index'] au unique together
        unique_together = ['chunk_index']
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
               update=False, replace=False, safe_insertion=False):
        """
        Insère ou met à jour une série dense.
        attrs contient uniquement les clés métier (version/kind/...).
        """
        if update and replace:
            raise ValueError('update and replace are mutuellement exclusifs.')
        update_or_replace = update or replace
        serie = cls._normalize_index(serie, safe_insertion)

        if replace:
            # we need to delete previous related chunks (previous serie may lay on a greater span than new one)
            cls.objects.filter(**attrs).delete()

        # Enregistrement par chunk
        rows = []
        for sub in cls._chunk(serie):
            if update_or_replace:
                cls._upsert_chunk_update_or_replace(attrs, sub, update, replace)
            else:
                rows.append(cls._build_row(attrs, sub))

        if not update_or_replace:
            cls._bulk_upsert(rows)

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
                    keys: tuple[str, ...], safe_insertion=False):
        """
        mapping : {(k1,k2,...): serie}
        keys    : ('version','kind',...)
        """
        rows = []
        for ktuple, serie in mapping.items():
            attrs = dict(zip(keys, ktuple))
            serie = cls._normalize_index(serie, safe_insertion)
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
    def _normalize_index(cls, serie: pd.Series, safe_insertion: bool) -> pd.Series:
        if not isinstance(serie.index, pd.DatetimeIndex):
            raise ValueError('Index doit être DatetimeIndex.')
        if serie.index.tz is None:
            logger.warning('Saving serie without tz may lead to inconsistent results')
            serie = serie.tz_localize(cls.STORE_TZ)
        else:
            serie = serie.tz_convert(cls.STORE_TZ)

        if safe_insertion:
            new_index = pd.date_range(start=serie.index[0], end=serie.index[-1], freq=cls.STORE_FREQ)
            serie = serie.reindex(new_index)

        if serie.isnull().all():
            raise ValueError('Série vide.')
        return serie

    @classmethod
    def _chunk(cls, serie: pd.Series):
        if not cls.CHUNK_AXIS:
            yield serie
            return
        grouper = serie.groupby([
            getattr(serie.index, ax) for ax in cls.CHUNK_AXIS
        ])
        for _, sub in grouper:
            yield sub

    @classmethod
    def _chunk_index(cls, ts):
        if cls.CHUNK_AXIS == ('year',):
            return ts.year
        return ts.year * 12 + ts.month - 1

    @classmethod
    def _build_row(cls, attrs, serie):
        compressed, arr = cls._compress(serie)
        first_ts = serie.index[0].tz_convert(cls.STORE_TZ)
        return cls(
            **attrs,
            chunk_index=cls._chunk_index(first_ts),
            start_ts=first_ts,
            length=len(arr),
            tz=str(serie.index.tz),
            dtype=str(arr.dtype),
            data=compressed
        )

    @classmethod
    def _upsert_chunk_update_or_replace(cls, attrs, serie, update, replace):
        row = cls._build_row(attrs, serie)
        attributes_fields = [*attrs.keys(), 'chunk_index']
        attributes = {f: getattr(row, f) for f in attributes_fields}

        if update:
            # combine first with existing
            qs = cls.objects.filter(**attributes)
            if qs.count() == 0:
                pass
            elif qs.count() == 1:
                row = qs.first()
                ds_existing = cls._decompress(row)
                # tz_convert UTC : avoid nan insertion at october tz switch
                serie.index = serie.index.tz_convert('UTC')
                ds_existing.index = ds_existing.index.tz_convert('UTC')
                ds_new = serie.combine_first(ds_existing)
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

        cls.objects.update_or_create(
            defaults=defaults,
            **attributes
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

    @classmethod
    def _filter_interval(cls, qs, start, end):
        if isinstance(start, str):
            start = pd.Timestamp(start, tz=cls.STORE_TZ)
        if isinstance(end, str):
            end = pd.Timestamp(end, tz=cls.STORE_TZ)
        if start:
            qs = qs.filter(chunk_index__gte=cls._chunk_index(start))
        if end:
            qs = qs.filter(chunk_index__lte=cls._chunk_index(end))
        return qs

# class ExampleTimeseriesChunkStoreWithAttributes(TimeseriesChunkStore):
#     # Clé fonctionnelle définie par l’utilisateur
#     version = models.IntegerField()
#     kind    = models.CharField(max_length=100)
#
#     class Meta:
#         abstract = False
#         unique_together = (
#             'version', 'kind', 'chunk_year', 'chunk_month'
#         )
#         indexes = [
#             models.Index(fields=['version', 'kind']),
#             models.Index(fields=['chunk_year', 'chunk_month']),
#             models.Index(fields=['start_ts']),  # pour between
#         ]
