from django.db import models
from django.utils import timezone
import lz4.frame as lz4
import numpy as np
import pandas as pd


class TimeseriesChunkStore(models.Model):
    # Partitionnement temporel (null si pas de chunk)
    chunk_year  = models.IntegerField(null=True)
    chunk_month = models.IntegerField(null=True)

    # Métadonnées obligatoires
    start_ts = models.DateTimeField()        # premier timestamp inclus
    length   = models.IntegerField()         # nb. d’éléments
    freq     = models.CharField(max_length=8)  # ex. 'H', '5T'
    tz       = models.CharField(max_length=48, default='UTC')
    dtype    = models.CharField(max_length=16)  # ex. 'float64'

    # Données brutes
    data = models.BinaryField()

    # Paramètres haut niveau
    CHUNK_AXIS = ('year', 'month')   # config. par classe enfant
    CHUNK_TZ   = 'Europe/Paris'
    ITER_CHUNK_SIZE = 500

    class Meta:
        abstract = True

    # ------------------ Sérialisation bas niveau ------------------

    @staticmethod
    def _compress(arr: np.ndarray) -> bytes:
        return lz4.compress(arr.tobytes())

    @staticmethod
    def _decompress(blob: bytes, dtype: str) -> np.ndarray:
        raw = lz4.decompress(blob)
        return np.frombuffer(raw, dtype=dtype)

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
            arr = cls._decompress(row.data, row.dtype)
            idx = cls._rebuild_index(row, len(arr))
            pieces.append(pd.Series(arr, index=idx))

        if not pieces:
            raise ValueError('Série introuvable.')
        full = pd.concat(pieces).sort_index()

        if start or end:
            full = full.loc[start:end]

        return full if flat else [{'data': full, **attrs}]

    @classmethod
    def set_many_ts(cls, mapping: dict[tuple, pd.Series],
                    keys: tuple[str, ...],
                    update=False, replace=False):
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
        cls._bulk_upsert(rows, update, replace)

    @classmethod
    def yield_ts(cls, filters: dict | None = None):
        qs = cls.objects.filter(**(filters or {})).iterator(
            chunk_size=cls.ITER_CHUNK_SIZE)
        for row in qs:
            arr = cls._decompress(row.data, row.dtype)
            idx = cls._rebuild_index(row, len(arr))
            yield pd.Series(arr, index=idx), row

    # -- private helpers --

    @classmethod
    def _normalize_index(cls, serie: pd.Series) -> pd.Series:
        if not isinstance(serie.index, pd.DatetimeIndex):
            raise ValueError('Index doit être DatetimeIndex.')
        serie = serie.sort_index()
        if serie.index.tz is None:
            serie = serie.tz_localize(cls.CHUNK_TZ)
        else:
            serie = serie.tz_convert(cls.CHUNK_TZ)
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
    def _build_row(cls, attrs, serie):
        arr = serie.to_numpy()
        compressed = cls._compress(arr)
        first_ts = serie.index[0].tz_convert('UTC')
        year = first_ts.year if 'year' in cls.CHUNK_AXIS else None
        month = first_ts.month if 'month' in cls.CHUNK_AXIS else None
        return cls(
            **attrs,
            chunk_year=year,
            chunk_month=month,
            start_ts=first_ts,
            length=len(arr),
            freq=pd.infer_freq(serie.index),
            tz=str(serie.index.tz),
            dtype=str(arr.dtype),
            data=compressed
        )

    @classmethod
    def _upsert_chunk(cls, attrs, serie, update, replace):
        row = cls._build_row(attrs, serie)
        conflict_fields = ['version', 'kind', 'chunk_year', 'chunk_month']
        if update or replace:
            cls.objects.update_or_create(
                defaults=row.__dict__,
                **{f: getattr(row, f) for f in conflict_fields}
            )
        else:
            # insert‑only path
            row.save(force_insert=True)

    @classmethod
    def _bulk_upsert(cls, rows, update, replace):
        if not rows:
            return
        with cls.objects.db.transaction.atomic():
            cls.objects.bulk_create(
                rows, ignore_conflicts=not (update or replace))
            # Optionnel : gérer update via SQL ON CONFLICT DO UPDATE
            # selon le SGBD → out of scope

    @classmethod
    def _rebuild_index(cls, row, length):
        start = row.start_ts.astimezone(
            timezone.pytz.timezone(row.tz))  # Dst‑safe
        return pd.date_range(
            start=start, periods=length, freq=row.freq, tz=row.tz)

    @classmethod
    def _filter_interval(cls, qs, start, end):
        if start:
            qs = qs.filter(start_ts__lte=end or start)
        if end:
            qs = qs.filter(start_ts__lte=end)
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
