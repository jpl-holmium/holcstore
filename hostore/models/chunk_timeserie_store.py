import logging
from typing import Union
from zoneinfo import ZoneInfo

import pytz
from django.db import models, transaction
import lz4.frame as lz4
import numpy as np
import pandas as pd
from pytz.exceptions import UnknownTimeZoneError

logger = logging.getLogger(__name__)

# KEYS_ABSTRACT_CLASS = set([field.name for field in TimeseriesChunkStore._meta.get_fields()])
# KEYS_ABSTRACT_CLASS.add('id')
# TODO automatiser la génération de ces clef ? source d'erreur possible
KEYS_ABSTRACT_CLASS = {'id', 'start_ts', 'data', 'dtype', 'chunk_index'}

class TimeseriesChunkStore(models.Model):
    # Partitionnement temporel (null si pas de chunk)
    chunk_index = models.IntegerField()

    # Métadonnées obligatoires
    start_ts = models.DateTimeField()        # premier timestamp inclus
    dtype    = models.CharField(max_length=16)  # ex. 'float64'

    # Données brutes
    data = models.BinaryField()

    # Paramètres haut niveau
    _ALLOWED_CHUNK_AXIS = {('year',), ('year', 'month')}
    CHUNK_AXIS = ('year', 'month')   # Chunking axis for timeseries storage. Configs : ('year',) / ('year', 'month')
    STORE_TZ   = 'Europe/Paris' # Chunking timezone
    STORE_FREQ   = '1h' # Timeseries storage frequency.
    ITER_CHUNK_SIZE = 200
    BULK_CREATE_BATCH_SIZE = 200
    _model_keys = None

    class Meta:
        # les classes héritant de TimeseriesChunkStore doivent rajouter ['chunk_index'] au unique together
        unique_together = ['chunk_index']
        abstract = True

    @classmethod
    def get_model_keys(cls):
        if cls._model_keys is None:
            cls._model_keys = set([field.name for field in cls._meta.get_fields()]) - KEYS_ABSTRACT_CLASS
        return cls._model_keys

    # valeurs de chunk autorisées
    def __init_subclass__(cls, **kwargs):
        """Valide les options de classe au moment où la sous-classe est créée."""
        super().__init_subclass__(**kwargs)
        # validate CHUNK_AXIS
        if tuple(getattr(cls, "CHUNK_AXIS", ())) not in cls._ALLOWED_CHUNK_AXIS:
            raise ValueError(
                f"Invalid CHUNK_AXIS {cls.CHUNK_AXIS!r} for model {cls.__name__} — allowed: {cls._ALLOWED_CHUNK_AXIS}"
            )
        # validate STORE_FREQ
        try:
            pd.to_timedelta(cls.STORE_FREQ)
        except ValueError:
            raise ValueError(
                f"Invalid STORE_FREQ {cls.STORE_FREQ!r} for model {cls.__name__}"
            )
        # validate STORE_TZ
        try:
            pytz.timezone(cls.STORE_TZ)
        except UnknownTimeZoneError:
            raise ValueError(
                f"Invalid STORE_TZ {cls.STORE_TZ!r} for model {cls.__name__}"
            )
    # ------------------ Sérialisation bas niveau ------------------
    @staticmethod
    def _compress(serie: pd.Series) -> (bytes, np.array):
        arr = serie.to_numpy()
        return lz4.compress(arr.tobytes()), arr

    @classmethod
    def _decompress(cls, row, mem_view=False) -> pd.Series:
        blob = row.data
        dtype = row.dtype
        if mem_view:
            raw = memoryview(lz4.decompress(blob))
            arr = np.frombuffer(raw, dtype=dtype, like=np.empty(0, dtype=dtype))
        else:
            raw = lz4.decompress(blob)
            arr = np.frombuffer(raw, dtype=dtype)
        idx = cls._rebuild_index(row, len(arr))
        return pd.Series(arr, index=idx)

    # -- public --
    @classmethod
    def set_ts(cls, attrs: dict, serie: pd.Series,
               update=False, replace=False):
        """
        Set a timeseries.
        Warning:
            * if replace=False and update=False, trying to insert over existing attrs will raise a
          django.db.utils.IntegrityError
            * All available fields of model must exist in attrs keys

        Args:
            attrs: attributes dict
            serie: timeseries set
            update: if True, update any existing series in database with provided one (combine_first).
            replace: if True, replace existing series in database

        Returns:

        """
        if update and replace:
            raise ValueError('update and replace are mutually exclusive')
        cls._ensure_all_attrs_specified(attrs)
        update_or_replace = update or replace
        serie = cls._normalize_index(serie)
        if serie is None:
            return
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
    def get_ts(cls, attrs: dict, start: pd.Timestamp=None, end: pd.Timestamp=None) -> None | pd.Series:
        """
        Extract timeseries data matching attrs.
        Warning: all available fields of model must exist in attrs keys

        Args:
            attrs: filters of query
            start: start index of timeseries
            end: end index of timeseries

        Returns: timeseries if exists, None otherwise
        """
        cls._ensure_all_attrs_specified(attrs)
        qs = cls.objects.filter(**attrs).order_by('chunk_index')
        if start or end:
            qs = cls._filter_interval(qs, start, end)

        pieces = []
        for row in qs.iterator(chunk_size=cls.ITER_CHUNK_SIZE):
            pieces.append(cls._decompress(row))

        if not pieces:
            return None

        full = pd.concat(pieces)
        full.index = pd.to_datetime(full.index, utc=True)
        full = full.tz_convert(cls.STORE_TZ)

        if start or end:
            full = full.loc[start:end]

        return full

    @classmethod
    def set_many_ts(cls, mapping: dict[tuple, pd.Series], keys: tuple[str, ...]):
        """
        Set many timeseries at once. User must clear matching filters upstream.
        Warning:
            * Trying to insert over existing attrs will raise a django.db.utils.IntegrityError
            * All available fields of model must exist in attrs keys

        Args:
            mapping : {(version_value, kind_value,...): serie}
            keys    : ('version','kind',...)

        Returns:

        """
        rows = []
        for ktuple, serie in mapping.items():
            attrs = dict(zip(keys, ktuple))
            cls._ensure_all_attrs_specified(attrs)
            serie = cls._normalize_index(serie)
            if serie is None:
                continue
            for sub in cls._chunk(serie):
                rows.append(cls._build_row(attrs, sub))
        cls._bulk_upsert(rows)

    @classmethod
    def yield_many_ts(cls, attrs: dict, start: pd.Timestamp=None, end: pd.Timestamp=None):
        """
        Yield (serie, attrs_dict) for each available timeseries with filters matching attrs.
            - serie will be expressed at STORE_FREQ, STORE_TZ
            - attrs_dict : mapping {model_key: value}

        Args:
            attrs: filters of query
            start: start index of timeseries
            end: end index of timeseries
        """
        # On valide seulement les clés fournies
        bad = set(attrs) - cls.get_model_keys()
        if bad:
            raise ValueError(f"Unknown attribute(s) {bad}")

        qs = cls.objects.filter(**attrs).order_by(*(cls.get_model_keys()), 'chunk_index')
        if start or end:
            qs = cls._filter_interval(qs, start, end)

        current_values = None
        buffer = []

        def flush():
            if not buffer:
                return
            serie = pd.concat(buffer)
            if start or end:
                serie = serie.loc[start:end]
            key_dict = dict(zip(cls.get_model_keys(), current_values))
            yield serie, key_dict
            buffer.clear()

        for row in qs.iterator(chunk_size=cls.ITER_CHUNK_SIZE):
            values = tuple(getattr(row, k) for k in cls.get_model_keys())
            if current_values is None:
                current_values = values
            elif values != current_values:
                # nouvelle combinaison → on émet la série courante
                yield from flush()
                current_values = values
            buffer.append(cls._decompress(row))

        # flush final
        yield from flush()

    # -- private helpers --

    @classmethod
    def _normalize_index(cls, serie: pd.Series) -> Union[None, pd.Series]:
        """
        normalize index of serie by reindexing over chunking grid
        Args:
            serie: initial serie

        Returns: transformed serie
        """
        if not isinstance(serie.index, pd.DatetimeIndex):
            raise ValueError('Index doit être DatetimeIndex.')

        if serie.isnull().all():
            return None

        if serie.index.tz is None:
            logger.warning('Saving serie without tz may lead to inconsistent results')
            serie = serie.tz_localize(cls.STORE_TZ)
        else:
            serie = serie.tz_convert(cls.STORE_TZ)

        first = serie.index[0]
        last = serie.index[-1]
        if cls.CHUNK_AXIS == ('year',):
            start = first.replace(month=1, day=1, hour=0, minute=0)
            end   = last + pd.offsets.YearEnd() + pd.offsets.Day()
        else:  # ('year','month')
            start = first.replace(day=1, hour=0, minute=0)
            end   = last + pd.offsets.MonthEnd() + pd.offsets.Day()

        new_index = pd.date_range(start=start, end=end, inclusive='left',freq=cls.STORE_FREQ)
        serie = serie.reindex(new_index)

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
        """ Compute chunk_index value """
        if cls.CHUNK_AXIS == ('year',):
            return ts.year
        # else ('year','month')
        return ts.year * 12 + ts.month - 1

    @classmethod
    def _build_row(cls, attrs, serie):
        """
        Build db object row from serie
        serie must be chunked and normalized
        """
        compressed, arr = cls._compress(serie)
        first_ts = serie.index[0]
        return cls(
            **attrs,
            chunk_index=cls._chunk_index(first_ts),
            start_ts=first_ts,
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
                batch_size=cls.BULK_CREATE_BATCH_SIZE,  # optionnel : tuning
            )

    @classmethod
    def _rebuild_index(cls, row, length):
        start = row.start_ts.astimezone(ZoneInfo(cls.STORE_TZ))
        return pd.date_range(start=start,
                             periods=length,
                             freq=cls.STORE_FREQ)

    @classmethod
    def _filter_interval(cls, qs, start, end):
        if isinstance(start, str):
            start = pd.Timestamp(start, tz=cls.STORE_TZ)
        else:
            start = pd.Timestamp(start).tz_convert(cls.STORE_TZ)
        if isinstance(end, str):
            end = pd.Timestamp(end, tz=cls.STORE_TZ)
        else:
            end = pd.Timestamp(end).tz_convert(cls.STORE_TZ)

        if start:
            qs = qs.filter(chunk_index__gte=cls._chunk_index(start))
        if end:
            qs = qs.filter(chunk_index__lte=cls._chunk_index(end))
        return qs

    @classmethod
    def _ensure_all_attrs_specified(cls, attrs):
        """
        Vérifie que l'utilisateur a renseigné tous les attributs "métiers"
        """
        # # check version 2
        # check = qs.values('chunk_index').annotate(count=Count('id')).filter(count__gt=1)
        # if check.exists():
        #     raise ValueError

        # check version 1
        attrs_keys = set(attrs.keys())
        model_keys = cls.get_model_keys()
        if model_keys != attrs_keys:
            raise ValueError(f'Trying to set or get partial attributes {attrs} while full attributes list is {model_keys}')


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
