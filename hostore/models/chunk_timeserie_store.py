import datetime as dt
import logging
from typing import Union, List
from zoneinfo import ZoneInfo

import pytz
from django.db import models, transaction
import lz4.frame as lz4
import numpy as np
import pandas as pd
from django.db.models import QuerySet
from django.db.models.aggregates import Max
from pytz.exceptions import UnknownTimeZoneError
from hostore.utils.timeseries import _localise_date


logger = logging.getLogger(__name__)

# KEYS_ABSTRACT_CLASS = set([field.name for field in TimeseriesChunkStore._meta.get_fields()])
# KEYS_ABSTRACT_CLASS.add('id')
# TODO automatiser la génération de ces clef ? source d'erreur possible
KEYS_ABSTRACT_CLASS = {'id', 'start_ts', 'data', 'dtype', 'updated_at', 'chunk_index'}

class TimeseriesChunkStore(models.Model):
    # Partitionnement temporel (null si pas de chunk)
    chunk_index = models.IntegerField()

    # Métadonnées obligatoires
    start_ts = models.DateTimeField()        # premier timestamp inclus
    dtype    = models.CharField(max_length=16)  # ex. 'float64'
    updated_at = models.DateTimeField(auto_now=True)  # synchro

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
        unique_together = ['chunk_index']
        abstract = True

    @classmethod
    def get_model_keys(cls) -> List[str]:
        """ Returns the list of the keys added to the non abstract class """
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

    # ------------------------------------------------------------------
    # PUBLIC METHODS
    # ------------------------------------------------------------------

    @classmethod
    def last_updated_at(cls):
        """
        Return the most recent ``updated_at`` timestamp stored in the DB.

        If the table is empty, fall back to 2000-01-01 in the store TZ.
        """
        qs = cls.objects.all()
        if qs.exists():
            # return qs.aggregate(last=Max("updated_at"))["last"]
            return qs.order_by('-updated_at').first().updated_at
        else:
            return _localise_date(dt.datetime(2000, 1, 1))

    @classmethod
    def set_ts(cls, attrs: dict, serie: pd.Series,
               update=False, replace=False):
        """
        Persist a dense time-series in the store.

        Parameters
        ----------
        attrs   : dict
            Business keys identifying the series (version, kind, …).
        serie   : pd.Series
            The data to save.
        update  : bool, default False
            If True, merge the new series with the existing one
            (``combine_first`` logic).
        replace : bool, default False
            If True, delete any existing chunks for the same keys
            before inserting the new series.

        Notes
        -----
        * ``update`` and ``replace`` are mutually exclusive.
        * All model business keys must be present in *attrs*.
        * Inserting over existing data with both flags at False raises ``IntegrityError``.
        """
        if update and replace:
            raise ValueError('update and replace are mutually exclusive')
        cls._ensure_all_attrs_specified(attrs)
        serie = cls._normalize_index(serie)
        if serie is None:
            return
        if replace:
            # we need to delete previous related chunks (previous serie may lay on a greater span than new one)
            cls.objects.filter(**attrs).delete()

        # Enregistrement par chunk
        rows = []
        for sub in cls._chunk(serie):
            if update:
                cls._update_chunk_with_existing(attrs, sub)
            else:
                rows.append(cls._build_row(attrs, sub))

        if not update:
            cls._bulk_create(rows)

    @classmethod
    def get_ts(cls, attrs: dict, start: pd.Timestamp=None, end: pd.Timestamp=None) -> None | pd.Series:
        """
        Retrieve a time-series matching *attrs*.

        attrs   : dict
            Business keys identifying the series (version, kind, …).
        start   : dt.datetime
            Optional, start of time range to retrieve.
        end   : dt.datetime
            Optional, end of time range to retrieve.

        Notes
        -----
        * All model business keys must be present in *attrs*.
        * Requesting non-existing attrs will return None.

        Returns
        -------
        pd.Series | None
            The reconstructed series, or *None* if no chunk matches.
        """
        cls._ensure_all_attrs_specified(attrs)
        qs = cls.objects.filter(**attrs).order_by('chunk_index')
        if start or end:
            qs = cls._filter_interval(qs, start, end)

        pieces = []
        for row in qs:
            pieces.append(cls._decompress(row))

        if not pieces:
            return None

        full = pd.concat(pieces)
        return cls._slice_serie(full, start, end)

    @classmethod
    def set_many_ts(cls, mapping: dict[tuple, pd.Series], keys: tuple[str, ...]):


        """
        Bulk insert many timeseries at once.

        Notes
        -----
        * All model business keys must be present in *attrs*.
        * Trying to insert over existing attrs will raise a django.db.utils.IntegrityError (user must clear matching
        filters upstream)

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
        cls._bulk_create(rows)

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
            serie = cls._slice_serie(serie, start, end)
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

    # ------------------------------------------------------------------
    #  SYNC CLIENT ⇆ SERVER
    # ------------------------------------------------------------------

    @classmethod
    def list_updates(cls, since: pd.Timestamp) -> list[dict]:
        """
        Return metadata for every chunk whose ``updated_at`` is strictly
        greater than *since*.

        Each dict contains:
            attrs       : full business key dict
            chunk_index : int
            dtype       : str
            start_ts    : dt.datetime
            updated_at  : dt.datetime
        """
        qs = (cls.objects
              .filter(updated_at__gt=since)
              .values(*cls.get_model_keys(),
                      "chunk_index", "dtype",
                      "start_ts", "updated_at"))
        out = []
        for row in qs:
            out.append({
                "attrs": {k: row[k] for k in cls.get_model_keys()},
                "chunk_index": row["chunk_index"],
                "dtype": row["dtype"],
                "start_ts": row["start_ts"],
                "updated_at": row["updated_at"],
            })
        return out

    @classmethod
    def export_chunks(cls, spec: list[dict]) -> list[tuple]:
        """
        Server-side helper: given *spec* (list of ``{"attrs": ..,
        "chunk_index": ..}``) return raw LZ4 blobs together with their
        metadata (blob_lz4: bytes, attrs: dict, meta: dict).
        No decompression is performed.
        """
        out = []
        for item in spec:
            attrs = item["attrs"]
            idx = item["chunk_index"]
            row = cls.objects.get(**attrs, chunk_index=idx)
            blob = row.data
            meta = {"dtype": row.dtype, "start_ts": row.start_ts}
            out.append((blob, attrs, meta))
        return out

    @classmethod
    def import_chunks(cls, payload: list[tuple]):
        """
        Client-side helper: ingest the list produced by *export_chunks*.
        Each tuple is ``(blob_lz4, attrs_dict, meta_dict)``.
        """
        for blob, attrs, meta in payload:
            cls._ensure_all_attrs_specified(attrs)
            idx = cls._chunk_index(pd.Timestamp(meta["start_ts"]))
            cls.objects.update_or_create(
                defaults=dict(data=blob, **meta),
                **attrs, chunk_index=idx
            )

    # -- private helpers --

    @classmethod
    def _normalize_index(cls, serie: pd.Series) -> Union[None, pd.Series]:
        """
        normalize index of serie by reindexing over chunking grid
        Args:
            serie: initial serie

        Returns: transformed serie
        """
        if serie.empty:
            return None

        if not isinstance(serie.index, pd.DatetimeIndex):
            raise ValueError('Index doit être DatetimeIndex.')

        if serie.isnull().all():
            return None

        if serie.index.tz is None:
            logger.warning(f'Saving serie without tz may lead to inconsistent results : localized to STORE_TZ {cls.STORE_TZ}')
            serie = serie.tz_localize(cls.STORE_TZ)
        else:
            serie = serie.tz_convert(cls.STORE_TZ)

        first = serie.index[0]
        last = serie.index[-1]
        if cls.CHUNK_AXIS == ('year',):
            start = first.replace(month=1, day=1, hour=0, minute=0)
            end   = last.replace(day=1, hour=0, minute=0) + pd.offsets.YearEnd() + pd.offsets.Day()
        else:  # ('year','month')
            start = first.replace(day=1, hour=0, minute=0)
            end   = last.replace(day=1, hour=0, minute=0) + pd.offsets.MonthEnd() + pd.offsets.Day()

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
    def _chunk_index(cls, ts: pd.DatetimeIndex) -> int:
        """ Compute chunk_index value """
        if cls.CHUNK_AXIS == ('year',):
            return ts.year
        # else ('year','month')
        return ts.year * 12 + ts.month - 1

    @classmethod
    def _build_row(cls, attrs: dict, serie: pd.Series):
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
    def _update_chunk_with_existing(cls, attrs: dict, serie: pd.Series):
        attributes = {**attrs, 'chunk_index': cls._chunk_index(serie.index[0])}

        # combine first with existing
        try:
            row = cls.objects.get(**attributes)
            ds_existing = cls._decompress(row)
            # tz_convert UTC : avoid nan insertion at october tz switch
            serie.index = serie.index.tz_convert('UTC')
            ds_existing.index = ds_existing.index.tz_convert('UTC')
            ds_new = serie.combine_first(ds_existing)
            row = cls._build_row(attrs, ds_new)
        except cls.DoesNotExist:
            row = cls._build_row(attrs, serie)
        except cls.MultipleObjectsReturned:
            raise ValueError(f'Multiple chunks found for attributes {attributes}')

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
    def _bulk_create(cls, rows: list):
        if not rows:
            return

        with transaction.atomic():
            cls.objects.bulk_create(
                rows,
                batch_size=cls.BULK_CREATE_BATCH_SIZE,  # optionnel : tuning
            )

    @classmethod
    def _rebuild_index(cls, row, length: int):
        start = row.start_ts.astimezone(ZoneInfo(cls.STORE_TZ))
        return pd.date_range(start=start,
                             periods=length,
                             freq=cls.STORE_FREQ)

    @classmethod
    def _filter_interval(cls, qs: QuerySet, start: pd.Timestamp, end: pd.Timestamp):
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
    def _ensure_all_attrs_specified(cls, attrs: dict):
        """
        Vérifie que l'utilisateur a renseigné tous les attributs "métiers"
        """
        attrs_keys = set(attrs.keys())
        model_keys = cls.get_model_keys()
        if model_keys != attrs_keys:
            raise ValueError(f'Trying to set or get partial attributes {attrs} while full attributes list is {model_keys}')

    @classmethod
    def _slice_serie(cls, serie, start, end):
        if start and end:
            serie = serie.loc[start:end]
        elif start:
            serie = serie.loc[start:]
        elif end:
            serie = serie.loc[:end]
        return serie

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
