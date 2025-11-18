import datetime as dt
import logging
from functools import lru_cache
from hashlib import blake2b
from typing import Union, List

import pytz
from django.core.exceptions import ImproperlyConfigured
from django.db import models, transaction
import lz4.frame as lz4
import numpy as np
import pandas as pd
from django.db.models import QuerySet, Max
from django.db.models.base import ModelBase
from django.db.models.signals import class_prepared
from django.dispatch import receiver
from pytz.exceptions import UnknownTimeZoneError
from hostore.utils.timeseries import _localise_date, _localised_now

logger = logging.getLogger(__name__)

# KEYS_ABSTRACT_CLASS = set([field.name for field in TimeseriesChunkStore._meta.get_fields()])
# KEYS_ABSTRACT_CLASS.add('id')
KEYS_ABSTRACT_CLASS = {'id', 'start_ts', 'data', 'dtype', 'updated_at', 'is_deleted', 'chunk_index'}
FROZEN_ATTRS = ('CHUNK_AXIS', 'STORE_TZ', 'STORE_FREQ', 'ALLOW_CLIENT_SERVER_SYNC')

EMPTY_DATA = lz4.compress(np.array([]))
PG_MAX_NAME = 63

class ChunkQuerySet(models.QuerySet):
    """Remplace l'effacement physique par un soft-delete."""
    def delete(self, keep_tracking=False, _disable_sync_safety=False):
        """
        Overridden delete method that handles the tracking of deleted object.

        Raises a ValueError if the user is trying to delete objects with ALLOW_CLIENT_SERVER_SYNC=True and
        keep_tracking=False

        Args:
            keep_tracking:
            _disable_sync_safety: should not be used by user. Disable consistency check between
            ALLOW_CLIENT_SERVER_SYNC and keep_tracking=False
        """
        if not keep_tracking:
            if self.model.ALLOW_CLIENT_SERVER_SYNC and not _disable_sync_safety:
                raise ValueError(f'Trying to delete {self.model.__name__} objects with ALLOW_CLIENT_SERVER_SYNC=True and '
                                 f'keep_tracking=False. This would lead to inconsistencies in client store.')
            super().delete()
        else:
            return super().update(is_deleted=True, data=EMPTY_DATA,
                                  updated_at=_localised_now(timezone_name='UTC'),
                                  )

def _meta_name(model, suffix: str, *, max_len: int = 30) -> str:
    """
    Return a unique, deterministic index name that satisfies common RDBMS
    length limits (≤ *max_len* chars, default 30).

    • Base pattern   "<db_table>_<suffix>"
    • If too long   keep the *prefix* (first part) and append a 4-hex digest
                    so the name stays human-readable **and** unique.

    Example
    -------
    >>> _meta_name(MyModel, "axis")
    'app_mymodel_axis_ab12'
    """
    base = f"{model._meta.db_table}_{suffix}"
    if len(base) <= max_len:
        return base

    # Reserve 5 chars (“_” + 4-char hash) for the uniqueness suffix
    prefix = base[: max_len - 5]
    digest = blake2b(base.encode(), digest_size=2).hexdigest()  # 4 hex chars
    return f"{prefix}_{digest}"

def _tbl_name(app_label: str, model_name: str, sig: str,
              *, max_len: int = PG_MAX_NAME) -> str:
    """
    Renvoie un nom de table <= *max_len*.
    • Patron de base : "<app>_<model>__<signature>"
    • Si trop long  : tronque le préfixe et ajoute un digest sur 8 hex.
    """
    base = f"{app_label}_{model_name.lower()}__{sig}"
    if len(base) <= max_len:
        return base

    # Réserver 9 car. pour "_" + digest(8 hex)
    digest = blake2b(base.encode(), digest_size=4).hexdigest()  # 8 hex
    prefix = base[: max_len - 9]
    return f"{prefix}_{digest}"


class _TCSMeta(ModelBase):
    """
    Métaclasse injectée dans `TimeseriesChunkStore`.

    • Ajoute automatiquement aux sous-classes NON abstraites :
        - UniqueConstraint  (*business_keys, 'chunk_index')
        - Index 'axis'     sur (*business_keys, 'chunk_index')
        - Index 'upd'      sur updated_at
        - Un nom qui fige les propriétés de classe
    """

    def __new__(mcls, name, bases, attrs, **kwargs):
        new_cls = super().__new__(mcls, name, bases, attrs, **kwargs)

        # On ignore la classe abstraite
        if new_cls._meta.abstract:
            return new_cls

        meta = new_cls._meta         # raccourci
        # 1 : UniqueConstraint + indexes
        # 1.1. Keys déclarées par l’utilisateur (hors champs internes)
        business_keys = [
            f.name for f in meta.local_fields          # uniquement ceux ajoutés dans le modèle concret
            if not f.auto_created and f.name not in KEYS_ABSTRACT_CLASS
        ]
        business_keys.sort()
        axis_keys = (*business_keys, "chunk_index")

        # 1.2. UniqueConstraint
        meta.constraints.append(
            models.UniqueConstraint(fields=list(axis_keys), name=_meta_name(new_cls, 'unq'))
        )
        meta.original_attrs["constraints"] = meta.constraints

        # 1.3. INDEXES
        def _has_index(fields) -> bool:
            return any(set(idx.fields) == set(fields) for idx in meta.indexes)

        required = [
            (axis_keys,        "axis"),
            (("updated_at",),  "upd"),
        ]
        for fields, tag in required:
            if not _has_index(fields):
                meta.indexes.append(
                    models.Index(fields=list(fields), name=_meta_name(new_cls, tag))
                )

        # idem pour le détecteur de migrations
        meta.original_attrs["indexes"] = meta.indexes

        # 2 : table signature with class properties
        # 2.1. Construire la signature immuable
        sig = "_".join([
            "ca" + "_".join(new_cls.CHUNK_AXIS),   # ex. cxyear_month
            "f" + new_cls.STORE_FREQ,          # ex. freq1h
            "tz" + new_cls.STORE_TZ.replace('/', '_'),  # ex. tzEurope_Paris
            "sync" if new_cls.ALLOW_CLIENT_SERVER_SYNC else "nosync",
        ])

        # 2.2. Nom de table par défaut : <app>_<model>__<signature>
        default_table = _tbl_name(meta.app_label, name, sig)

        # 2.3. Détecter si l’utilisateur a déjà fixé db_table
        # -----------------------------------------------
        default_django_table = f"{meta.app_label}_{name.lower()}"

        if meta.db_table == default_django_table:
            meta.db_table = default_table
            meta.original_attrs["db_table"] = default_table
        else:
            # Vérif cohérence si db_table a été fourni
            if sig not in meta.db_table:
                raise ImproperlyConfigured(
                    f"`db_table` ({meta.db_table}) ne reflète pas la config "
                    f"immuable ({sig})."
                )
        return new_cls

    def __setattr__(cls, name, value):
        """ block setting any of _FROZEN_ATTRS """
        if name in FROZEN_ATTRS and hasattr(cls, name):
            raise AttributeError(f"{name} est immuable après la 1ʳᵉ migration.")
        super().__setattr__(name, value)


class TimeseriesChunkStore(models.Model, metaclass=_TCSMeta):
    # Partitionnement temporel (null si pas de chunk)
    chunk_index = models.IntegerField()

    # Métadonnées obligatoires
    start_ts = models.DateTimeField()        # premier timestamp inclus
    dtype    = models.CharField(max_length=16)  # ex. 'float64'
    updated_at = models.DateTimeField(auto_now=False)  # synchro
    is_deleted = models.BooleanField(default=False)  # keep trace of deleted objects

    # Données brutes
    data = models.BinaryField()

    # Paramètres haut niveau
    _ALLOWED_CHUNK_AXIS = {('year',), ('year', 'month')}
    CHUNK_AXIS = ('year', 'month')   # Chunking axis for timeseries storage. Configs : ('year',) / ('year', 'month')
    STORE_TZ   = 'Europe/Paris' # Chunking timezone
    STORE_FREQ   = '1h' # Timeseries storage frequency.
    ALLOW_CLIENT_SERVER_SYNC = False # if True, enable the sync features
    CACHED_INDEX_SIZE = 120  # max size for cached date indexes

    _model_keys = None
    _model_td = None
    objects = ChunkQuerySet.as_manager()

    class Meta:
        abstract = True

    @classmethod
    def get_model_keys(cls) -> List[str]:
        """ Returns the list of the keys added to the non abstract class """
        if cls._model_keys is None:
            cls._model_keys = set([field.name for field in cls._meta.get_fields()]) - KEYS_ABSTRACT_CLASS
        return cls._model_keys

    @classmethod
    def _get_model_timedelta(cls) -> pd.Timedelta:
        if cls._model_td is None:
            cls._model_td = pd.to_timedelta(cls.STORE_FREQ)
        return cls._model_td

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

        # wrap the index rebuild helper with an LRU cache sized per subclass
        cls._cached_index = classmethod(
            lru_cache(maxsize=cls.CACHED_INDEX_SIZE)(cls._cached_index.__func__)
        )

    def delete(self, **kwargs):
        """ Soft-delete : conserve la ligne comme tombstone. """
        self.__class__.objects.filter(id=self.id).delete(**kwargs)

    # ------------------ Sérialisation bas niveau ------------------
    @staticmethod
    def _compress(serie: pd.Series) -> (bytes, np.array):
        arr = serie.to_numpy()
        return lz4.compress(arr.tobytes()), arr

    @classmethod
    def _decompress(cls, row, mem_view=False, return_mode=None) -> pd.Series:
        blob = row.data
        dtype = row.dtype
        if mem_view:
            raw = memoryview(lz4.decompress(blob))
            arr = np.frombuffer(raw, dtype=dtype, like=np.empty(0, dtype=dtype))
        else:
            raw = lz4.decompress(blob)
            arr = np.frombuffer(raw, dtype=dtype)

        if return_mode=='light':
            idx_min, idx_max = cls._bound_index(row, len(arr))
            return idx_min, idx_max, arr
        elif return_mode=='max_idx':
            non_nan_indices = np.where(~np.isnan(arr))[0]
            index_max_non_nan = non_nan_indices[-1] if non_nan_indices.size > 0 else 0

            max_horodate = pd.Timestamp(row.start_ts).tz_convert(cls.STORE_TZ) + index_max_non_nan  * cls._get_model_timedelta()
            return max_horodate
        else:
            idx = cls._rebuild_index(row, len(arr))
            return pd.Series(arr, index=idx)

    # ------------------------------------------------------------------
    # PUBLIC METHODS
    # ------------------------------------------------------------------

    @classmethod
    def last_updated_at(cls, filters: dict = None) -> pd.Timestamp:
        """
        Return the most recent ``updated_at`` timestamp stored in the DB.
        If the table is empty, fall back to 2000-01-01 in the store TZ.

        Parameters
        ----------
        filters   : dict
            Filter the data for the update lookup
        """
        if filters is None:
            filters = dict()

        qs = cls.objects.filter(**filters)
        if qs.exists():
            # return qs.aggregate(last=Max("updated_at"))["last"]
            return qs.order_by('-updated_at').first().updated_at
        else:
            return _localise_date(dt.datetime(2000, 1, 1))

    @classmethod
    def set_ts(cls, attrs: dict, serie: pd.Series,
               update=False, replace=False, bulk_create_batch_size=200):
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
        bulk_create_batch_size : bulk batch size for bulk insertion
        Notes
        -----
        * ``update`` and ``replace`` are mutually exclusive.
        * All model business keys must be present in *attrs*.
        * Inserting over existing data with both flags at False raises ``IntegrityError``.
        """
        if update and replace:
            raise ValueError('update and replace are mutually exclusive')

        if (not update) and (not replace) and cls.ALLOW_CLIENT_SERVER_SYNC:
            raise ValueError(f'Trying to use set_ts with model {cls.__name__} '
                             f'without update or replace option while ALLOW_CLIENT_SERVER_SYNC=True.')

        cls._ensure_all_attrs_specified(attrs)
        serie = cls._normalize_serie(serie)
        if serie is None:
            return

        # Enregistrement par chunk
        rows = []
        for sub in cls._chunk(serie):
            if update:
                cls._update_chunk_with_existing(attrs, sub)
            else:
                rows.append(cls._build_row(attrs, sub))
                if len(rows) >= bulk_create_batch_size:
                    cls._bulk_create(rows, bulk_create_batch_size)
                    rows = []

        if replace:
            # we need to hard delete previous related chunks (previous serie may lay on a greater span than new one)
            # only over chunk_index deleted (to allow propagation of deleted index to client store)
            chunk_index_replaced = [r.chunk_index for r in rows]
            qd_attrs = cls.objects.filter(
                **attrs,
            )
            # hard delete part of the serie within existing chunks index : will be replaced in _bulk_create
            # _disable_sync_safety to avoid check raise
            qd_attrs.filter(
                chunk_index__in=chunk_index_replaced
            ).delete(keep_tracking=False, _disable_sync_safety=True)
            # soft delete other chunks : keep trace of deletion
            qd_attrs.delete(keep_tracking=True)

        if not update:
            cls._bulk_create(rows, bulk_create_batch_size)

    @classmethod
    def get_ts(cls, attrs: dict, start: pd.Timestamp=None, end: pd.Timestamp=None, drop_bounds_na=True) -> None | pd.Series:
        """
        Retrieve a time-series matching *attrs*.

        attrs   : dict
            Business keys identifying the series (version, kind, …).
        start   : dt.datetime
            Optional, start of time range to retrieve.
        end   : dt.datetime
            Optional, end of time range to retrieve.
        drop_bounds_na : bool
            Optional, drop nans from result.

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
        qs = cls.objects.filter(**attrs, is_deleted=False).order_by('chunk_index')
        if start or end:
            qs = cls._filter_interval(qs, start, end)

        pieces = []
        for row in qs:
            pieces.append(cls._decompress(row))

        if not pieces:
            return None

        full = pd.concat(pieces)
        serie = cls._finish_serie(full, start, end, drop_bounds_na)

        return serie

    @classmethod
    def set_many_ts(cls, mapping: dict[tuple, pd.Series], keys: tuple[str, ...],
                    bulk_create_batch_size=200):


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
            bulk_create_batch_size : bulk batch size for bulk insertion

        Returns:
        """
        if cls.ALLOW_CLIENT_SERVER_SYNC:
            raise ValueError(f'Trying to use set_many_ts with model {cls.__name__} '
                             f'while ALLOW_CLIENT_SERVER_SYNC=True.')

        rows = []
        for ktuple, serie in mapping.items():
            attrs = dict(zip(keys, ktuple))
            cls._ensure_all_attrs_specified(attrs)
            serie = cls._normalize_serie(serie)
            if serie is None:
                continue
            for sub in cls._chunk(serie):
                rows.append(cls._build_row(attrs, sub))
                if len(rows) >= bulk_create_batch_size:
                    cls._bulk_create(rows, bulk_create_batch_size)
                    rows = []
        cls._bulk_create(rows, bulk_create_batch_size)

    @classmethod
    def yield_many_ts(cls, filters: dict, start: pd.Timestamp=None, end: pd.Timestamp=None,
                      qs_iterator_chunk_size=200, drop_bounds_na=True):
        """
        Yield (serie, filters_dict) for each available timeseries with filters matching filters.
            - serie will be expressed at STORE_FREQ, STORE_TZ
            - attrs_dict : mapping {model_key: value}

        Args:
            filters: filters of query
            start: start index of timeseries
            end: end index of timeseries
            qs_iterator_chunk_size: size of queryset batch
            drop_bounds_na: drop NaNs
        """
        # On valide seulement les clés fournies
        cls._check_attrs(filters)

        qs = cls.objects.filter(**filters, is_deleted=False).order_by(*(cls.get_model_keys()), 'chunk_index')
        if start or end:
            qs = cls._filter_interval(qs, start, end)

        current_values = None
        buffer_data = []
        min_idx, max_idx = None, None
        def flush():
            if not buffer_data:
                return

            serie = pd.Series(
                index=pd.date_range(start=min_idx, end=max_idx, inclusive='both', freq=cls.STORE_FREQ),
                data=np.concatenate(buffer_data)
            )
            serie = cls._finish_serie(serie, start, end, drop_bounds_na)
            key_dict = dict(zip(cls.get_model_keys(), current_values))
            yield serie, key_dict
            # fixme ? done outside to be allowed to erase _idx
            # buffer_data.clear()
            # min_idx = None
            # max_idx = None

        for row in qs.iterator(chunk_size=qs_iterator_chunk_size):
            values = tuple(getattr(row, k) for k in cls.get_model_keys())
            if current_values is None:
                current_values = values
            elif values != current_values:
                # nouvelle combinaison → on émet la série courante
                yield from flush()
                buffer_data.clear()
                min_idx = None
                max_idx = None

                current_values = values
            _idx_min, _idx_max, data = cls._decompress(row, return_mode='light')
            min_idx = min(min_idx, _idx_min) if min_idx else _idx_min
            max_idx = max(max_idx, _idx_max) if max_idx else _idx_max
            buffer_data.append(data)

        # flush final
        yield from flush()

    @classmethod
    def get_max_horodate(cls, filters: dict, qs_iterator_chunk_size=200):
        """
        Get maximum horodate of timeseries with filters matching filters.

        Args:
            filters: filters of query
        """
        # On valide seulement les clés fournies
        cls._check_attrs(filters)

        qs = cls.objects.filter(**filters, is_deleted=False).order_by(*(cls.get_model_keys()), 'chunk_index')
        max_qs_chunk_index = qs.aggregate(max_chunk=Max('chunk_index'))['max_chunk']
        qs = qs.filter(chunk_index=max_qs_chunk_index)
        current_values = None
        max_idxs = []
        for row in qs.iterator(chunk_size=qs_iterator_chunk_size):
            # max de la série actuelle
            max_idxs.append(cls._decompress(row, return_mode='max_idx'))

        return max(max_idxs) if max_idxs else None

    # ------------------------------------------------------------------
    #  SYNC CLIENT ⇆ SERVER
    # ------------------------------------------------------------------

    @classmethod
    def updates_queryset(cls, since: pd.Timestamp, filters: dict | None = None):
        """Return a queryset of chunks updated after *since*.

        The queryset is ordered by ``updated_at`` and primary key to ensure
        deterministic pagination.
        """
        filters = filters or {}
        return (
            cls.objects
            .filter(**filters, updated_at__gte=since)
            .order_by("updated_at", "pk")
            .values(
                *cls.get_model_keys(),
                "chunk_index",
                "dtype",
                "start_ts",
                "updated_at",
                "is_deleted",
            )
        )

    @classmethod
    def list_updates(
        cls,
        since: pd.Timestamp,
        filters: dict | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[dict]:
        """Convenience helper returning a list from ``updates_queryset``.

        Args:
            since: timestamp lower bound (excluded).
            filters: additional filters on the query.
            limit: maximum number of items to return.
            offset: number of items to skip from the beginning.

        Each dict contains::
            attrs       : full business key dict
            chunk_index : int
            dtype       : str
            start_ts    : dt.datetime
            updated_at  : dt.datetime
            is_deleted  : bool
        """
        qs = cls.updates_queryset(since, filters)
        if offset:
            qs = qs[offset:]
        if limit is not None:
            qs = qs[:limit]

        out = []
        for row in qs:
            out.append({
                "attrs": {k: row[k] for k in cls.get_model_keys()},
                "chunk_index": row["chunk_index"],
                "dtype": row["dtype"],
                "start_ts": row["start_ts"],
                "updated_at": row["updated_at"],
                "is_deleted": row["is_deleted"],
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
            attrs["chunk_index"] = row.chunk_index
            blob = row.data
            meta = {
                "dtype": row.dtype,
                "start_ts": row.start_ts,
                "is_deleted": row.is_deleted,
                "updated_at": row.updated_at,
            }
            out.append((blob, attrs, meta))
        return out

    @classmethod
    def import_chunks(cls, payload: list[tuple]):
        """
        Client-side helper: ingest the list produced by *export_chunks*.
        Each tuple is ``(blob_lz4, attrs_dict, meta_dict)``.
        """
        if not payload:
            return

        # Pre-compute union of meta fields to update using bulk_update later on
        meta_fields = set()
        for _, _, meta in payload:
            meta_fields.update(k for k in meta.keys() if k != "updated_at")

        now = _localised_now(timezone_name="UTC")

        with transaction.atomic():
            attrs_list = [attrs for _, attrs, _ in payload]
            q = models.Q()
            for attrs in attrs_list:
                cls._ensure_all_attrs_specified(attrs, bonus_keys=["chunk_index"])
                q |= models.Q(**attrs)

            existing = {}
            if attrs_list:
                for row in cls.objects.filter(q):
                    key = tuple(sorted((f, getattr(row, f)) for f in attrs_list[0].keys()))
                    existing[key] = row

            rows_to_create = []
            rows_to_update = []
            for blob, attrs, meta in payload:
                key = tuple(sorted(attrs.items()))
                updated_at = meta.get("updated_at")
                if updated_at is not None:
                    updated_at = pd.Timestamp(updated_at)
                    if updated_at.tzinfo is None:
                        updated_at = updated_at.tz_localize("UTC")
                    updated_at = updated_at.to_pydatetime()
                else:
                    updated_at = now
                meta_without_updated = {k: v for k, v in meta.items() if k != "updated_at"}
                if key in existing:
                    row = existing[key]
                    row.data = blob
                    row.updated_at = updated_at
                    for k, v in meta_without_updated.items():
                        setattr(row, k, v)
                    rows_to_update.append(row)
                else:
                    obj = cls(
                        **attrs,
                        data=blob,
                        updated_at=updated_at,
                        **meta_without_updated,
                    )
                    rows_to_create.append(obj)

            if rows_to_create:
                cls.objects.bulk_create(rows_to_create)
            if rows_to_update:
                update_fields = ["data", "updated_at", *meta_fields]
                cls.objects.bulk_update(rows_to_update, update_fields)

    # -- private helpers --

    @classmethod
    def _normalize_serie(cls, serie: pd.Series) -> Union[None, pd.Series]:
        serie = cls._normalize_index(serie)
        if (serie is not None) and (str(serie.dtype) == 'object'):
            serie = serie.apply(float)
        return serie

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
        nulls = serie.isnull()
        if nulls.all():
            return None
        # avoid to save null parts in db
        serie = serie[~nulls]
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
        else:
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
            data=compressed,
            is_deleted=False,
            updated_at=_localised_now(timezone_name='UTC'),
        )

    @classmethod
    def _update_chunk_with_existing(cls, attrs: dict, serie: pd.Series):
        attributes = {**attrs, 'chunk_index': cls._chunk_index(serie.index[0])}

        with transaction.atomic():
            # combine first with existing
            try:
                row = cls.objects.select_for_update().get(**attributes)
                if row.is_deleted:
                    # avoid using deleted row
                    raise cls.DoesNotExist
                ds_existing = cls._decompress(row)
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
                'is_deleted': False,
                'updated_at': _localised_now(timezone_name='UTC'),
            }

            cls.objects.update_or_create(
                defaults=defaults,
                **attributes
            )

    @classmethod
    def _bulk_create(cls, rows: list, bulk_create_batch_size: int):
        if not rows:
            return

        with transaction.atomic():
            cls.objects.bulk_create(
                rows,
                batch_size=bulk_create_batch_size,  # optionnel : tuning
            )

    @classmethod
    def _bound_index(cls, row, length: int):
        min_idx = pd.Timestamp(row.start_ts).tz_convert(cls.STORE_TZ)
        max_idx = min_idx + (length - 1) * cls._get_model_timedelta()
        return min_idx, max_idx

    @classmethod
    def _rebuild_index(cls, row, length: int):
        return cls._cached_index(row.start_ts, length)

    @classmethod
    def _cached_index(cls, start_ts, length):
        return pd.date_range(
            start=pd.Timestamp(start_ts).tz_convert(cls.STORE_TZ),
            periods=length,
            freq=cls.STORE_FREQ,
            tz=cls.STORE_TZ,
        )

    @classmethod
    def _filter_interval(cls, qs: QuerySet, start: pd.Timestamp, end: pd.Timestamp):
        if isinstance(start, str):
            start = pd.Timestamp(start, tz=cls.STORE_TZ)
        elif start is None:
            pass
        else:
            start = pd.Timestamp(start).tz_convert(cls.STORE_TZ)

        if isinstance(end, str):
            end = pd.Timestamp(end, tz=cls.STORE_TZ)
        elif end is None:
            pass
        else:
            end = pd.Timestamp(end).tz_convert(cls.STORE_TZ)

        if start:
            qs = qs.filter(chunk_index__gte=cls._chunk_index(start))
        if end:
            qs = qs.filter(chunk_index__lte=cls._chunk_index(end))
        return qs

    @classmethod
    def _ensure_all_attrs_specified(cls, attrs: dict, bonus_keys=None):
        """
        Vérifie que l'utilisateur a renseigné tous les attributs "métiers"
        """
        attrs_keys = set(attrs.keys())
        model_keys = cls.get_model_keys()
        if bonus_keys:
            model_keys = {*model_keys, *bonus_keys}
        if model_keys != attrs_keys:
            raise ValueError(f'Trying to set or get partial attributes {attrs} while full attributes list is {model_keys}')

    @classmethod
    def _finish_serie(cls, serie, start, end, drop_bounds_na):
        if start and end:
            serie = serie.loc[start:end]
        elif start:
            serie = serie.loc[start:]
        elif end:
            serie = serie.loc[:end]

        if drop_bounds_na:
            serie = serie.loc[serie.first_valid_index(): serie.last_valid_index()]

        return serie

    @classmethod
    def _check_attrs(cls, attrs):
        fields_required = []
        for k in attrs.keys():
            if '__' in k:
                k = k.split('__')[0]
            fields_required.append(k)

        bad = set(fields_required) - cls.get_model_keys()
        if bad:
            raise ValueError(f"Unknown attribute(s) {bad}")


# class TestMyTimeseriesChunkStore(TimeseriesChunkStore):
#     version = models.IntegerField()
#     kind_very_long_name_for_testing_purpose = models.CharField(max_length=50)
#     CHUNK_AXIS = ('year', 'month')


