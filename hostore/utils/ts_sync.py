# ts_sync/views.py
import functools
import logging
import traceback

import backoff
import requests

from rest_framework import viewsets
from rest_framework.decorators import action
from rest_framework.pagination import LimitOffsetPagination
from rest_framework.response import Response
import base64, pandas as pd
from typing import Type

DEBUG = False


logger = logging.getLogger(__name__)

def print_api_exception(view_func):
    """
    affiche erreur api dans une view
    """
    @functools.wraps(view_func)
    def wrapper(self, request, *args, **kw):
        try:
            return view_func(self, request, *args, **kw)
        except Exception as exc:
            if DEBUG:
                print(f'\nview func {view_func} unexpected error')
                print(traceback.format_exc())
            raise
    return wrapper


class ChunkIteratorPagination(LimitOffsetPagination):
    """Limit/offset paginator that streams queryset results."""

    def paginate_queryset(self, queryset, request, view=None):
        self.limit = self.get_limit(request)
        if self.limit is None:
            return None

        self.count = self.get_count(queryset)
        self.request = request
        self.offset = self.get_offset(request)

        if self.count == 0 or self.offset > self.count:
            return []

        chunk_size = getattr(view, "qs_iterator_chunk_size", 200)
        return list(
            queryset[self.offset : self.offset + self.limit]
        )


class TimeseriesChunkStoreSyncViewSet(viewsets.GenericViewSet):
    """
    Base server-side ViewSet that exposes a REST interface for synchronising
    any TimeseriesChunkStore subclass.

    End-points
    ──────────
    • GET  /updates/?since=ISO    → list of modified chunks
    • GET /pack/                 → export requested chunks

    Usage example in your own views.py:

        class YearStoreSyncView(TimeseriesChunkStoreSyncViewSet):
            store_model = TestStoreChunkYear     # ← your store

        router.register("ts/year", YearStoreSyncView, basename="ts-year")
    """

    store_model: Type['TimeseriesChunkStore'] = None
    pagination_class = ChunkIteratorPagination

    # 1) /updates/?since=ISO
    @action(detail=False, methods=["get"])
    @print_api_exception
    def updates(self, request):
        if not self.store_model.ALLOW_CLIENT_SERVER_SYNC:
            raise ValueError(f'Trying to use TimeseriesChunkStoreSyncViewSet with model {self.store_model.__name__} '
                             f'while ALLOW_CLIENT_SERVER_SYNC=False.')

        since = pd.Timestamp(request.query_params["since"])
        filters = {
            k: v
            for k, v in request.query_params.items()
            if k not in {"since", "limit", "offset"}
        }

        # log query
        try:
            user = self.request.user
        except:
            user = None
        logger.info(f'Sync updates for model {self.store_model.__name__} required by user {user} since {since} (filters {filters})')

        qs = self.store_model.updates_queryset(since, filters)
        page = self.paginate_queryset(qs)
        data = [
            {
                "attrs": {k: row[k] for k in self.store_model.get_model_keys()},
                "chunk_index": row["chunk_index"],
                "dtype": row["dtype"],
                "start_ts": row["start_ts"],
                "updated_at": row["updated_at"],
                "is_deleted": row["is_deleted"],
            }
            for row in page
        ]
        return self.get_paginated_response(data)

    # 2) /pack/   GET → export
    @action(detail=False, methods=["get"])
    @print_api_exception
    def pack(self, request):
        if not self.store_model.ALLOW_CLIENT_SERVER_SYNC:
            raise ValueError(f'Trying to use TimeseriesChunkStoreSyncViewSet with model {self.store_model.__name__} '
                             f'while ALLOW_CLIENT_SERVER_SYNC=False.')

        spec    = request.data

        # log query
        try:
            user = self.request.user
        except:
            user = None
        logger.info(
            f'Sync pack for model {self.store_model.__name__} required by user ({len(spec)} pieces)')

        chunks  = self.store_model.export_chunks(spec)
        payload = [
            {
                "blob":  base64.b64encode(b).decode(),
                "attrs": attrs,
                "meta":  meta,
            } for b, attrs, meta in chunks
        ]
        return Response(payload, content_type="application/json")

    @classmethod
    def as_factory(cls, model, **extra_attrs):
        """
        Dynamically create a ViewSet bound to *model* and
        enriched with optional DRF attributes.

        Example
        -------
        YearSync = TimeseriesChunkStoreSyncViewSet.as_factory(
            TestStoreChunkYear,
            permission_classes=[IsAuthenticatedActive],
            parser_classes=[JSONParser],
            throttle_classes=[],               # disable throttling
        )
        router.register("ts/year", YearSync, basename="ts-year")
        """
        if not hasattr(model, "updates_queryset"):
            raise TypeError("model must inherit TimeseriesChunkStore")
        attrs = {"store_model": model, **extra_attrs}
        return type(f"{model.__name__}SyncViewSet", (cls,), attrs)


class TimeseriesChunkStoreSyncClient:
    """
    client = TimeseriesChunkStoreSyncClient(
                endpoint="https://api.example.com/ts/year/",
                store_model=TestStoreChunkYear)
    client.pull(since) : récupère les nouveautés
    """

    def __init__(self, *,
                 endpoint: str,
                 store_model: Type['TimeseriesChunkStore'],
                 retry_max_tries: int = 5,
                 retry_max_time: int = 300,
                 requests_get_kwargs=None
                 ):
        """
        Construct the synchronization client for store_model.

        Args:
            endpoint: url to call /updates/ and /pack/
            store_model: client TimeseriesChunkStore model to insert updates
            retry_max_tries: number of retry attemps
            retry_max_time: retry time in milliseconds
            requests_get_kwargs: kwargs to pass to requests.get
        """
        if not store_model.ALLOW_CLIENT_SERVER_SYNC:
            raise ValueError(f'Trying to use TimeseriesChunkStoreSyncClient with model {store_model.__name__} '
                             f'while ALLOW_CLIENT_SERVER_SYNC=False.')

        self.endpoint = endpoint.rstrip("/")
        self.store_model    = store_model
        self._retry_tries = retry_max_tries
        self._retry_time  = retry_max_time
        self.requests_get_kwargs  = requests_get_kwargs if requests_get_kwargs is not None else {}

    # ----------- pull depuis le serveur -------------------------------
    def pull(
        self,
        filters: dict | None = None,
        batch: int = 50,
        page_size: int = 200,
    ):
        """Fetch updates from the server in a paginated fashion.

        Args:
            filters: optional server-side filters.
            batch: chunk size for `/pack/` requests.
            page_size: number of items to request from `/updates/` per call.
        """
        filters = dict(filters or {})

        since = self.store_model.last_updated_at(filters)
        url_updates = f"{self.endpoint}/updates/"
        params = {"since": since.isoformat(), "limit": page_size, **filters}
        total_fetch = total_delete = 0

        def _parse_updated_at(value):
            ts = pd.Timestamp(value)
            if ts.tzinfo is None:
                ts = ts.tz_localize("UTC")
            return ts.to_pydatetime()

        while url_updates:
            page = self._get(url_updates, params=params)
            params = None  # next links already contain query parameters
            updates = page.get("results", [])
            if not updates:
                break

            to_fetch, to_delete = [], []
            for u in updates:
                (to_delete if u["is_deleted"] else to_fetch).append(u)

            for d in to_delete:
                qs = self.store_model.objects.filter(
                    **d["attrs"], chunk_index=d["chunk_index"]
                )
                qs.delete(keep_tracking=True)
                qs.update(updated_at=_parse_updated_at(d["updated_at"]))

            for i in range(0, len(to_fetch), batch):
                spec = to_fetch[i : i + batch]
                pack = self._get(f"{self.endpoint}/pack/", json=spec)
                tuples = [
                    (base64.b64decode(item["blob"]), item["attrs"], item["meta"])
                    for item in pack
                ]
                self.store_model.import_chunks(tuples)

            total_fetch += len(to_fetch)
            total_delete += len(to_delete)
            url_updates = page.get("next")

        return total_fetch, total_delete

    # ----------- requête HTTP avec back-off paramétrable --------------
    def _get(self, url: str, **kwargs):
        @backoff.on_exception(
            backoff.expo,
            requests.exceptions.RequestException,
            max_tries=self._retry_tries,
            max_time=self._retry_time,
        )
        def _call():
            resp = requests.get(url, **kwargs, **self.requests_get_kwargs)
            resp.raise_for_status()
            return resp.json()
        
        return _call()
