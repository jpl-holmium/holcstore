# ts_sync/views.py
import functools
import traceback

import backoff
import requests

from rest_framework import viewsets
from rest_framework.decorators import action
from rest_framework.response import Response
import base64, pandas as pd
from typing import Type

DEBUG = False


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

    return wrapper


class TimeseriesChunkStoreSyncViewSet(viewsets.ViewSet):
    """
    Base server-side ViewSet that exposes a REST interface for synchronising
    any TimeseriesChunkStore subclass.

    End-points
    ──────────
    • GET  /updates/?since=ISO    → list of modified chunks
    • POST /pack/                 → export requested chunks

    Usage example in your own views.py:

        class YearStoreSyncView(TimeseriesChunkStoreSyncViewSet):
            store_model = TestStoreChunkYear     # ← your store

        router.register("ts/year", YearStoreSyncView, basename="ts-year")
    """

    store_model: Type['TimeseriesChunkStore'] = None

    # 1) /updates/?since=ISO
    @action(detail=False, methods=["get"])
    @print_api_exception
    def updates(self, request):
        since = pd.Timestamp(request.query_params["since"])
        filters = {
            k: v for k, v in request.query_params.items()
            if k != "since"
        }
        data  = self.store_model.list_updates(since, filters)
        return Response(data, content_type="application/json")

    # 2) /pack/   POST → export
    @action(detail=False, methods=["get"])
    @print_api_exception
    def pack(self, request):
        spec    = request.data
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
        if not hasattr(model, "list_updates"):
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
                 ):
        """
        Construct the synchronization client for store_model.

        Args:
            endpoint: url to call /updates/ and /pack/
            store_model: client TimeseriesChunkStore model to insert updates
            retry_max_tries: number of retry attemps
            retry_max_time: retry time in seconds
        """
        self.endpoint = endpoint.rstrip("/")
        self.store_model    = store_model
        self._retry_tries = retry_max_tries
        self._retry_time  = retry_max_time

    # ----------- pull depuis le serveur -------------------------------
    def pull(self, batch: int = 50, filters: dict | None = None):
        filters = filters or {}

        since = self.store_model.last_updated_at()
        params = {"since": since.isoformat(), **filters}

        updates = self._get(f"{self.endpoint}/updates/", params=params)

        # split fetch delete
        to_fetch, to_delete = [], []
        for u in updates:
            (to_delete if u["is_deleted"] else to_fetch).append(u)

        # suppression locale
        for d in to_delete:
            self.store_model.objects.filter(
                **d["attrs"], chunk_index=d["chunk_index"]
            ).delete()

        # téléchargement / import
        for i in range(0, len(to_fetch), batch):
            spec  = updates[i : i + batch]
            pack  = self._get(f"{self.endpoint}/pack/", json=spec)
            tuples = [
                (base64.b64decode(item["blob"]), item["attrs"], item["meta"])
                for item in pack
            ]
            self.store_model.import_chunks(tuples)

    # ----------- requête HTTP avec back-off paramétrable --------------
    def _get(self, url: str, **kwargs):
        @backoff.on_exception(
            backoff.expo,
            requests.exceptions.RequestException,
            max_tries=self._retry_tries,
            max_time=self._retry_time,
        )
        def _call():
            resp = requests.get(url, **kwargs)
            resp.raise_for_status()
            return resp.json()

        return _call()