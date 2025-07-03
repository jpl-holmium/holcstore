# ts_sync/views.py
import requests
from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.response import Response
import base64, pandas as pd
from typing import Type



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
    def updates(self, request):
        since = pd.Timestamp(request.query_params["since"])
        data  = self.store_model.list_updates(since)
        return Response(data)

    # 2) /pack/   POST → export
    @action(detail=False, methods=["post"])
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
        return Response(payload)

    @classmethod
    def as_factory(cls, model):
        """
        Retourne dynamiquement un ViewSet déjà relié à `model`.
        Exemple:
            RemoteSync = TimeseriesChunkStoreSyncViewSet.as_factory(RemoteStore)
        """
        if not hasattr(model, "list_updates"):
            raise TypeError("model must inherit TimeseriesChunkStore")
        return type(f"{model.__name__}SyncViewSet", (cls,), {"store_model": model})


class TimeseriesChunkStoreSyncClient:
    """
    client = TimeseriesChunkStoreSyncClient(
                endpoint="https://api.example.com/ts/year/",
                store_model=TestStoreChunkYear)
    client.pull(since) : récupère les nouveautés
    """

    def __init__(self, *, endpoint: str, store_model: Type['TimeseriesChunkStore']):
        self.endpoint = endpoint.rstrip("/")
        self.store_model    = store_model

    # ----------- pull depuis le serveur -------------------------------
    def pull(self, batch=50):
        since = self.store_model.last_updated_at()
        updates = requests.get(
            f"{self.endpoint}/updates/",
            params={"since": since.isoformat()}
        ).json()

        for i in range(0, len(updates), batch):
            spec = updates[i:i+batch]
            pack = requests.post(f"{self.endpoint}/pack/", json=spec).json()
            tuples = [
                (base64.b64decode(item["blob"]), item["attrs"], item["meta"])
                for item in pack
            ]
            self.store_model.import_chunks(tuples)
