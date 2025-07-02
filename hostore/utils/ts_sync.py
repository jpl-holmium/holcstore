# ts_sync/views.py
import requests
from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.response import Response
import base64, pandas as pd
from typing import Type



class TimeseriesChunkStoreSyncViewSet(viewsets.ViewSet):
    """
    Base ViewSet for server side to synchronize time series data from a TimeseriesChunkStore

    Example (view.py) :

        class YearStoreSyncView(TimeseriesSyncViewSet):
            store_model = TestStoreChunkYear

        router.register("ts/year", YearStoreSyncView, basename="ts-year")
    """

    store_model: Type['TimeseriesChunkStore'] = None

    # 1) /updates/?since=ISO
    def updates(self, request):
        since = pd.Timestamp(request.query_params["since"])
        data  = self.store_model.list_updates(since)
        return Response(data)

    # 2) /pack/   POST → export ;  PUT → import
    @action(detail=False, methods=["post", "put"])
    def pack(self, request):
        if request.method == "POST":          # export
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

        # import
        payload = [
            (base64.b64decode(item["blob"]), item["attrs"], item["meta"])
            for item in request.data
        ]
        self.store_model.import_chunks(payload)
        return Response(status=status.HTTP_204_NO_CONTENT)


class TimeseriesChunkStoreSyncClient:
    """
    client = TimeseriesChunkStoreSyncClient(
                endpoint="https://api.example.com/ts/year/",
                local_model=TestStoreChunkYear)
    client.pull(since) : récupère les nouveautés
    """

    def __init__(self, *, endpoint: str, local_model):
        self.endpoint = endpoint.rstrip("/")
        self.local    = local_model

    # ----------- pull depuis le serveur -------------------------------
    def pull(self, batch=50):
        since = self.local.last_updated_at()
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
            self.local.import_chunks(tuples)
