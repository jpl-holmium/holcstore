import base64, datetime as dt
from unittest.mock import patch

import requests
from django.db import connection, models
from django.test import TransactionTestCase, override_settings
from django.urls import path, include
from rest_framework.routers import SimpleRouter
from rest_framework.test import APIClient, RequestsClient
import pandas as pd
import numpy as np
import lz4.frame

from hostore.models import TimeseriesChunkStore
from hostore.utils.ts_sync import TimeseriesChunkStoreSyncViewSet, TimeseriesChunkStoreSyncClient


# ------------------------------------------------------------------
#  Modèles “remote” et “local” (mêmes champs, tables distinctes)
# ------------------------------------------------------------------
class RemoteYearStore(TimeseriesChunkStore):
    """Table côté ‘serveur’"""
    version = models.IntegerField()
    kind    = models.CharField(max_length=20)

    class Meta(TimeseriesChunkStore.Meta):
        app_label   = "ts_remote"
        db_table    = "ts_remote_year"
        unique_together = ('version', 'kind', 'chunk_index')


class LocalYearStore(TimeseriesChunkStore):
    """Table côté ‘client’"""
    version = models.IntegerField()
    kind    = models.CharField(max_length=20)

    class Meta(TimeseriesChunkStore.Meta):
        app_label   = "ts_local"
        db_table    = "ts_local_year"
        unique_together = ('version', 'kind', 'chunk_index')


# ------------------------------------------------------------------
#  ViewSet serveur (lié au RemoteYearStore)
# ------------------------------------------------------------------
RemoteSyncViewSet = TimeseriesChunkStoreSyncViewSet.as_factory(RemoteYearStore)
router = SimpleRouter()
router.register("sync/year", RemoteSyncViewSet, basename="sync-year")
urlpatterns = [path("", include(router.urls))]      #   (1)  renommé → urlpatterns

test_urls = [path("", include(router.urls))]


# ------------------------------------------------------------------
#  Test d’intégration complet
# ------------------------------------------------------------------
@override_settings(ROOT_URLCONF=__name__)           #   (2)  on passe le module courant
class SyncIntegrationTestCase(TransactionTestCase):
    reset_sequences = True

    def setUp(self):
        # crée les tables manquantes
        with connection.schema_editor(atomic=False) as se:
            for mdl in (RemoteYearStore, LocalYearStore):
                if mdl._meta.db_table not in connection.introspection.table_names():
                    se.create_model(mdl)
        RemoteYearStore.objects.all().delete()
        self.store_client = TimeseriesChunkStoreSyncClient(endpoint='http://testserver/sync/year', store_model=LocalYearStore)
        LocalYearStore.objects.all().delete()
        self.req_client = RequestsClient()
        self.req_client.base_url = "http://testserver/"
        # self.client = APIClient()

    def _sync(self):

        # # METHODE AVEC APICLIENT client → /updates
        # since = LocalYearStore.last_updated_at()
        # updates = self.client.get("/sync/year/updates/", {"since": since.isoformat()}).json()
        # # client → /pack export
        # pack = self.client.post("/sync/year/pack/", updates, format="json").json()
        # # import côté local
        # tuples = [
        #     (base64.b64decode(item["blob"]), item["attrs"], item["meta"])
        #     for item in pack
        # ]
        # LocalYearStore.import_chunks(tuples)

        with (
            patch.object(requests, "get",  self.req_client.get),
            patch.object(requests, "post", self.req_client.post),
            patch.object(requests, "put",  self.req_client.put),
        ):
            self.store_client.pull(batch=20)

    # --------------------------------------------------------------
    def test_full_sync_and_update(self):
        # seed serveur
        sere_a1_v1 = self._make_series("2025-01-01", 24 * 31 * 4)
        sere_a2_v1 = self._make_series("2026-01-05", 24 * 50)
        series = (
            ({"version": 1, "kind": "A"}, sere_a1_v1),
            ({"version": 2, "kind": "A"}, sere_a2_v1),
            ({"version": 2, "kind": "B"}, self._make_series("2024-01-05", 24 * 50)),
            ({"version": 2, "kind": "c"}, self._make_series("2000-01-05", 24 * 50)),
        )
        for attr, serie in series:
            RemoteYearStore.set_ts(attr, serie)

        self._sync()

        # vérif intégrité 1
        for attr, serie in series:
            got = LocalYearStore.get_ts(attr)
            got = got[got.notnull()]
            pd.testing.assert_series_equal(got, serie)

        # update server
        sere_a1_v2 = self._make_series("2025-02-01", 24 * 31 * 4)
        sere_a2_v2 = self._make_series("2026-03-01", 24 * 31 * 4)
        series2 = (
            ({"version": 1, "kind": "A"}, sere_a1_v2),
            ({"version": 2, "kind": "A"}, sere_a2_v2),
        )
        for attr, serie in series2:
            RemoteYearStore.set_ts(attr, serie, update=True)

        self._sync()

        series_verif = [
            ({"version": 1, "kind": "A"}, sere_a1_v2.combine_first(sere_a1_v1)),
            ({"version": 2, "kind": "A"}, sere_a2_v2.combine_first(sere_a2_v1)),
            series[2], series[3],
        ]
        # vérif intégrité 2
        for attr, serie in series_verif:
            got = LocalYearStore.get_ts(attr)
            got = got[got.notnull()]
            pd.testing.assert_series_equal(got, serie)

    # --------------------------------------------------------------
    @staticmethod
    def _make_series(start, periods, freq="1h", tz="Europe/Paris"):
        idx = pd.date_range(start=start, periods=periods, freq=freq, tz=tz)
        np.random.seed(0)
        return pd.Series(np.random.randn(periods), index=idx)
