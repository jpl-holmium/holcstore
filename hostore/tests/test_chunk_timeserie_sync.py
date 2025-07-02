import base64, datetime as dt

from django.db import connection, models
from django.test import TransactionTestCase, override_settings
from django.urls import path, include
from rest_framework.routers import SimpleRouter
from rest_framework.test import APIClient
import pandas as pd
import numpy as np
import lz4.frame

from hostore.models import TimeseriesChunkStore
from hostore.utils.ts_sync import TimeseriesChunkStoreSyncViewSet


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
        LocalYearStore.objects.all().delete()
        self.client = APIClient()

    # --------------------------------------------------------------
    def test_full_sync(self):
        # 1) seed serveur
        serie = self._make_series("2025-01-01", 24 * 31)
        attrs = {"version": 1, "kind": "A"}
        RemoteYearStore.set_ts(attrs, serie)

        # 2) client → /updates
        since = pd.Timestamp("2000-01-01", tz="UTC")
        updates = self.client.get("/sync/year/updates/", {"since": since.isoformat()}).json()
        self.assertEqual(len(updates), 1)
        spec = updates

        # 3) client → /pack export
        pack = self.client.post("/sync/year/pack/", spec, format="json").json()
        self.assertEqual(len(pack), 1)

        # 4) import côté local
        tuples = [
            (base64.b64decode(item["blob"]), item["attrs"], item["meta"])
            for item in pack
        ]
        LocalYearStore.import_chunks(tuples)

        # 5) vérif intégrité
        got = LocalYearStore.get_ts(attrs)
        pd.testing.assert_series_equal(got, serie)

    # --------------------------------------------------------------
    @staticmethod
    def _make_series(start, periods, freq="1h", tz="Europe/Paris"):
        idx = pd.date_range(start=start, periods=periods, freq=freq, tz=tz)
        np.random.seed(0)
        return pd.Series(np.random.randn(periods), index=idx)
