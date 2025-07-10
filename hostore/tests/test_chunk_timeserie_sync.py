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
from hostore.tests.test_chunk_timeserie_store import assert_series_equal
from hostore.utils.ts_sync import TimeseriesChunkStoreSyncViewSet, TimeseriesChunkStoreSyncClient


# ------------------------------------------------------------------
#  Modèles “remote” et “local” (mêmes champs, tables distinctes)
# ------------------------------------------------------------------
class ServerStore(TimeseriesChunkStore):
    """Table côté ‘serveur’"""
    version = models.IntegerField()
    kind    = models.CharField(max_length=20)

    class Meta(TimeseriesChunkStore.Meta):
        app_label   = "ts_remote"
        db_table    = "ts_remote_year"


class ClientStore(TimeseriesChunkStore):
    """Table côté ‘client’"""
    version = models.IntegerField()
    kind    = models.CharField(max_length=20)

    class Meta(TimeseriesChunkStore.Meta):
        app_label   = "ts_local"
        db_table    = "ts_local_year"


# ------------------------------------------------------------------
#  ViewSet serveur (lié au ServerStore)
# ------------------------------------------------------------------
RemoteSyncViewSet = TimeseriesChunkStoreSyncViewSet.as_factory(ServerStore)
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
            for mdl in (ServerStore, ClientStore):
                if mdl._meta.db_table not in connection.introspection.table_names():
                    se.create_model(mdl)
        ServerStore.objects.all().delete()
        self.sync_client = TimeseriesChunkStoreSyncClient(endpoint='http://testserver/sync/year', store_model=ClientStore)
        ClientStore.objects.all().delete()
        self.req_client = RequestsClient()
        self.req_client.base_url = "http://testserver/"
        # self.client = APIClient()

    def _sync(self, filters=None):
        with (
            patch.object(requests, "get",  self.req_client.get),
            patch.object(requests, "post", self.req_client.post),
            patch.object(requests, "put",  self.req_client.put),
        ):
            self.sync_client.pull(batch=20, filters=filters)

    # --------------------------------------------------------------
    def test_full_sync_and_update(self):
        # =========== TEST BASE
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
            ServerStore.set_ts(attr, serie)
        self._sync()

        # vérif intégrité 1
        for attr, serie in series:
            got = ClientStore.get_ts(attr)
            got = got[got.notnull()]
            assert_series_equal(got, serie)

        # =========== TEST UPDATE + SYNC PARTIEL / TOTAL
        # update server
        sere_a1_v2 = self._make_series("2025-02-01", 24 * 31 * 4)
        sere_a2_v2 = self._make_series("2026-03-01", 24 * 31 * 4)
        series2 = (
            ({"version": 1, "kind": "A"}, sere_a1_v2),
            ({"version": 2, "kind": "A"}, sere_a2_v2),
        )
        for attr, serie in series2:
            ServerStore.set_ts(attr, serie, update=True)

        # sync kind B : no changes
        self._sync(filters={"kind": "B"})

        # vérif intégrité 1 bis
        for attr, serie in series:
            got = ClientStore.get_ts(attr)
            got = got[got.notnull()]
            assert_series_equal(got, serie)

        # sync all
        self._sync(filters={"kind": "A"})
        # vérif intégrité 2
        series_verif = [
            ({"version": 1, "kind": "A"}, sere_a1_v2.combine_first(sere_a1_v1)),
            ({"version": 2, "kind": "A"}, sere_a2_v2.combine_first(sere_a2_v1)),
            series[2], series[3],
        ]
        for attr, serie in series_verif:
            got = ClientStore.get_ts(attr)
            got = got[got.notnull()]
            assert_series_equal(got, serie)

        # =========== TEST SUPPRESSION
        first = ServerStore.objects.filter(version=1).first()
        first.delete()
        first.refresh_from_db()
        self.assertEqual(first.is_deleted, True)
        ServerStore.objects.filter(version=1).delete()

        self._sync()
        seen_local = {
            (key_dict['version'], key_dict['kind']): serie
            for serie, key_dict in ClientStore.yield_many_ts({})
        }
        seen_remote = {
            (key_dict['version'], key_dict['kind']): serie
            for serie, key_dict in ServerStore.yield_many_ts({})
        }

        self.assertEqual(seen_local.keys(), seen_remote.keys())
        for k in seen_local.keys():
            assert_series_equal(seen_local[k], seen_remote[k])

        self.assertFalse(ClientStore.objects.filter(version=1, is_deleted=False).exists())

    # --------------------------------------------------------------
    @staticmethod
    def _make_series(start, periods, freq="1h", tz="Europe/Paris"):
        idx = pd.date_range(start=start, periods=periods, freq=freq, tz=tz)
        np.random.seed(0)
        return pd.Series(np.random.randn(periods), index=idx)


def _display_table_content(table, filters=None):
    """ debug tool to analyse table content (without data)"""
    filters = filters or {}
    df = pd.DataFrame(table.objects.filter(**filters).values())
    df.drop(['data'], inplace=True, axis=1)
    print(f'Raw table content for {table.__name__}:')
    print(df.sort_values(list(df.columns)).to_markdown())