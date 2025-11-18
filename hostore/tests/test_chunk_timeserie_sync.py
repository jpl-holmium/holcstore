import base64, datetime as dt
from unittest.mock import patch

import requests
from django.db import connection, models, IntegrityError
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
    ALLOW_CLIENT_SERVER_SYNC = True


class ClientStore(TimeseriesChunkStore):
    """Table côté ‘client’"""
    version = models.IntegerField()
    kind    = models.CharField(max_length=20)
    ALLOW_CLIENT_SERVER_SYNC = True


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
        ServerStore.objects.all().delete(_disable_sync_safety=True)
        self.sync_client = TimeseriesChunkStoreSyncClient(endpoint='http://testserver/sync/year', store_model=ClientStore)
        ClientStore.objects.all().delete(_disable_sync_safety=True)
        self.req_client = RequestsClient()
        self.req_client.base_url = "http://testserver/"
        # self.client = APIClient()


    def _sync(self, filters=None, **kwargs):
        with (
            patch.object(requests, "get",  self.req_client.get),
            # patch.object(requests, "post", self.req_client.post),
            # patch.object(requests, "put",  self.req_client.put),
        ):
            call_kwargs = {"batch": 20, "filters": filters, "page_size": 4}
            call_kwargs.update(kwargs)
            return self.sync_client.pull(**call_kwargs)

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
            ServerStore.set_ts(attr, serie, replace=True)
        self._sync()
        self._assert_stores_equal()

        # =========== TEST UPDATE + SYNC PARTIEL / TOTAL
        # update server
        serie_a1_v2 = self._make_series("2025-02-28", 24 * 31 * 4)
        serie_a2_v2 = self._make_series("2026-03-01", 24 * 31 * 4)
        series2 = (
            ({"version": 1, "kind": "A"}, serie_a1_v2),
            ({"version": 2, "kind": "A"}, serie_a2_v2),
        )
        for attr, serie in series2:
            ServerStore.set_ts(attr, serie, update=True)

        # sync kind B : no changes for kind A on client side
        n_fetch, n_del = self._sync(filters={"kind": "B"})
        self.assertEqual(n_fetch, 1)  # gte => 1 chunk
        self.assertEqual(n_del, 0)
        self._assert_stores_equal(filters={"kind": "B"})

        # check no changes for kind A on client side
        for attr, serie in series:
            got = ClientStore.get_ts(attr)
            got = got[got.notnull()]
            assert_series_equal(got, serie)

        # sync all
        self._sync()
        self._assert_stores_equal()

        # manual check of content, in duplicate with previous _assert_stores_equal
        series_verif = [
            ({"version": 1, "kind": "A"}, serie_a1_v2.combine_first(sere_a1_v1)),
            ({"version": 2, "kind": "A"}, serie_a2_v2.combine_first(sere_a2_v1)),
            series[2], series[3],
        ]
        for attr, serie in series_verif:
            got = ClientStore.get_ts(attr)
            got = got[got.notnull()]
            assert_series_equal(got, serie)

        # =========== TEST SUPPRESSION
        # delete one object
        first = ServerStore.objects.filter(version=1).first()
        first.delete(keep_tracking=True)
        first.refresh_from_db()
        self.assertEqual(first.is_deleted, True)
        # delete qs
        ServerStore.objects.filter(version=1).delete(keep_tracking=True)

        self._sync()
        self._assert_stores_equal()
        self.assertFalse(ClientStore.objects.filter(version=1, is_deleted=False).exists())

        # =========== TEST REPLACE
        # replace server
        series3 = (
            ({"version": 1, "kind": "A"}, self._make_series("2025-03-01", 24 * 31 * 4)),
            ({"version": 2, "kind": "A"}, self._make_series("2026-04-01", 24 * 31 * 4)),
            ({"version": 2, "kind": "new_one"}, self._make_series("2025-04-01", 24 * 31)),
        )
        for attr, serie in series3:
            ServerStore.set_ts(attr, serie, replace=True)

        self._sync()
        self._assert_stores_equal()

        # =========== TEST DELETE SET MANY
        # delete without tracking on server side WILL lead to inconsistent results client side
        # => forbidden
        with self.assertRaises(ValueError):
            ServerStore.objects.all().delete(keep_tracking=False)

        # tracked model => you cannot set
        attr, serie = series[0]
        with self.assertRaises(ValueError):
            ServerStore.set_ts(attr, serie)

        # tracked model => you cannot set_many_ts
        mapping = {
            (k['version'], k['kind']): ds for k, ds in series3
        }
        with self.assertRaises(ValueError):
            ServerStore.set_many_ts(mapping, keys=("version", "kind"))

        # some legal update of series
        series4 = (
            ({"version": 1, "kind": "A"}, self._make_series("2025-03-01", 24 * 31 * 6)),
            ({"version": 2, "kind": "A"}, self._make_series("2026-04-01", 24 * 31 * 4)),
            ({"version": 2, "kind": "new_one"}, self._make_series("2025-04-01", 24 * 31 * 7)),
            ({"version": 2, "kind": "new_one2"}, self._make_series("2025-04-01", 24 * 31 * 7)),
            ({"version": 42, "kind": "d"}, self._make_series("1780-04-01", 24 * 31 * 7)),
        )
        for attr, serie in series4:
            ServerStore.set_ts(attr, serie, update=True)
        self._sync()
        self._assert_stores_equal()

        # =========== TEST DELETE FILTER
        ServerStore.objects.filter(kind="A").delete(keep_tracking=True)
        self._sync()
        self._assert_stores_equal()

        # =========== TEST DELETE ALL
        ServerStore.objects.all().delete(keep_tracking=True)
        self._sync()
        self._assert_stores_equal()


    # --------------------------------------------------------------
    @staticmethod
    def _make_series(start, periods, freq="1h", tz="Europe/Paris"):
        idx = pd.date_range(start=start, periods=periods, freq=freq, tz=tz)
        np.random.seed(0)
        return pd.Series(np.random.randn(periods), index=idx)

    def _assert_stores_equal(self, filters=None):
        filters = filters or {}
        seen_local = {
            (key_dict['version'], key_dict['kind']): serie
            for serie, key_dict in ClientStore.yield_many_ts(filters)
        }
        seen_remote = {
            (key_dict['version'], key_dict['kind']): serie
            for serie, key_dict in ServerStore.yield_many_ts(filters)
        }

        self.assertEqual(seen_local.keys(), seen_remote.keys())
        for k in seen_local.keys():
            assert_series_equal(seen_local[k], seen_remote[k])

    def test_partial_sync_crash_resumes(self):
        # Seed server with two series and sync once
        series = (
            ({"version": 1, "kind": "A"}, self._make_series("2025-01-01", 24)),
            ({"version": 2, "kind": "A"}, self._make_series("2025-01-02", 24)),
        )
        for attrs, serie in series:
            ServerStore.set_ts(attrs, serie, replace=True)
        self._sync()

        # Update both series on server to create two updates
        updated_series = (
            ({"version": 1, "kind": "A"}, self._make_series("2025-01-01", 48)),
            ({"version": 2, "kind": "A"}, self._make_series("2025-01-02", 48)),
        )
        for attrs, serie in updated_series:
            ServerStore.set_ts(attrs, serie, update=True)

        original_import = ClientStore.import_chunks.__func__
        call_count = {"value": 0}

        def crashing_import(cls, payload):
            call_count["value"] += 1
            original_import(cls, payload)
            if call_count["value"] == 1:
                raise RuntimeError("boom")

        with patch.object(ClientStore, "import_chunks", new=classmethod(crashing_import)):
            with self.assertRaises(RuntimeError):
                self._sync(batch=1)

        # Ensure only one update made it through
        self.assertEqual(call_count["value"], 1)

        # Second sync (without crashing) should resume and finish importing
        fetch, delete = self._sync()
        self.assertEqual(fetch, 2)
        self.assertEqual(delete, 0)
        self._assert_stores_equal()

    def test_pull_uses_filters_for_since(self):
        series = (
            ({"version": 1, "kind": "A"}, self._make_series("2025-01-01", 24)),
            ({"version": 1, "kind": "B"}, self._make_series("2025-01-01", 24)),
        )
        for attrs, serie in series:
            ServerStore.set_ts(attrs, serie, replace=True)

        called_with = {}

        original_last_updated = ClientStore.last_updated_at.__func__

        def wrapped_last_updated(cls, filters=None):
            called_with["filters"] = filters
            return original_last_updated(cls, filters)

        with patch.object(ClientStore, "last_updated_at", new=classmethod(wrapped_last_updated)):
            self._sync(filters={"kind": "A"})

        self.assertEqual(called_with["filters"], {"kind": "A"})


def _display_table_content(table, filters=None):
    """ debug tool to analyse table content (without data)"""
    filters = filters or {}
    df = pd.DataFrame(table.objects.filter(**filters).values())
    if 'data' in df.columns:
        df.drop(['data'], inplace=True, axis=1)
    print(f'Raw table content for {table.__name__}:')
    print(df.sort_values(list(df.columns)).to_markdown())