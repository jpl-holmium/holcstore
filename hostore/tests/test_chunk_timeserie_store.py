
import numpy as np
import pandas as pd
from django.db import models, connection
from django.test import TestCase, TransactionTestCase

from hostore.models import TimeseriesChunkStore
from hostore.utils.timeseries import ts_combine_first


class TestStore(TimeseriesChunkStore):
    version = models.IntegerField()
    kind = models.CharField(max_length=50)
    CHUNK_AXIS = ('year', 'month')

    class Meta:
        app_label = "ts_inline"
        managed = True
        unique_together = ("version", "kind", "chunk_year", "chunk_month")


class NoChunkStore(TimeseriesChunkStore):
    version = models.IntegerField()
    kind = models.CharField(max_length=50)
    CHUNK_AXIS = ()  # désactive le découpage temporel

    class Meta:
        app_label = "ts_inline"
        managed = True
        unique_together = ("version", "kind")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_series(start, periods, freq="1h", tz="Europe/Paris", seed=0):
    """Génère une série aléatoire de longueur `periods`."""
    rng = pd.date_range(start=start, periods=periods, freq=freq, tz=tz)
    np.random.seed(seed)
    return pd.Series(np.random.randn(periods), index=rng)


# ---------------------------------------------------------------------------
# Tests principaux
# ---------------------------------------------------------------------------

class TimeseriesChunkStoreTestCase(TransactionTestCase):
    """Tests end‑to‑end : insert → fetch → update …"""

    # -------------------------------------------------------------------
    # Infrastructure : création des tables à chaque test
    # -------------------------------------------------------------------

    def _ensure_tables(self):
        """(Re)crée les tables manquantes après un flush Django."""
        existing = connection.introspection.table_names()
        with connection.schema_editor(atomic=False) as se:
            if TestStore._meta.db_table not in existing:
                se.create_model(TestStore)
            if NoChunkStore._meta.db_table not in existing:
                se.create_model(NoChunkStore)

    def setUp(self):
        # Flush de TransactionTestCase supprime nos tables dynamiques.
        # On les recrée si nécessaire avant chaque test.
        self._ensure_tables()


    # Django vide et détruit la base de test à la fin du run ;
    # pas besoin de supprimer manuellement les tables inline. Supprimer
    # la méthode `tearDownClass` évite les erreurs « no such table ».
    pass

    # -------------------------------------------------------------------
    # Scénarios
    # -------------------------------------------------------------------

    def test_set_and_get(self):
        serie = make_series("2020-01-01", 24 * 365)
        attrs = {"version": 1, "kind": "A"}
        TestStore.set_ts(attrs, serie)
        got = TestStore.get_ts(attrs)
        pd.testing.assert_series_equal(got, serie)
        self.assertGreaterEqual(TestStore.objects.filter(**attrs).count(), 12)

    def test_range_filter(self):
        serie = make_series("2019-01-01", 24 * 365)
        attrs = {"version": 3, "kind": "C"}
        TestStore.set_ts(attrs, serie)
        sub = TestStore.get_ts(attrs, start="2019-06-01", end="2019-06-02")
        expected = serie["2019-06-01":"2019-06-02"]
        pd.testing.assert_series_equal(sub, expected)

    def test_update_and_replace(self):
        s1 = make_series("2022-01-01", 365*24)
        s2 = make_series("2022-06-01", 380*24, seed=42)
        attrs = {"version": 4, "kind": "D"}
        s3 = make_series("2022-06-01", 380*24, seed=42)
        attrs_ot = {"version": 4, "kind": "other"}

        # other
        TestStore.set_ts(attrs_ot, s3)
        pd.testing.assert_series_equal(TestStore.get_ts(attrs_ot), s3)

        TestStore.set_ts(attrs, s1)
        pd.testing.assert_series_equal(TestStore.get_ts(attrs), s1)

        TestStore.set_ts(attrs, s2, update=True)
        pd.testing.assert_series_equal(TestStore.get_ts(attrs), ts_combine_first([s2, s1]), check_freq=False)

        TestStore.set_ts(attrs, s2, replace=True)
        pd.testing.assert_series_equal(TestStore.get_ts(attrs), s2)

        # other
        pd.testing.assert_series_equal(TestStore.get_ts(attrs_ot), s3)

    def test_set_many_ts(self):
        mapping = {
            (5, "E"): make_series("2023-01-01", 24),
            (5, "F"): make_series("2023-02-01", 24 * 2),
        }
        NoChunkStore.set_many_ts(mapping, keys=("version", "kind"))
        for (v, k), serie in mapping.items():
            got = NoChunkStore.get_ts({"version": v, "kind": k})
            pd.testing.assert_series_equal(got, serie)

    def test_yield_ts(self):
        mapping = {
            (6, "G"): make_series("2024-03-01", 24),
            (6, "H"): make_series("2024-04-01", 24),
        }
        NoChunkStore.set_many_ts(mapping, keys=("version", "kind"))
        seen = {
            (row.version, row.kind): serie
            for serie, row in NoChunkStore.yield_ts({"version": 6})
        }
        self.assertEqual(set(seen.keys()), {(6, "G"), (6, "H")})
        for key, serie in seen.items():
            print(serie, mapping[key])
            print(serie - mapping[key])
            print(serie.dtype)
            print(mapping[key].dtype)
            print("serie.index")
            print(serie.index)
            print(mapping[key].index)
            pd.testing.assert_series_equal(serie.tz_convert('Europe/Paris'), mapping[key].tz_convert('Europe/Paris'))

    def test_invalid_calls(self):
        serie = make_series("2025-01-01", 24)
        with self.assertRaises(ValueError):
            TestStore.set_ts({"version": 7, "kind": "I"}, serie, update=True, replace=True)

        bad = pd.Series([1, 2, 3])
        with self.assertRaises(ValueError):
            NoChunkStore.set_ts({"version": 8, "kind": "J"}, bad)
