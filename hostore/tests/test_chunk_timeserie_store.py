
import numpy as np
import pandas as pd
from django.db import models, connection
from django.test import TestCase, TransactionTestCase

from hostore.models import TimeseriesChunkStore
from hostore.utils.timeseries import ts_combine_first


class TestStoreChunkYearMonth(TimeseriesChunkStore):
    version = models.IntegerField()
    kind = models.CharField(max_length=50)
    CHUNK_AXIS = ('year', 'month')

    class Meta:
        app_label = "ts_inline"
        managed = True
        unique_together = ("version", "kind", "chunk_year", "chunk_month")


class TestStoreChunkYear(TimeseriesChunkStore):
    version = models.IntegerField()
    kind = models.CharField(max_length=50)
    CHUNK_AXIS = ('year', )

    class Meta:
        app_label = "ts_inline"
        managed = True
        unique_together = ("version", "kind", "chunk_year",)


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

class BaseTimeseriesChunkStoreTestCase(TransactionTestCase):
    test_table = None
    year_count_expected = None

    # -------------------------------------------------------------------
    # création des tables à chaque test
    # -------------------------------------------------------------------

    def _ensure_tables(self):
        """(Re)crée les tables manquantes après un flush Django."""
        existing = connection.introspection.table_names()
        if self.test_table is None:
            raise ValueError('test_table is None')
        with connection.schema_editor(atomic=False) as se:
            if self.test_table._meta.db_table not in existing:
                se.create_model(self.test_table)

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
        self.test_table.set_ts(attrs, serie)
        got = self.test_table.get_ts(attrs)
        pd.testing.assert_series_equal(got, serie)
        self.assertGreaterEqual(self.test_table.objects.filter(**attrs).count(), self.year_count_expected)

    def test_range_filter(self):
        serie = make_series("2019-01-01", 24 * 365)
        attrs = {"version": 3, "kind": "C"}
        self.test_table.set_ts(attrs, serie)
        sub = self.test_table.get_ts(attrs, start="2019-06-01", end="2019-06-02")
        expected = serie["2019-06-01":"2019-06-02"]
        pd.testing.assert_series_equal(sub, expected)

    def test_update_and_replace(self):
        """
        ./manage.py test hostore.tests.test_chunk_timeserie_store.BaseTimeseriesChunkStoreTestCase.test_update_and_replace
        """
        s1 = make_series("2022-01-01", 365*24)
        s2 = make_series("2022-06-01", 380*24, seed=42)
        s3 = make_series("2022-06-01", 380*24, seed=42)
        attrs = {"version": 4, "kind": "D"}
        attrs_ot = {"version": 4, "kind": "other"}
        s4 = make_series("2022-06-01", 380*24, seed=42)

        # other
        self.test_table.set_ts(attrs_ot, s4)
        pd.testing.assert_series_equal(self.test_table.get_ts(attrs_ot), s4)

        self.test_table.set_ts(attrs, s1)
        pd.testing.assert_series_equal(self.test_table.get_ts(attrs), s1)

        self.test_table.set_ts(attrs, s2, update=True)
        pd.testing.assert_series_equal(self.test_table.get_ts(attrs), ts_combine_first([s2, s1]), check_freq=False)

        self.test_table.set_ts(attrs, s3, replace=True)
        pd.testing.assert_series_equal(self.test_table.get_ts(attrs), s3)

        # other
        pd.testing.assert_series_equal(self.test_table.get_ts(attrs_ot), s4)

    def test_update_and_replace_simple(self):
        """
        ./manage.py test hostore.tests.test_chunk_timeserie_store.BaseTimeseriesChunkStoreTestCase.test_update_and_replace_simple
        """
        s1 = make_series("2022-01-01", 365*24)
        s2 = make_series("2022-06-01", 380*24, seed=42)
        attrs = {"version": 4, "kind": "D2"}

        self.test_table.set_ts(attrs, s1)
        self.test_table.set_ts(attrs, s2, update=True)

        pd.testing.assert_series_equal(self.test_table.get_ts(attrs), ts_combine_first([s2, s1]), check_freq=False)

    def test_set_many_ts(self):
        mapping = {
            (5, "E"): make_series("2023-01-01", 24),
            (5, "F"): make_series("2023-02-01", 24 * 2),
        }
        self.test_table.set_many_ts(mapping, keys=("version", "kind"))
        for (v, k), serie in mapping.items():
            got = self.test_table.get_ts({"version": v, "kind": k})
            pd.testing.assert_series_equal(got, serie)

    def test_yield_ts(self):
        mapping = {
            (6, "G"): make_series("2024-03-01", 24),
            (6, "H"): make_series("2024-04-01", 24),
        }
        self.test_table.set_many_ts(mapping, keys=("version", "kind"))
        seen = {
            (row.version, row.kind): serie
            for serie, row in self.test_table.yield_ts({"version": 6})
        }
        self.assertEqual(set(seen.keys()), {(6, "G"), (6, "H")})
        for key, serie in seen.items():
            pd.testing.assert_series_equal(serie.tz_convert('Europe/Paris'), mapping[key].tz_convert('Europe/Paris'))

    def test_invalid_calls(self):
        serie = make_series("2025-01-01", 24)
        with self.assertRaises(ValueError):
            self.test_table.set_ts({"version": 7, "kind": "I"}, serie, update=True, replace=True)

        bad = pd.Series([1, 2, 3])
        with self.assertRaises(ValueError):
            self.test_table.set_ts({"version": 8, "kind": "J"}, bad)


class TestTimeseriesWith1ChunkTestCase(BaseTimeseriesChunkStoreTestCase):
    test_table = TestStoreChunkYear
    year_count_expected = 1

class TestTimeseriesWith2ChunkTestCase(BaseTimeseriesChunkStoreTestCase):
    test_table = TestStoreChunkYearMonth
    year_count_expected = 12
