import datetime as dt
import numpy as np
import pandas as pd
import pytz
from django.db import models, connection
from django.test import TransactionTestCase

from hostore.models import TimeseriesChunkStore
from hostore.utils.timeseries import ts_combine_first


TZ_PARIS = 'Europe/Paris'

def localise_date(pydate, time=dt.time(), timezone_name=TZ_PARIS):
    if timezone_name is None:
        return dt.datetime.combine(pydate, time)
    else:
        return pytz.timezone(timezone_name).localize(dt.datetime.combine(pydate, time))

def localise_date_interval(date_start, date_end, timezone_name=TZ_PARIS):
    return (localise_date(date_start, timezone_name=timezone_name),
            localise_date(date_end, time=dt.time.max, timezone_name=timezone_name))


class TestStoreChunkYearMonth(TimeseriesChunkStore):
    version = models.IntegerField()
    kind = models.CharField(max_length=50)
    CHUNK_AXIS = ('year', 'month')

    class Meta:
        app_label = "ts_inline"
        managed = True
        unique_together = ("version", "kind", "chunk_index")


class TestStoreChunkYear(TimeseriesChunkStore):
    version = models.IntegerField()
    kind = models.CharField(max_length=50)
    CHUNK_AXIS = ('year', )

    class Meta:
        app_label = "ts_inline"
        managed = True
        unique_together = ("version", "kind", "chunk_index",)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def assert_series_equal(s1, s2, **kwargs):
    tz_check = 'UTC'
    s1 = s1.dropna()
    s2 = s2.dropna()
    s1 = s1.tz_convert(tz_check)
    s2 = s2.tz_convert(tz_check)
    try:
        pd.testing.assert_series_equal(s1, s2, **kwargs, check_freq=False)
    except AssertionError as e:
        diff = s1-s2
        diff = diff.loc[diff != 0]
        print('assertion error with series diff')
        print(diff)
        raise e

# ---------------------------------------------------------------------------
# Tests principaux
# ---------------------------------------------------------------------------

class BaseTimeseriesChunkStoreTestCase(TransactionTestCase):
    __unittest_skip__ = True

    test_table = None
    year_count_expected = None
    series_na = None
    drop_series_na = False
    safe_insertion = False
    input_tz = 'Europe/Paris'

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
        self.test_table.objects.all().delete()

    def setUp(self):
        # Flush de TransactionTestCase supprime nos tables dynamiques.
        # On les recrée si nécessaire avant chaque test.
        self._ensure_tables()

    @classmethod
    def make_series(cls, start, periods, freq="1h", seed=0, full_nan=False):
        """Génère une série aléatoire de longueur `periods`."""
        rng = pd.date_range(start=start, periods=periods, freq=freq, tz=cls.input_tz)
        if full_nan:
            return pd.Series(None, index=rng)
        np.random.seed(seed)
        ds = pd.Series(np.random.randn(periods), index=rng)
        if cls.series_na is not None:
            for imin, imax in cls.series_na:
                ds.iloc[imin:imax] = None

        if cls.drop_series_na:
            ds.dropna(inplace=True)
        return ds

    # -------------------------------------------------------------------
    # Scénarios
    # -------------------------------------------------------------------

    def test_set_and_get(self):
        # serie_a only
        serie_a = self.make_series("2020-01-01", 24 * 365)
        attrs = {"version": 1, "kind": "A"}
        self.test_table.set_ts(attrs, serie_a, safe_insertion=self.safe_insertion)
        got = self.test_table.get_ts(attrs)
        assert_series_equal(got, serie_a)
        self.assertGreaterEqual(self.test_table.objects.filter(**attrs).count(), self.year_count_expected)

        # serie_a + serie_b
        serie_b = self.make_series("2020-01-01", 24 * 365)
        attrs = {"version": 1, "kind": "B"}
        self.test_table.set_ts(attrs, serie_b, safe_insertion=self.safe_insertion)
        got = self.test_table.get_ts(attrs)
        assert_series_equal(got, serie_b)

        # nonexistent
        got = self.test_table.get_ts({"version": 1, "kind": "nonexistent"})
        self.assertEqual(got, None)

    def test_set_and_get_underconstrained(self):
        # "under constrained" request - secured by _ensure_all_attrs_specified

        serie_a = self.make_series("2020-01-01", 24 * 365)
        attrs = {"version": 1}
        with self.assertRaises(ValueError):
            self.test_table.set_ts(attrs, serie_a, safe_insertion=self.safe_insertion)

        with self.assertRaises(ValueError):
            got = self.test_table.get_ts(attrs)

    def test_set_and_get_full_nan(self):
        serie = self.make_series("2020-01-01", 24 * 365, full_nan=True)
        attrs = {"version": 1, "kind": "Anan"}
        self.test_table.set_ts(attrs, serie, safe_insertion=self.safe_insertion)
        got = self.test_table.get_ts(attrs)
        self.assertEqual(got, None)

    def test_range_filter(self):
        serie = self.make_series("2019-01-01", 24 * 365)
        attrs = {"version": 3, "kind": "C"}
        self.test_table.set_ts(attrs, serie, safe_insertion=self.safe_insertion)
        start, end = localise_date_interval(dt.date(2019,6,1), dt.date(2019,6,2))
        sub = self.test_table.get_ts(attrs, start=start, end=end)
        expected = serie[start:end]
        assert_series_equal(sub, expected)

    def test_update_and_replace(self):
        """
        ./manage.py test hostore.tests.test_chunk_timeserie_store.BaseTimeseriesChunkStoreTestCase.test_update_and_replace
        """
        s1 = self.make_series("2022-01-01", 365*24)
        s2 = self.make_series("2022-06-01", 380*24, seed=42)
        s3 = self.make_series("2022-06-01", 380*24, seed=42)
        attrs = {"version": 4, "kind": "D"}
        attrs_ot = {"version": 4, "kind": "other"}
        s4 = self.make_series("2022-06-01", 380*24, seed=42)

        # other
        self.test_table.set_ts(attrs_ot, s4, safe_insertion=self.safe_insertion)
        assert_series_equal(self.test_table.get_ts(attrs_ot), s4)

        self.test_table.set_ts(attrs, s1, safe_insertion=self.safe_insertion)
        assert_series_equal(self.test_table.get_ts(attrs), s1)

        self.test_table.set_ts(attrs, s2, update=True, safe_insertion=self.safe_insertion)
        assert_series_equal(self.test_table.get_ts(attrs), ts_combine_first([s2, s1]))

        self.test_table.set_ts(attrs, s3, replace=True, safe_insertion=self.safe_insertion)
        assert_series_equal(self.test_table.get_ts(attrs), s3)

        # other
        assert_series_equal(self.test_table.get_ts(attrs_ot), s4)

    def test_update_and_replace_simple(self):
        """
        ./manage.py test hostore.tests.test_chunk_timeserie_store.BaseTimeseriesChunkStoreTestCase.test_update_and_replace_simple
        """
        s1 = self.make_series("2022-01-01", 365*24)
        s2 = self.make_series("2022-06-01", 380*24, seed=42)
        attrs = {"version": 4, "kind": "D2"}

        self.test_table.set_ts(attrs, s1, safe_insertion=self.safe_insertion)
        self.test_table.set_ts(attrs, s2, update=True, safe_insertion=self.safe_insertion)

        assert_series_equal(self.test_table.get_ts(attrs), ts_combine_first([s2, s1]))

    def test_set_many_ts(self):
        mapping = {
            (5, "E"): self.make_series("2023-01-01", 24),
            (5, "F"): self.make_series("2023-02-01", 24 * 2),
        }
        self.test_table.set_many_ts(mapping, keys=("version", "kind"), safe_insertion=self.safe_insertion)
        for (v, k), serie in mapping.items():
            got = self.test_table.get_ts({"version": v, "kind": k})
            assert_series_equal(got, serie)

    def test_yield_ts(self):
        mapping = {
            (6, "G"): self.make_series("2024-01-01", 24*390),
            (6, "H"): self.make_series("2024-01-01", 24*390),
            (6, "I"): self.make_series("2023-01-01", 24*366),
            (5, "G"): self.make_series("2028-01-01", 24*15),
        }
        self.test_table.set_many_ts(mapping, keys=("version", "kind"), safe_insertion=self.safe_insertion)
        seen = {
            (key_dict['version'], key_dict['kind']): serie
            for serie, key_dict in self.test_table.yield_many_ts({"version": 6})
        }
        self.assertEqual(set(seen.keys()), {(6, "G"), (6, "H"), (6, "I")})
        for key, serie in seen.items():
            assert_series_equal(serie, mapping[key])

    def test_invalid_calls(self):
        serie = self.make_series("2025-01-01", 24)
        with self.assertRaises(ValueError):
            self.test_table.set_ts({"version": 7, "kind": "I"}, serie, update=True, replace=True, safe_insertion=self.safe_insertion)

        bad = pd.Series([1, 2, 3])
        with self.assertRaises(ValueError):
            self.test_table.set_ts({"version": 8, "kind": "J"}, bad, safe_insertion=self.safe_insertion)


class TestTimeseriesWith1ChunkTestCase(BaseTimeseriesChunkStoreTestCase):
    """
    Test chunk Year
    """
    __unittest_skip__ = False
    test_table = TestStoreChunkYear
    year_count_expected = 1

class TestTimeseriesWith2ChunkTestCase(BaseTimeseriesChunkStoreTestCase):
    """
    Test chunk Year Month
    """
    __unittest_skip__ = False
    test_table = TestStoreChunkYearMonth
    year_count_expected = 12

class TestTimeseriesWith1ChunkWithNanTestCase(BaseTimeseriesChunkStoreTestCase):
    """
    Test chunk Year
    Series input avec nan
    """
    __unittest_skip__ = False
    test_table = TestStoreChunkYear
    year_count_expected = 1
    series_na = [
        [24*20, 24*60],
        [24*150, 24*200],
        [24*205, 24*220],
    ]

class TestTimeseriesWith2ChunkWithNanTestCase(BaseTimeseriesChunkStoreTestCase):
    """
    Test chunk Year Month
    Series input avec nan
    """
    __unittest_skip__ = False
    test_table = TestStoreChunkYearMonth
    year_count_expected = 12
    series_na = [
        [24*20, 24*60],
        [24*150, 24*200],
        [24*205, 24*220],
    ]

class TestTimeseriesWith2ChunkWithNanUtcTestCase(BaseTimeseriesChunkStoreTestCase):
    """
    Test chunk Year Month
    Series input avec nan, en TZ UTC
    """
    __unittest_skip__ = False
    test_table = TestStoreChunkYearMonth
    year_count_expected = 12
    series_na = [
        [24 * 20, 24 * 60],
        [24 * 150, 24 * 200],
        [24 * 201, 24 * 220],
    ]
    input_tz='UTC'

class TestTimeseriesWith2ChunkWithHoleTestCase(BaseTimeseriesChunkStoreTestCase):
    """
    Test chunk Year Month
    Series input avec trous (index a espacement non constant) => safe_insertion obligatoire
    """
    __unittest_skip__ = False
    test_table = TestStoreChunkYearMonth
    year_count_expected = 12
    series_na = [
        [24 * 20, 24 * 60],
    ]
    # drop_series_na + safe_insertion => fine
    # if a serie is inserted with an incomplete index (holes) user must use safe_insertion option
    drop_series_na = True
    safe_insertion = True
