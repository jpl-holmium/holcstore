import datetime as dt
import numpy as np
import pandas as pd
from django.db import models, IntegrityError
from django.test import TransactionTestCase

from hostore.models import TimeseriesChunkStore
from hostore.utils.timeseries import ts_combine_first, _localise_date_interval
from hostore.utils.utils_test import TempTestTableHelper


class TestStoreChunkYearMonth(TimeseriesChunkStore):
    version = models.IntegerField()
    kind_very_long_name_for_testing_purpose = models.CharField(max_length=50)
    CHUNK_AXIS = ('year', 'month')

    class Meta:
        app_label = "ts_inline"
        managed = True

class TestStoreChunkYearMonthUTC(TimeseriesChunkStore):
    version = models.IntegerField()
    kind_very_long_name_for_testing_purpose = models.CharField(max_length=50)
    CHUNK_AXIS = ('year', 'month')
    STORE_TZ = 'UTC'

    class Meta:
        app_label = "ts_inline"
        managed = True

class TestStoreChunkYear(TimeseriesChunkStore):
    version = models.IntegerField()
    kind_very_long_name_for_testing_purpose = models.CharField(max_length=50)
    CHUNK_AXIS = ('year', )

    class Meta:
        app_label = "ts_inline"
        managed = True


class TestStoreChunkYearMonthNoAttrs(TimeseriesChunkStore):
    CHUNK_AXIS = ('year', 'month')

    class Meta:
        app_label = "ts_inline"
        managed = True


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

class BaseTimeseriesChunkStoreTestCase(TransactionTestCase, TempTestTableHelper):
    __unittest_skip__ = True

    test_table = None
    year_count_expected = None
    series_na = None
    drop_series_na = False
    no_user_fields = False  # special case with table without any attributes
    input_tz = 'Europe/Paris'

    # -------------------------------------------------------------------
    # création des tables à chaque test
    # -------------------------------------------------------------------

    def setUp(self):
        # Flush de TransactionTestCase supprime nos tables dynamiques.
        # On les recrée si nécessaire avant chaque test.
        self._ensure_tables(delete_kw=dict(keep_tracking=False))

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

    @classmethod
    def make_attrs(cls, orig_attrs):
        if cls.no_user_fields:
            return {}
        else:
            return orig_attrs

    # -------------------------------------------------------------------
    # Scénarios
    # -------------------------------------------------------------------

    def test_set_and_get(self):
        # serie_a only
        serie_a = self.make_series("2020-01-01", 24 * 365)
        attrs = self.make_attrs({"version": 1, "kind_very_long_name_for_testing_purpose": "A"})
        self.test_table.set_ts(attrs, serie_a)
        got = self.test_table.get_ts(attrs)

        # test index metadatas
        self.assertEqual(str(got.index.tz), self.test_table.STORE_TZ)
        self.assertIsInstance(got, pd.Series)
        self.assertIsInstance(got.index, pd.DatetimeIndex)
        self.assertEqual(got.index.freq, self.test_table.STORE_FREQ)

        # test content
        assert_series_equal(got, serie_a)
        self.assertGreaterEqual(self.test_table.objects.filter(**attrs).count(), self.year_count_expected)

        # serie_a + serie_b
        if not self.no_user_fields:
            serie_b = self.make_series("2020-01-01", 24 * 345)
            attrs = self.make_attrs({"version": 1, "kind_very_long_name_for_testing_purpose": "B"})
            self.test_table.set_ts(attrs, serie_b)
            got = self.test_table.get_ts(attrs)
            assert_series_equal(got, serie_b)

            self.assertEqual(self.test_table.get_max_horodate({"version": 1}).isoformat(),
                             serie_a[serie_a.notna()].index.max().tz_convert(self.test_table.STORE_TZ).isoformat())

        # try re set other
        serie_c = self.make_series("2020-01-01", 24 * 365)
        with self.assertRaises(IntegrityError):
            self.test_table.set_ts(attrs, serie_c)

        # nonexistent
        if not self.no_user_fields:
            got = self.test_table.get_ts({"version": 1, "kind_very_long_name_for_testing_purpose": "nonexistent"})
            self.assertEqual(got, None)

    def test_set_and_get_underconstrained(self):
        # "under constrained" request - secured by _ensure_all_attrs_specified
        if self.no_user_fields:
            return

        serie_a = self.make_series("2020-01-01", 24 * 365)
        attrs = self.make_attrs({"version": 1})
        with self.assertRaises(ValueError):
            self.test_table.set_ts(attrs, serie_a)

        with self.assertRaises(ValueError):
            got = self.test_table.get_ts(attrs)

    def test_set_and_get_full_nan(self):
        serie = self.make_series("2020-01-01", 24 * 365, full_nan=True)
        attrs = self.make_attrs({"version": 1, "kind_very_long_name_for_testing_purpose": "Anan"})
        self.test_table.set_ts(attrs, serie)
        got = self.test_table.get_ts(attrs)
        self.assertEqual(got, None)
        self.assertEqual(self.test_table.get_max_horodate(attrs), None)

    def test_range_filter(self):
        serie = self.make_series("2019-01-01", 24 * 365)
        attrs = self.make_attrs({"version": 3, "kind_very_long_name_for_testing_purpose": "C"})
        self.test_table.set_ts(attrs, serie)
        start, end = _localise_date_interval(dt.date(2019, 6, 1), dt.date(2019, 6, 2))
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
        self._abstract_test_update_and_replace(s1, s2, s3)

    def test_update_and_replace_discontinuous(self):
        s1 = self.make_series("2022-01-01", 24 * 10)
        s2 = self.make_series("2022-01-15", 24 * 5, seed=42)
        s3 = self.make_series("2022-06-01", 24 * 280, seed=42)
        self._abstract_test_update_and_replace(s1, s2, s3)

    def _abstract_test_update_and_replace(self, s1, s2, s3):
        """
        s1 : first set
        s2 : update s1
        s3 : replace s2
        """
        # other serie
        attrs = self.make_attrs({"version": 4, "kind_very_long_name_for_testing_purpose": "D"})
        attrs_ot = {"version": 4, "kind_very_long_name_for_testing_purpose": "other"}
        s4 = self.make_series("2022-06-01", 380*24, seed=42)

        # other
        if not self.no_user_fields:
            self.test_table.set_ts(attrs_ot, s4)
            assert_series_equal(self.test_table.get_ts(attrs_ot), s4)

        self.test_table.set_ts(attrs, s1)
        assert_series_equal(self.test_table.get_ts(attrs), s1)
        horomax = s1[s1.notna()].index.max().tz_convert(self.test_table.STORE_TZ)
        self.assertEqual(self.test_table.get_max_horodate(attrs).isoformat(),
                         horomax.isoformat())

        self.test_table.set_ts(attrs, s2, update=True)
        assert_series_equal(self.test_table.get_ts(attrs), ts_combine_first([s2, s1]))
        self.assertEqual(self.test_table.get_max_horodate(attrs).isoformat(),
                         max(
                             horomax,
                             s2[s2.notna()].index.max().tz_convert(self.test_table.STORE_TZ)
                         ).isoformat())
        self.test_table.set_ts(attrs, s3, replace=True)
        assert_series_equal(self.test_table.get_ts(attrs), s3)

        # other
        if not self.no_user_fields:
            assert_series_equal(self.test_table.get_ts(attrs_ot), s4)

    def test_update_and_replace_simple(self):
        """
        ./manage.py test hostore.tests.test_chunk_timeserie_store.BaseTimeseriesChunkStoreTestCase.test_update_and_replace_simple
        """
        s1 = self.make_series("2022-01-01", 365*24)
        s2 = self.make_series("2022-06-01", 380*24, seed=42)
        s3 = self.make_series("2022-06-01", 381*24, seed=43)
        s4 = self.make_series("2022-06-01", 420*24, seed=44)
        s5 = self.make_series("2026-06-01", 365*24, seed=44)
        attrs = self.make_attrs({"version": 4, "kind_very_long_name_for_testing_purpose": "D2"})

        self.test_table.set_ts(attrs, s1)
        self.test_table.set_ts(attrs, s2, update=True)
        assert_series_equal(self.test_table.get_ts(attrs), ts_combine_first([s2, s1]))
        self.test_table.set_ts(attrs, s3, update=True)
        assert_series_equal(self.test_table.get_ts(attrs), ts_combine_first([s3, s2, s1]))
        self.test_table.set_ts(attrs, s4, update=True)
        assert_series_equal(self.test_table.get_ts(attrs), ts_combine_first([s4, s3, s2, s1]))
        self.test_table.set_ts(attrs, s5, update=True)
        assert_series_equal(self.test_table.get_ts(attrs), ts_combine_first([s5, s4, s3, s2, s1]))

    def test_set_many_ts(self):
        if self.no_user_fields:
            return
        mapping = {
            (5, "E"): self.make_series("2023-01-01", 24),
            (5, "F"): self.make_series("2023-02-01", 24 * 2),
        }
        self.test_table.set_many_ts(mapping, keys=("version", "kind_very_long_name_for_testing_purpose"))
        for (v, k), serie in mapping.items():
            got = self.test_table.get_ts({"version": v, "kind_very_long_name_for_testing_purpose": k})
            assert_series_equal(got, serie)

        with self.assertRaises(IntegrityError):
            self.test_table.set_many_ts(mapping, keys=("version", "kind_very_long_name_for_testing_purpose"))
    def test_yield_ts(self):
        if self.no_user_fields:
            return

        mapping = {
            (6, "G"): self.make_series("2024-01-01", 24*390),
            (6, "H"): self.make_series("2024-01-01", 24*390),
            (6, "I"): self.make_series("2023-01-01", 24*366),
            (5, "G"): self.make_series("2028-01-01", 24*15),
        }
        self.test_table.set_many_ts(mapping, keys=("version", "kind_very_long_name_for_testing_purpose"))
        seen = {
            (key_dict['version'], key_dict['kind_very_long_name_for_testing_purpose']): serie
            for serie, key_dict in self.test_table.yield_many_ts({"version": 6})
        }
        self.assertEqual(set(seen.keys()), {(6, "G"), (6, "H"), (6, "I")})
        for key, serie in seen.items():
            assert_series_equal(serie, mapping[key])

    def test_invalid_calls(self):
        serie = self.make_series("2025-01-01", 24)
        with self.assertRaises(ValueError):
            self.test_table.set_ts({"version": 7, "kind_very_long_name_for_testing_purpose": "I"}, serie, update=True, replace=True)

        bad = pd.Series([1, 2, 3])
        with self.assertRaises(ValueError):
            self.test_table.set_ts({"version": 8, "kind_very_long_name_for_testing_purpose": "J"}, bad)


class TestTimeseries_1ChunkTestCase(BaseTimeseriesChunkStoreTestCase):
    """
    Test chunk Year
    """
    __unittest_skip__ = False
    test_table = TestStoreChunkYear
    year_count_expected = 1

class TestTimeseries_2ChunkTestCase(BaseTimeseriesChunkStoreTestCase):
    """
    Test chunk Year Month
    """
    __unittest_skip__ = False
    test_table = TestStoreChunkYearMonth
    year_count_expected = 12

class TestTimeseries_2Chunk_NoAttrs_TestCase(BaseTimeseriesChunkStoreTestCase):
    """
    Test chunk Year Month
    """
    __unittest_skip__ = False
    test_table = TestStoreChunkYearMonthNoAttrs
    year_count_expected = 12
    no_user_fields = True

class TestTimeseries_2ChunkUTCTestCase(BaseTimeseriesChunkStoreTestCase):
    """
    Test chunk Year Month
    Table en UTC
    """
    __unittest_skip__ = False
    test_table = TestStoreChunkYearMonthUTC
    year_count_expected = 12

class TestTimeseries_1Chunk_NanTestCase(BaseTimeseriesChunkStoreTestCase):
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

class TestTimeseries_2Chunk_NanTestCase(BaseTimeseriesChunkStoreTestCase):
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

class TestTimeseries_2Chunk_NanUtcTestCase(BaseTimeseriesChunkStoreTestCase):
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

