import io
import random
import datetime as dt
import numpy as np
import pandas as pd
import pytz
from django.test import TestCase

from hostore.models import TestTimeseriesStoreWithAttribute


# from hostore.models import TestTimeseriesStoreWithAttribute


# ./manage.py test hostore.tests.tests_timeseries_store.TimeseriesCacheTestCase

def gen_serie(start, end, data, freq='1h'):
    dt_rng = pd.date_range(start, end, freq=freq)
    return pd.Series(data, index=dt_rng, name='data')


class TimeseriesCacheTestCase(TestCase):
    databases = ('default',)

    def setUp(self):
        ts_attrs_y_2020_kind_a = dict(year=2020, kind='a')
        ds_y_2020_kind_a = gen_serie("2020-01-01 00:00:00+00:00", "2020-01-01 02:00:00+00:00", [1, 2, 3])
        TestTimeseriesStoreWithAttribute.set_ts(ts_attrs_y_2020_kind_a, ds_y_2020_kind_a)
        self.ts_attrs_y_2020_kind_a = ts_attrs_y_2020_kind_a
        self.ds_y_2020_kind_a = ds_y_2020_kind_a

        ts_attrs_y_2020_kind_b = dict(year=2020, kind='b')
        ds_y_2020_kind_b = gen_serie("2020-01-01 00:00:00+01:00", "2020-01-01 02:00:00+01:00", [10, 20, 30])
        TestTimeseriesStoreWithAttribute.set_ts(ts_attrs_y_2020_kind_b, ds_y_2020_kind_b)
        self.ts_attrs_y_2020_kind_b = ts_attrs_y_2020_kind_b
        self.ds_y_2020_kind_b = ds_y_2020_kind_b
        
        ts_attrs_y_2024_kind_a = dict(year=2024, kind='a')
        ds_y_2024_kind_a = gen_serie("2024-01-01 00:00:00+00:00", "2024-01-01 02:00:00+00:00", [11, 21, 31])
        TestTimeseriesStoreWithAttribute.set_ts(ts_attrs_y_2024_kind_a, ds_y_2024_kind_a)
        self.ts_attrs_y_2024_kind_a = ts_attrs_y_2024_kind_a
        self.ds_y_2024_kind_a = ds_y_2024_kind_a
        
    def test_set_get_w_attributes(self):

        # test get - 2020 a
        data = TestTimeseriesStoreWithAttribute.get_ts(self.ts_attrs_y_2020_kind_a)
        pd.testing.assert_series_equal(data[0]['data'], self.ds_y_2020_kind_a, check_names=False, check_freq=False)

        # test get - 2020 a - flat
        ds = TestTimeseriesStoreWithAttribute.get_ts(self.ts_attrs_y_2020_kind_a, flat=True)
        pd.testing.assert_series_equal(ds, self.ds_y_2020_kind_a, check_names=False, check_freq=False)

        # test get - 2020 b (tz +1)
        data = TestTimeseriesStoreWithAttribute.get_ts(self.ts_attrs_y_2020_kind_b)
        ds = data[0]['data']
        ds = ds
        # we need to tz_convert, otherwise comparison fails :
        # Attribute "dtype" are different
        # [left]:  datetime64[ns, pytz.FixedOffset(60)]
        # [right]: datetime64[ns, UTC+01:00]
        ds = ds.tz_convert('utc')
        ds_y_2020_kind_b = self.ds_y_2020_kind_b.tz_convert('utc')
        pd.testing.assert_series_equal(
           ds, ds_y_2020_kind_b, check_names=False, check_freq=False)

        # test get - 2024 a
        data = TestTimeseriesStoreWithAttribute.get_ts(self.ts_attrs_y_2024_kind_a)
        pd.testing.assert_series_equal(data[0]['data'], self.ds_y_2024_kind_a, check_names=False, check_freq=False)

    def test_set_get_w_attributes_multiple(self):
        # check we found the 2 series registered as kind a
        data = TestTimeseriesStoreWithAttribute.get_ts(dict(kind='a'))
        assert len(data) == 2
        assert data[0]['year'] == 2020
        assert data[1]['year'] == 2024

        # check flat option fails (multiple series for a request)
        with self.assertRaises(ValueError):
            data = TestTimeseriesStoreWithAttribute.get_ts(dict(kind='a'), flat=True)

    def test_set_get_w_nodata(self):
        # check we found the 2 series registered as kind a
        data = TestTimeseriesStoreWithAttribute.get_ts(dict(kind='z'))
        assert len(data) == 0

        # check flat option fails (no series for a request)
        with self.assertRaises(ValueError):
            data = TestTimeseriesStoreWithAttribute.get_ts(dict(kind='z'), flat=True)
