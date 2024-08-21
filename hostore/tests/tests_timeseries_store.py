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

    def test_set_get_w_attributes(self):
        ds_y_2020_kind_a = gen_serie("2020-01-01 00:00:00+00:00", "2020-01-01 02:00:00+00:00", [1, 2, 3])
        TestTimeseriesStoreWithAttribute.set_ts(dict(year=2020, kind='a'), ds_y_2020_kind_a)

        # ds_y_2020_kind_b = gen_serie("2020-01-01 00:00:00+00:00", "2020-01-01 02:00:00+00:00", [11, 22, 33])
        # TestTimeseriesStoreWithAttribute.set_ts(dict(year=2020, kind='b'), ds_y_2020_kind_b)
        #
        # ds_y_2024_kind_a = gen_serie("2024-01-01 00:00:00+00:00", "2024-01-01 02:00:00+00:00", [10, 20, 30])
        # TestTimeseriesStoreWithAttribute.set_ts(dict(year=2024, kind='a'), ds_y_2024_kind_a)

        data = TestTimeseriesStoreWithAttribute.get_ts(dict(year=2020, kind='a'))

        pd.testing.assert_series_equal(data[0]['data'], ds_y_2020_kind_a, check_names=False, check_freq=False)

        # TODO verifier qu'on est pas oblige de faire un set et get index dans le store
