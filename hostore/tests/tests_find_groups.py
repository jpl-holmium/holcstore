import datetime as dt

import pandas as pd
import pytz
from django.test import TransactionTestCase

from hostore.tests.tests_basics import TestDataStore
from hostore.utils.range.range import Range
from hostore.utils.utils_test import TempTestTableHelper


class HoCacheFindGroupsTestCase(TransactionTestCase, TempTestTableHelper):
    databases = ('default',)
    test_table = TestDataStore

    def setUp(self):
        self._ensure_tables()
        self.test_client_id = 1
        self.test_data = {}

        for sd, ed, prm, val in [("2024-01-01 00:00:00+00:00", "2025-01-01 00:00:00+00:00", 'prm1', 1),
                                 ("2024-01-01 00:00:00+00:00", "2025-01-01 00:00:00+00:00", 'prm2', 2),
                                 ("2023-01-01 00:00:00+00:00", "2025-01-01 00:00:00+00:00", 'prm3', 3),]:
            idx = pd.date_range(sd, ed, freq='h')
            self.test_data[prm] = pd.Series(val, index=idx, name='data')

            TestDataStore.set_lc(prm=prm, value=self.test_data[prm], client_id=self.test_client_id,
                                 versionning=False)

    def test_find_groups(self):
        # Sample input values
        prms = ['prm1', 'prm2', 'prm3']
        sd = dt.datetime(2023, 1, 1).astimezone(tz=pytz.UTC)
        ed = dt.datetime(2023, 2, 1).astimezone(tz=pytz.UTC)
        drr = Range(sd, ed)  # Mock Range instance
        freq = '1h'

        # Call the function
        groups, data = TestDataStore.find_groups(self.test_client_id, prms, drr, freq)

        # Assertions for the groups
        self.assertIn(drr, groups)
        self.assertIn('prm2', groups[drr])  # prm2 has missing data
        self.assertIn('prm1', groups[drr])  # prm1 has missing data
        self.assertNotIn('prm3', groups[drr])  # prm3 was absent

        # Assertions for the data
        self.assertNotIn('prm1', data)
        self.assertNotIn('prm2', data)
        self.assertIn('prm3', data)

        pd.testing.assert_series_equal(data['prm3'][0]['data'], self.test_data['prm3'][drr.sd:drr.ed],
                                       check_dtype=False, check_freq=False, check_names=False)






