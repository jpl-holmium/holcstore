import pandas as pd
from django.test import TransactionTestCase

from hostore.tests.tests_basics import TestDataStore
from hostore.utils.timeseries import ts_combine_first
from hostore.utils.utils_test import TempTestTableHelper


class HoCacheSliceDelayTestCase(TransactionTestCase, TempTestTableHelper):
    databases = ('default',)
    test_table = TestDataStore

    def setUp(self):
        self._ensure_tables()
        # Set up data for the tests
        # For example, create a HoCache instance
        self.test_prm = 'test_prm'
        self.test_prm_2 = 'test_prm_2'
        self.test_client_id = 1

        idx = pd.date_range("2024-01-01 00:00:00+00:00", "2025-01-01 00:00:00+00:00", freq='h')
        self.test_data = pd.Series(1, index=idx, name='data')
        idx_2 = pd.date_range("2024-01-02 00:00:00+00:00", "2025-01-01 00:00:00+00:00", freq='h')
        self.test_data_2 = pd.Series(2, index=idx_2, name='data')

        TestDataStore.set_lc(prm=self.test_prm, value=self.test_data, client_id=self.test_client_id,
                             versionning=True)
        TestDataStore.set_lc(prm=self.test_prm, value=self.test_data_2, client_id=self.test_client_id,
                             versionning=True)

    def test_slice_delay(self):
        data = TestDataStore.get_lc(self.test_prm, self.test_client_id, combined_versions=True,
                                    combined_delay=pd.Timedelta(days=1))

        self.assertEquals(len(data), 1)
        # Nous sommes décalés de 1 jour
        expected_data = ts_combine_first([self.test_data_2.iloc[24:], self.test_data.iloc[24:]])
        pd.testing.assert_series_equal(data[0]['data'], expected_data, check_names=False)

        data = TestDataStore.get_lc(self.test_prm, self.test_client_id, combined_versions=True,
                                    combined_delay=pd.Timedelta(days=2))

        self.assertEquals(len(data), 1)
        # Nous sommes décalés de 2 jours
        expected_data = ts_combine_first([self.test_data_2.iloc[48:], self.test_data.iloc[48:]])
        pd.testing.assert_series_equal(data[0]['data'], expected_data, check_names=False)

        data = TestDataStore.get_many_lc([self.test_prm], self.test_client_id, combined_versions=True,
                                         combined_delay=pd.Timedelta(days=2))

        self.assertEquals(len(data), 1)
        # Nous sommes décalés de 2 jours
        expected_data = ts_combine_first([self.test_data_2.iloc[48:], self.test_data.iloc[48:]])
        pd.testing.assert_series_equal(data[self.test_prm][0]['data'], expected_data, check_names=False)




