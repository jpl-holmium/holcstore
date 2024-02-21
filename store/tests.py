import io
import random

import numpy as np
import pandas as pd
from django.test import TestCase

from .models import TestDataStore


class HoCacheTestCase(TestCase):
    databases = ('default',)
    def setUp(self):
        # Set up data for the tests
        # For example, create a HoCache instance
        self.test_prm = 'test_prm'
        self.test_client_id = 1
        self.test_data = pd.Series([1, 2, 3], index=[0, 1, 2], name='data')

        # Convert test data to a binary format
        df = self.test_data.to_frame(name=self.test_prm)
        df.reset_index(inplace=True, names=['index'])
        buf = io.BytesIO()
        df.to_feather(buf, compression='lz4')
        self.test_binary_data = buf.getvalue()

        TestDataStore.objects.create(
            prm=self.test_prm,
            client_id=self.test_client_id,
            data=self.test_binary_data
        )

    def test_get_lc(self):
        # Test the get_lc method
        cache_entry = TestDataStore.get_lc(prm=self.test_prm, client_id=self.test_client_id)
        self.assertIsNotNone(cache_entry)
        self.assertEqual(cache_entry['prm'], self.test_prm)
        # Add more assertions as needed
        pd.testing.assert_series_equal(cache_entry['data'], self.test_data, check_names=False)

    def test_get_many_lc(self):
        # Test the get_many_lc method
        prms = [self.test_prm, 'non_existing_prm']
        results = TestDataStore.get_many_lc(prms=prms, client_id=self.test_client_id)
        self.assertIn(self.test_prm, results)
        self.assertNotIn('non_existing_prm', results)
        # Add more assertions as needed
        pd.testing.assert_series_equal(results[self.test_prm]['data'], self.test_data, check_names=False)

    def test_set_lc(self):
        # Test the set_lc method
        new_prm = 'new_test_param'
        TestDataStore.set_lc(prm=new_prm, value=self.test_data, client_id=self.test_client_id)
        new_entry = TestDataStore.objects.get(prm=new_prm, client_id=self.test_client_id)
        self.assertIsNotNone(new_entry)

        data = TestDataStore.get_lc(new_prm, self.test_client_id)
        pd.testing.assert_series_equal(data['data'], self.test_data, check_names=False)
    # *******************************************************

    def test_clearing(self):
        TestDataStore.clear([self.test_prm], self.test_client_id)
        ds_after_clear = TestDataStore.get_lc(self.test_prm, self.test_client_id)
        self.assertEqual(ds_after_clear, None)

    def test_clearing_all(self):
        TestDataStore.clear_all()
        ds_after_clear = TestDataStore.get_lc(self.test_prm, self.test_client_id)
        self.assertEqual(ds_after_clear, None)

    def test_set_get_clear_dataframe(self):
        idx = pd.date_range('2022-01-01', '2024-01-01', freq='30min')
        data = [random.random() for k in range(0, len(idx))]
        ds_initial = pd.Series(data, index=idx)
        new_client_id = self.test_client_id + 1
        # Set key, value in cache
        TestDataStore.set_lc('key', ds_initial, new_client_id)
        # Get key, value from cache
        data = TestDataStore.get_lc('key', new_client_id)
        self.assertEqual(ds_initial.compare(data['data']).shape[0], 0)
        TestDataStore.clear_all(client_id=new_client_id)
        self.assertEqual(TestDataStore.count(self.test_client_id), 1)

    def test_set_dataframe_with_nan(self):
        idx = pd.date_range('2022-01-01', '2024-01-01', freq='30min')
        ds_initial = pd.Series(np.nan, index=idx)

        # Set key, value in cache
        TestDataStore.set_lc('key', ds_initial, self.test_client_id)
        # Get key, value from cache
        data = TestDataStore.get_lc('key', self.test_client_id)
        self.assertEqual(data, None)

    def test_not_existing_key(self):
        ds = TestDataStore.get_lc('not_existing_key', self.test_client_id)
        self.assertEqual(ds, None)

    def test_cache_not_series(self):
        self.assertRaises(ValueError, TestDataStore.set_lc, 'key', 'hello', self.test_client_id)

    def test_set_index_with_different_names(self):
        ds = pd.Series([1, 2, 3])
        ds.index.name = 'test'
        TestDataStore.set_lc('key', ds, self.test_client_id)
        # Get the serie from cache
        ds_from_cache = TestDataStore.get_lc('key', self.test_client_id)['data']
        pd.testing.assert_series_equal(ds, ds_from_cache, check_names=False)
