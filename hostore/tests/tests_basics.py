import io
import random
import datetime as dt
import numpy as np
import pandas as pd
import pytz
from django.test import TransactionTestCase

from hostore.models import Store
from hostore.utils.utils_test import TempTestTableHelper


class TestDataStore(Store):
    class Meta(Store.Meta):
        abstract = False
        app_label = 'hostore'

class HoCacheTestCase(TransactionTestCase, TempTestTableHelper):
    databases = ('default',)
    test_table = TestDataStore

    def setUp(self):
        self._ensure_tables()
        # Set up data for the tests
        # For example, create a HoCache instance
        self.test_prm = 'test_prm'
        self.test_prm_2 = 'test_prm_2'
        self.test_client_id = 1
        self.test_data = pd.Series([1, 2, 3], index=[0, 1, 2], name='data')
        self.test_data_2 = self.test_data * 2

        # Convert test data to a binary format
        TestDataStore.set_lc(self.test_prm, self.test_data, self.test_client_id)
        TestDataStore.set_lc(self.test_prm_2, self.test_data, self.test_client_id, versionning=True)
        TestDataStore.set_lc(self.test_prm_2, self.test_data_2, self.test_client_id, versionning=True)

    def test_get_lc(self):
        # Test the get_lc method
        cache_entry = TestDataStore.get_lc(prm=self.test_prm, client_id=self.test_client_id)
        self.assertIsNotNone(cache_entry)
        self.assertEquals(len(cache_entry), 1)
        self.assertEqual(cache_entry[0]['prm'], self.test_prm)
        pd.testing.assert_series_equal(cache_entry[0]['data'], self.test_data, check_names=False)

    def test_get_many_lc(self):
        # Test the get_many_lc method
        prms = [self.test_prm, 'non_existing_prm']
        results = TestDataStore.get_many_lc(prms=prms, client_id=self.test_client_id)
        self.assertIn(self.test_prm, results)
        self.assertNotIn('non_existing_prm', results)
        self.assertEqual(len(results[self.test_prm]), 1)
        pd.testing.assert_series_equal(results[self.test_prm][0]['data'], self.test_data, check_names=False)

    def test_set_many_lc(self):
        # Test the get_many_lc method
        dataseries = dict(prm_1=pd.Series([1, 2, 3]),
                          prm_2=pd.Series([4, 5, 6]))
        TestDataStore.set_many_lc(dataseries, self.test_client_id)
        results = TestDataStore.get_many_lc(['prm_1', 'prm_2'], self.test_client_id)
        self.assertIn("prm_1", results)
        self.assertIn("prm_2", results)
        self.assertEqual(len(results), 2)
        self.assertNotIn('non_existing_prm', results)
        self.assertEqual(len(results['prm_1']), 1)
        self.assertEqual(len(results['prm_2']), 1)
        pd.testing.assert_series_equal(results['prm_1'][0]['data'], dataseries['prm_1'], check_names=False)
        pd.testing.assert_series_equal(results['prm_2'][0]['data'], dataseries['prm_2'], check_names=False)

    def test_set_lc(self):
        # Test the set_lc method
        new_prm = 'new_test_prm'
        TestDataStore.set_lc(prm=new_prm, value=self.test_data, client_id=self.test_client_id)
        # We should find the prm
        TestDataStore.objects.get(prm=new_prm, client_id=self.test_client_id, version=0)

        # Récupération des données du PRM sans spécifier de version
        data = TestDataStore.get_lc(new_prm, self.test_client_id)
        self.assertEquals(len(data), 1)
        pd.testing.assert_series_equal(data[0]['data'], self.test_data, check_names=False)

        # Récupération des données du PRM en spécifiant la version
        data = TestDataStore.get_lc(new_prm, self.test_client_id, version=0)
        self.assertEquals(len(data), 1)
        pd.testing.assert_series_equal(data[0]['data'], self.test_data, check_names=False)

    def test_set_existing_lc(self):
        # Test the set_lc method
        new_prm = 'new_test_param'
        # Insert 2 times in a row without versionning
        TestDataStore.set_lc(prm=new_prm, value=self.test_data, client_id=self.test_client_id, versionning=False)
        TestDataStore.set_lc(prm=new_prm, value=self.test_data, client_id=self.test_client_id, versionning=False)

        data = TestDataStore.get_lc(new_prm, self.test_client_id, version=0)
        self.assertEquals(len(data), 1)
        self.assertEquals(data[0]['version'], 0)

        # Insert 2 times in a row with versionning
        TestDataStore.set_lc(prm=new_prm, value=self.test_data, client_id=self.test_client_id, versionning=True)
        TestDataStore.set_lc(prm=new_prm, value=self.test_data, client_id=self.test_client_id, versionning=True)

        data = TestDataStore.get_lc(new_prm, self.test_client_id, combined_versions=False)
        self.assertEquals(len(data), 3)
        self.assertEquals(data[0]['version'], 2)

        data = TestDataStore.get_lc(new_prm, self.test_client_id, combined_versions=True)
        self.assertEquals(len(data), 1)
        self.assertEquals(data[0]['version'], 2)

    def test_combined_versions(self):
        data = TestDataStore.get_lc(self.test_prm_2, self.test_client_id, combined_versions=True)
        self.assertEquals(len(data), 1)
        self.assertEquals(data[0]['version'], 1)
        # Vérifier que la combinaison donne la deuxième version
        pd.testing.assert_series_equal(data[0]['data'], self.test_data_2, check_names=False)

        # test if we specify a version
        data = TestDataStore.get_lc(self.test_prm_2, self.test_client_id, combined_versions=False, version=1)
        self.assertEquals(len(data), 1)
        self.assertEquals(data[0]['version'], 1)
        # Vérifier que la combinaison donne la deuxième version
        pd.testing.assert_series_equal(data[0]['data'], self.test_data_2, check_names=False)

    def test_non_combined_versions(self):
        data = TestDataStore.get_lc(self.test_prm_2, self.test_client_id, combined_versions=False)
        self.assertEquals(len(data), 2)
        self.assertEquals(data[0]['version'], 1)
        self.assertEquals(data[1]['version'], 0)
        # Vérifier que la combinaison donne la deuxième version
        pd.testing.assert_series_equal(data[0]['data'], self.test_data_2, check_names=False)
        pd.testing.assert_series_equal(data[1]['data'], self.test_data, check_names=False)

        # test if we specify a version
        data = TestDataStore.get_lc(self.test_prm_2, self.test_client_id, combined_versions=False, version=0)
        self.assertEquals(len(data), 1)
        self.assertEquals(data[0]['version'], 0)
        # Vérifier que la combinaison donne la deuxième version
        pd.testing.assert_series_equal(data[0]['data'], self.test_data, check_names=False)

    # *******************************************************

    def test_clearing(self):
        TestDataStore.clear([self.test_prm], self.test_client_id)
        data_after_clear = TestDataStore.get_lc(self.test_prm, self.test_client_id)
        self.assertEqual(len(data_after_clear), 0)

    def test_clearing_version(self):
        TestDataStore.clear([self.test_prm_2], self.test_client_id, version=0)
        data_after_clear = TestDataStore.get_lc(self.test_prm, self.test_client_id, combined_versions=False)
        self.assertEqual(len(data_after_clear), 1)

    def test_clearing_all(self):
        TestDataStore.clear_all()
        data_after_clear = TestDataStore.get_lc(self.test_prm, self.test_client_id)
        self.assertEqual(len(data_after_clear), 0)

    def test_set_get_clear_dataframe(self):
        idx = pd.date_range('2022-01-01', '2024-01-01', freq='30min')
        data = [random.random() for k in range(0, len(idx))]
        ds_initial = pd.Series(data, index=idx)
        new_client_id = self.test_client_id + 1
        # Set key, value in cache
        TestDataStore.set_lc('key', ds_initial, new_client_id, versionning=False)
        # Get key, value from cache
        data = TestDataStore.get_lc('key', new_client_id)
        self.assertEqual(ds_initial.compare(data[0]['data']).shape[0], 0)
        TestDataStore.clear_all(client_id=new_client_id)
        # 3 entrées sur le client_id self.test_client_id
        self.assertEqual(TestDataStore.count(self.test_client_id), 3)
        # 3 entrées sur le client_id self.test_client_id
        self.assertEqual(TestDataStore.count(self.test_client_id + 1), 0)

    def test_set_dataframe_with_nan(self):
        idx = pd.date_range('2022-01-01', '2024-01-01', freq='30min')
        ds_initial = pd.Series(np.nan, index=idx)

        # Set key, value in cache
        TestDataStore.set_lc('key', ds_initial, self.test_client_id)
        # Get key, value from cache
        data = TestDataStore.get_lc('key', self.test_client_id)
        self.assertEqual(len(data), 0)

    def test_not_existing_key(self):
        data = TestDataStore.get_lc('not_existing_key', self.test_client_id)
        self.assertEqual(len(data), 0)

    def test_cache_not_series(self):
        self.assertRaises(ValueError, TestDataStore.set_lc, 'key', 'hello', self.test_client_id)

    def test_set_index_with_different_names(self):
        ds = pd.Series([1, 2, 3])
        ds.index.name = 'test'
        TestDataStore.set_lc('key', ds, self.test_client_id)
        # Get the serie from cache
        ds_from_cache = TestDataStore.get_lc('key', self.test_client_id)[0]['data']
        pd.testing.assert_series_equal(ds, ds_from_cache, check_names=False)

    def test_find_holes(self):
        sd = dt.datetime(2024, 1, 1).astimezone(tz=pytz.UTC)
        ed = dt.datetime(2024, 1, 11).astimezone(tz=pytz.UTC)
        idx = pd.date_range(sd, ed, freq='D')
        ds_no_hole = pd.Series(10, idx)
        ds_no_hole.index.name = 'no_hole'

        ds_one_hole = ds_no_hole.copy()
        ds_one_hole.loc['2024-01-02':'2024-01-03'] = None
        ds_one_hole.index.name = 'one_hole'

        ds_two_holes = ds_no_hole.copy()
        ds_two_holes.loc['2024-01-02':'2024-01-03'] = None
        ds_two_holes.loc['2024-01-05':'2024-01-08'] = None
        ds_two_holes.index.name = 'two_holes'

        TestDataStore.set_lc('no_hole', ds_no_hole, self.test_client_id)
        TestDataStore.set_lc('one_hole', ds_one_hole, self.test_client_id)
        TestDataStore.set_lc('two_holes', ds_two_holes, self.test_client_id)

        # Get holes of one_hole
        holes = list(TestDataStore.find_holes(self.test_client_id, sd, ed, freq='D', prms=['one_hole']))
        self.assertEquals(len(holes), 1)
        self.assertEquals(holes[0][0], 'one_hole')
        self.assertEquals(holes[0][1][0],
                          (dt.datetime(2024, 1, 2).astimezone(tz=pytz.UTC),
                           dt.datetime(2024, 1, 3).astimezone(tz=pytz.UTC)))

        # Get holes of two_holes
        holes = list(TestDataStore.find_holes(self.test_client_id, sd, ed, freq='D', prms=['two_holes']))
        self.assertEquals(len(holes), 1)
        self.assertEquals(holes[0][0], 'two_holes')
        self.assertEquals(holes[0][1][0],
                          (dt.datetime(2024, 1, 2).astimezone(tz=pytz.UTC),
                           dt.datetime(2024, 1, 3).astimezone(tz=pytz.UTC)))
        self.assertEquals(holes[0][1][1],
                          (dt.datetime(2024, 1, 5).astimezone(tz=pytz.UTC),
                           dt.datetime(2024, 1, 8).astimezone(tz=pytz.UTC)))

        # Get holes of no_hole
        holes = list(TestDataStore.find_holes(self.test_client_id, sd, ed, freq='D', prms=['no_hole']))
        self.assertEquals(holes[0][0], 'no_hole')
        self.assertEquals(holes[0][1], [])

        # Get all holes
        holes = list(TestDataStore.find_holes(self.test_client_id, sd, ed, freq='D', prms=['no_hole', 'one_hole', 'two_holes', 'non_existing']))
        self.assertEquals(len(holes), 4)
