import datetime as dt
from zoneinfo import ZoneInfo

import pandas as pd
from django.db import models
from django.test import TransactionTestCase

from hostore.models import Store
from hostore.utils.utils_test import TempTestTableHelper


class TestDataStoreWithAttribute(Store):
    year = models.IntegerField()
    class Meta(Store.Meta):
        abstract = False
        constraints = [models.UniqueConstraint(fields=['prm', 'client_id', 'year', 'created_at'], name='hostore_TestDataStoreWithAttribute_unq'), ]
        app_label = 'ts_inline'
        managed = True


class HoCacheWithAttributesTestCase(TransactionTestCase, TempTestTableHelper):
    databases = ('default',)
    test_table = TestDataStoreWithAttribute

    def setUp(self):
        self._ensure_tables()
        # Set up data for the tests
        # For example, create a HoCache instance
        self.test_prm = 'test_prm'
        self.test_prm_2 = 'test_prm_2'
        self.test_client_id = 1
        self.test_data = pd.Series([1, 2, 3], index=[0, 1, 2], name='data')
        self.test_data_2 = self.test_data * 2

    def test_combined_by(self):
        TestDataStoreWithAttribute.clear_all(self.test_client_id)
        data_name = 'data_name'
        created_at_one = dt.datetime.now().astimezone(ZoneInfo("Europe/Paris"))
        TestDataStoreWithAttribute.set_lc(prm=data_name, value=self.test_data, client_id=self.test_client_id,
                                          attributes_to_set=dict(created_at=created_at_one,
                                                                 last_modified=created_at_one,
                                                                 year=2024),
                                          versionning=True,
                                          versionning_by=('prm', 'year'))

        # On ajoute des données sur le même PRM mais pas sur la même année
        TestDataStoreWithAttribute.set_lc(prm=data_name, value=self.test_data, client_id=self.test_client_id,
                                          attributes_to_set=dict(created_at=created_at_one,
                                                                 last_modified=created_at_one,
                                                                 year=2025),
                                          versionning=True,
                                          versionning_by=('prm', 'year'))

        # On récupére les données data_name en combinant par prm et année (2 entrées)
        data = TestDataStoreWithAttribute.get_lc(data_name, self.test_client_id, combined_versions=True,
                                                 combined_by=('prm', 'year'))
        self.assertEquals(len(data), 2)

        # On récupére les données data_name en combinant par prm et année (1 prm avec 2 entrées)
        data = TestDataStoreWithAttribute.get_many_lc([data_name], self.test_client_id, combined_versions=True,
                                                      combined_by=('prm', 'year'))
        self.assertEquals(len(data), 1)
        self.assertEquals(len(data[data_name]), 2)

        # On récupére les données data_name en combinant par version (toutes à 0)
        data = TestDataStoreWithAttribute.get_lc(data_name, self.test_client_id, combined_versions=True,
                                                 combined_by=('version',))
        self.assertEquals(len(data), 1)

        created_at_two = dt.datetime.now().astimezone(ZoneInfo("Europe/Paris"))
        # Ajout d'une deuxième courbe sur 2024
        TestDataStoreWithAttribute.set_lc(prm=data_name, value=self.test_data_2, client_id=self.test_client_id,
                                          attributes_to_set=dict(created_at=created_at_two,
                                                                 last_modified=created_at_two,
                                                                 year=2024),
                                          versionning=True,
                                          versionning_by=('prm', 'year'))

        # On récupére les données data_name en combinant par prm (2 entrées) mais pour l'année 2024 nous devons avoir les valeur de test_data_2
        data = TestDataStoreWithAttribute.get_lc(data_name, self.test_client_id, combined_versions=True,
                                                 combined_by=('prm', 'year'), order_by=('-created_at',))
        self.assertEquals(len(data), 2)

        data_2024 = next(filter(lambda d: d['year'] == 2024, data), None)
        pd.testing.assert_series_equal(data_2024['data'], self.test_data_2, check_names=False)

    def test_versionning_by(self):
        TestDataStoreWithAttribute.clear_all(self.test_client_id)
        data_name = 'data_name'
        created_at_one = dt.datetime.now().astimezone(ZoneInfo("Europe/Paris"))
        TestDataStoreWithAttribute.set_lc(prm=data_name, value=self.test_data, client_id=self.test_client_id,
                                          attributes_to_set=dict(created_at=created_at_one,
                                                                 last_modified=created_at_one,
                                                                 year=2024),
                                          versionning=True,
                                          versionning_by=('prm',))

        # On ajoute des données sur le même PRM mais pas sur la même année
        TestDataStoreWithAttribute.set_lc(prm=data_name, value=self.test_data_2, client_id=self.test_client_id,
                                          attributes_to_set=dict(created_at=created_at_one,
                                                                 last_modified=created_at_one,
                                                                 year=2025),
                                          versionning=True,
                                          versionning_by=('prm',))

        data = TestDataStoreWithAttribute.get_lc(data_name, self.test_client_id, combined_versions=False, order_by=('-created_at',))
        self.assertEquals(len(data), 2)
        self.assertEquals(sorted([d['version'] for d in data]), [0, 1])
        data = TestDataStoreWithAttribute.get_lc(data_name, self.test_client_id, combined_versions=True, combined_by=('prm',),
                                                 order_by=('-created_at',))
        self.assertEquals(len(data), 1)
        data = TestDataStoreWithAttribute.get_lc(data_name, self.test_client_id, combined_versions=True,
                                                 combined_by=('prm', 'year'),
                                                 order_by=('-created_at',))
        self.assertEquals(len(data), 2)

        # On supprime les données et on versionne par prm et year
        TestDataStoreWithAttribute.clear_all(self.test_client_id)
        data_name = 'data_name'
        created_at_one = dt.datetime.now().astimezone(ZoneInfo("Europe/Paris"))
        TestDataStoreWithAttribute.set_lc(prm=data_name, value=self.test_data, client_id=self.test_client_id,
                                          attributes_to_set=dict(created_at=created_at_one,
                                                                 last_modified=created_at_one,
                                                                 year=2024),
                                          versionning=True,
                                          versionning_by=('prm', 'year'))

        # On ajoute des données sur le même PRM mais pas sur la même année
        TestDataStoreWithAttribute.set_lc(prm=data_name, value=self.test_data_2, client_id=self.test_client_id,
                                          attributes_to_set=dict(created_at=created_at_one,
                                                                 last_modified=created_at_one,
                                                                 year=2024),
                                          versionning=True,
                                          versionning_by=('prm', 'year'))
        TestDataStoreWithAttribute.set_lc(prm=data_name, value=self.test_data, client_id=self.test_client_id,
                                          attributes_to_set=dict(created_at=created_at_one,
                                                                 last_modified=created_at_one,
                                                                 year=2025),
                                          versionning=True,
                                          versionning_by=('prm', 'year'))

        data = TestDataStoreWithAttribute.get_lc(data_name, self.test_client_id, combined_versions=False,
                                                 order_by=('-created_at',))
        self.assertEquals(len(data), 3)
        # 2 versions pour 2024, 1 version pour 2025
        self.assertEquals(sorted([d['version'] for d in data]), [0, 0, 1])
        data = TestDataStoreWithAttribute.get_lc(data_name, self.test_client_id, combined_versions=True,
                                                 combined_by=('prm', 'year'),
                                                 order_by=('-created_at',))
        # 2024 et 2025 combiné
        self.assertEquals(len(data), 2)
        data_2024 = next(filter(lambda d: d['year'] == 2024, data), None)
        pd.testing.assert_series_equal(data_2024['data'], self.test_data_2, check_names=False)
        data_2025 = next(filter(lambda d: d['year'] == 2025, data), None)
        pd.testing.assert_series_equal(data_2025['data'], self.test_data, check_names=False)

