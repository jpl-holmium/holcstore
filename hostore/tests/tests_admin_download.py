import io
import zipfile

import pandas as pd
from unittest.mock import Mock

from django.contrib import admin
from django.contrib.admin import AdminSite
from django.contrib.auth import get_user_model
from django.db import models
from django.http import HttpResponse
from django.test import TransactionTestCase

from hostore.admin_actions import download_timeseries_from_store, download_timeseries_from_chunkstore
from hostore.models import TimeseriesStore, TimeseriesChunkStore
from hostore.utils.utils_test import TempTestTableHelper


def gen_serie(start, end, data, freq='1h'):
    dt_rng = pd.date_range(start, end, freq=freq)
    return pd.Series(data, index=dt_rng, name='data')


class TestAdminTimeseriesStoreWithAttribute(TimeseriesStore):
    year = models.IntegerField()
    kind = models.CharField(max_length=100)

    class Meta(TimeseriesStore.Meta):
        abstract = False
        constraints = [models.UniqueConstraint(fields=['year', 'kind'], name='hostore_TestAdminTimeseriesStoreWithAttribute_unq'), ]
        app_label = 'ts_inline'
        managed = True


class TestTimeseriesStoreWithAttributeAdmin(admin.ModelAdmin):
    resource_classes = [TestAdminTimeseriesStoreWithAttribute]
    list_display = ('year', 'kind', )
    list_filter = ('year', 'kind', )
    actions = [download_timeseries_from_store]


class DownloadTimeseriesAdminActionTest(TransactionTestCase, TempTestTableHelper):
    databases = ('default',)
    test_table = TestAdminTimeseriesStoreWithAttribute

    def setUp(self):
        self._ensure_tables()
        # populate TestTimeseriesStoreWithAttribute
        ts_attrs_y_2020_kind_a = dict(year=2020, kind='a')
        ds_y_2020_kind_a = gen_serie("2020-01-01 00:00:00+00:00", "2020-01-01 02:00:00+00:00", [1, 2, 3])
        TestAdminTimeseriesStoreWithAttribute.set_ts(ts_attrs_y_2020_kind_a, ds_y_2020_kind_a)
        self.ts_attrs_y_2020_kind_a = ts_attrs_y_2020_kind_a
        self.ds_y_2020_kind_a = ds_y_2020_kind_a

        ts_attrs_y_2020_kind_b = dict(year=2020, kind='b')
        ds_y_2020_kind_b = gen_serie("2020-01-01 00:00:00+01:00", "2020-01-01 02:00:00+01:00", [10, 20, 30])
        TestAdminTimeseriesStoreWithAttribute.set_ts(ts_attrs_y_2020_kind_b, ds_y_2020_kind_b)
        self.ts_attrs_y_2020_kind_b = ts_attrs_y_2020_kind_b
        self.ds_y_2020_kind_b = ds_y_2020_kind_b

        # Mock request
        self.user = get_user_model().objects.create(
            email='user@example.com', password='password', is_superuser=False)
        self.mock_request = Mock(user=self.user)

        # Create an instance of the admin site
        self.site = AdminSite()

        # Create an instance of the admin class for your model
        self.model_admin = TestTimeseriesStoreWithAttributeAdmin(
            TestAdminTimeseriesStoreWithAttribute, self.site)

    def test_zip_content(self):
        queryset = TestAdminTimeseriesStoreWithAttribute.objects.all()
        response = download_timeseries_from_store(self.model_admin, self.mock_request, queryset)
        self.assertIsInstance(response, HttpResponse)
        byte_content = response.content
        zip_bytes_io = io.BytesIO(byte_content)
        with zipfile.ZipFile(zip_bytes_io, 'r') as zip_file:
            # List all files inside the zip file
            file_list = zip_file.namelist()
            self.assertListEqual(file_list, ['export_serie_0.csv', 'export_serie_1.csv', 'content_summary.csv'])
            with zip_file.open('content_summary.csv') as specific_file:
                # Read the file content (assuming it's a text file, like CSV)
                file_content = specific_file.read().decode('utf-8')
                expected = ';filename;year;kind\n0;export_serie_0.csv;2020;a\n1;export_serie_1.csv;2020;b\n'
                self.assertEquals(file_content, expected)


class TestAdminTimeseriesChunkStore(TimeseriesChunkStore):
    version = models.IntegerField()
    kind = models.CharField(max_length=100)
    STORE_TZ = "UTC"

    class Meta(TimeseriesChunkStore.Meta):
        app_label = "ts_inline"
        managed = True


class TestTimeseriesChunkStoreAdmin(admin.ModelAdmin):
    resource_classes = [TestAdminTimeseriesChunkStore]
    actions = [download_timeseries_from_chunkstore]


class DownloadTimeseriesChunkAdminActionTest(TransactionTestCase, TempTestTableHelper):
    databases = ("default",)
    test_table = TestAdminTimeseriesChunkStore

    def setUp(self):
        self._ensure_tables()
        attrs_a = dict(version=1, kind="a")
        ds_a = gen_serie(
            "2020-01-01 00:00:00+00:00",
            "2020-01-01 02:00:00+00:00",
            [1, 2, 3],
        )
        TestAdminTimeseriesChunkStore.set_ts(attrs_a, ds_a)

        attrs_b = dict(version=1, kind="b")
        ds_b = gen_serie(
            "2020-01-01 00:00:00+00:00",
            "2020-01-01 02:00:00+00:00",
            [10, 20, 30],
        )
        TestAdminTimeseriesChunkStore.set_ts(attrs_b, ds_b)

        self.user = get_user_model().objects.create(
            email="user@example.com", password="password", is_superuser=False
        )
        self.mock_request = Mock(user=self.user)

        self.site = AdminSite()
        self.model_admin = TestTimeseriesChunkStoreAdmin(
            TestAdminTimeseriesChunkStore, self.site
        )

    def test_zip_content(self):
        queryset = TestAdminTimeseriesChunkStore.objects.all()
        response = download_timeseries_from_chunkstore(
            self.model_admin, self.mock_request, queryset
        )
        self.assertIsInstance(response, HttpResponse)
        byte_content = response.content
        zip_bytes_io = io.BytesIO(byte_content)
        with zipfile.ZipFile(zip_bytes_io, "r") as zip_file:
            file_list = zip_file.namelist()
            self.assertListEqual(
                file_list,
                ["export_serie_0.csv", "export_serie_1.csv", "content_summary.csv"],
            )
            with zip_file.open("content_summary.csv") as specific_file:
                file_content = specific_file.read().decode("utf-8")
                self.assertIn("version", file_content)
                self.assertIn("kind", file_content)