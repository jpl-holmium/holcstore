import datetime as dt
import io
import zipfile

import pandas as pd
from unittest.mock import Mock, patch

from django.contrib.admin import AdminSite
from django.contrib.auth import get_user_model
from django.http import HttpResponse
from django.test import TestCase

from hostore.admin import TestTimeseriesStoreWithAttributeAdmin
from hostore.admin_actions import download_timeseries_from_store
from hostore.models import TestTimeseriesStoreWithAttribute


def gen_serie(start, end, data, freq='1h'):
    dt_rng = pd.date_range(start, end, freq=freq)
    return pd.Series(data, index=dt_rng, name='data')


class DownloadTimeseriesAdminActionTest(TestCase):

    def setUp(self):
        # populate TestTimeseriesStoreWithAttribute
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

        # Mock request
        self.user = get_user_model().objects.create(
            email='user@example.com', password='password', is_superuser=False)
        self.mock_request = Mock(user=self.user)

        # Create an instance of the admin site
        self.site = AdminSite()

        # Create an instance of the admin class for your model
        self.model_admin = TestTimeseriesStoreWithAttributeAdmin(
            TestTimeseriesStoreWithAttribute, self.site)

    def test_zip_content(self):
        queryset = TestTimeseriesStoreWithAttribute.objects.all()
        response = download_timeseries_from_store(self.model_admin, self.mock_request, queryset)
        self.assertIsInstance(response, HttpResponse)
        byte_content = response.content
        zip_bytes_io = io.BytesIO(byte_content)
        with zipfile.ZipFile(zip_bytes_io, 'r') as zip_file:
            # List all files inside the zip file
            file_list = zip_file.namelist()
            self.assertListEqual(file_list, ['export_serie_0.csv', 'export_serie_1.csv', 'content_summary.csv'])
            print("Files in the ZIP file:", file_list)
            with zip_file.open('content_summary.csv') as specific_file:
                # Read the file content (assuming it's a text file, like CSV)
                file_content = specific_file.read().decode('utf-8')
                self.assertEquals(file_content, ';filename;id;year;kind\n0;export_serie_0.csv;1;2020;a\n1;export_serie_1.csv;2;2020;b\n')
