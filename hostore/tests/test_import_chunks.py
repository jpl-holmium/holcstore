import pandas as pd
from django.db import models
from django.test import TransactionTestCase

from hostore.models import TimeseriesChunkStore
from hostore.utils.utils_test import TempTestTableHelper


class ImportStore(TimeseriesChunkStore):
    version = models.IntegerField()
    kind = models.CharField(max_length=5)
    CHUNK_AXIS = ('year', 'month')
    ALLOW_CLIENT_SERVER_SYNC = True

    class Meta:
        app_label = 'ts_inline'
        managed = True


class ImportChunksAtomicTest(TransactionTestCase, TempTestTableHelper):
    test_table = ImportStore

    def setUp(self):
        self._ensure_tables(delete_kw=dict(_disable_sync_safety=True))

    @staticmethod
    def _make_series():
        idx = pd.date_range('2024-01-01', periods=40 * 24, freq='1h', tz='UTC')
        return pd.Series(range(len(idx)), index=idx)

    def test_failed_import_rolls_back(self):
        serie = self._make_series()
        attrs = {'version': 1, 'kind': 'A'}
        self.test_table.set_ts(attrs, serie, replace=True)
        spec = [
            {'attrs': {'version': row.version, 'kind': row.kind}, 'chunk_index': row.chunk_index}
            for row in self.test_table.objects.all()
        ]
        payload = self.test_table.export_chunks(spec)
        self.test_table.objects.all().delete(_disable_sync_safety=True)

        bad_attrs = payload[1][1].copy()
        bad_attrs.pop('chunk_index')
        bad_payload = [payload[0], (payload[1][0], bad_attrs, payload[1][2])]

        with self.assertRaises(ValueError):
            self.test_table.import_chunks(bad_payload)
        self.assertEqual(self.test_table.objects.count(), 0)

        self.test_table.import_chunks(payload)
        self.assertEqual(self.test_table.objects.count(), len(payload))
