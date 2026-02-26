import io

import pandas as pd
from django.test import TransactionTestCase

from hostore.models.dataframe_store import DataFrameStore
from hostore.utils.utils_test import TempTestTableHelper


class DefaultDataFrameStore(DataFrameStore):

    class Meta:
        app_label = "ts_inline"
        managed = True

class DataFrameStoreTestCase(TransactionTestCase, TempTestTableHelper):
    test_table = DefaultDataFrameStore

    def setUp(self):
        self._ensure_tables()
        self.client_id = 123
        self.key = "features_users_v1"

    def _df_basic(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "user_id": [1, 2, 3],
                "score": [0.1, 0.2, 0.3],
                "name": ["a", "b", "c"],
            }
        )

    def _df_other(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "user_id": [1, 2],
                "score": [9.9, 8.8],
                "name": ["x", "y"],
            }
        )

    # ---------- Happy path ----------

    def test_create_and_load_roundtrip(self):
        df = self._df_basic()
        meta = {"version": 1, "tags": ["unit-test"]}

        obj, created = DefaultDataFrameStore.update_or_create_df(
            client_id=self.client_id,
            key=self.key,
            df=df,
            meta=meta,
        )

        self.assertTrue(created)
        self.assertEqual(obj.client_id, self.client_id)
        self.assertEqual(obj.key, self.key)
        self.assertEqual(obj.meta, meta)
        self.assertIsInstance(obj.payload, (bytes, bytearray))
        self.assertGreater(len(obj.payload), 0)

        loaded = DefaultDataFrameStore.load_df(self.client_id, self.key)
        self.assertIsNotNone(loaded)

        # On compare les données, pas forcément l'index (selon index=True)
        pd.testing.assert_frame_equal(
            loaded.reset_index(drop=True),
            df.reset_index(drop=True),
            check_dtype=False,  # tolérance selon pyarrow/pandas versions
        )

    def test_update_existing_row_created_flag_false_and_payload_changes(self):
        df1 = self._df_basic()
        obj1, created1 = DefaultDataFrameStore.update_or_create_df(self.client_id, self.key, df1, meta={"v": 1})
        self.assertTrue(created1)

        payload1 = obj1.payload

        df2 = self._df_other()
        obj2, created2 = DefaultDataFrameStore.update_or_create_df(self.client_id, self.key, df2, meta={"v": 2})
        self.assertFalse(created2)

        obj2.refresh_from_db()
        self.assertNotEqual(obj2.payload, payload1)
        self.assertEqual(obj2.meta["v"], 2)
        self.assertIn("store_index", obj2.meta)
        self.assertTrue(obj2.meta["store_index"])

        loaded = DefaultDataFrameStore.load_df(self.client_id, self.key)
        pd.testing.assert_frame_equal(
            loaded.reset_index(drop=True),
            df2.reset_index(drop=True),
            check_dtype=False,
        )

    def test_load_returns_none_when_missing(self):
        self.assertIsNone(DefaultDataFrameStore.load_df(self.client_id, "missing_key"))

    # ---------- Validation ----------

    def test_update_or_create_df_rejects_non_dataframe(self):
        with self.assertRaises(TypeError):
            DefaultDataFrameStore.update_or_create_df(self.client_id, self.key, df={"not": "a df"}, meta={})

    def test_update_or_create_df_rejects_empty_key(self):
        with self.assertRaises(ValueError):
            DefaultDataFrameStore.update_or_create_df(self.client_id, "", df=self._df_basic(), meta={})

    def test_update_or_create_df_rejects_non_str_key(self):
        with self.assertRaises(ValueError):
            DefaultDataFrameStore.update_or_create_df(self.client_id, 12345, df=self._df_basic(), meta={})  # type: ignore[arg-type]

    def test_update_or_create_df_meta_none_ok(self):
        df = self._df_basic()
        obj, created = DefaultDataFrameStore.update_or_create_df(self.client_id, self.key, df=df, meta=None)
        self.assertTrue(created)
        obj.refresh_from_db()
        self.assertIn("store_index", obj.meta)
        self.assertTrue(obj.meta["store_index"])

    def test_update_or_create_df_meta_must_be_dict_or_list(self):
        with self.assertRaises(TypeError):
            DefaultDataFrameStore.update_or_create_df(self.client_id, self.key, df=self._df_basic(), meta="not-json-container")

    def test_update_or_create_df_meta_must_be_json_serializable(self):
        # set() n'est pas JSON serializable
        with self.assertRaises(TypeError):
            DefaultDataFrameStore.update_or_create_df(self.client_id, self.key, df=self._df_basic(), meta={"bad": {1, 2, 3}})

    # ---------- DB constraints / uniqueness ----------

    def test_unique_constraint_enforced(self):
        # Création directe DB 2 fois -> IntegrityError
        df = self._df_basic()

        obj1, created1 = DefaultDataFrameStore.update_or_create_df(self.client_id, self.key, df, meta={"v": 1})
        self.assertTrue(created1)

        # Le second appel doit UPDATE (pas IntegrityError) et created=False
        obj2, created2 = DefaultDataFrameStore.update_or_create_df(self.client_id, self.key, df, meta={"v": 2})
        self.assertFalse(created2)

        self.assertEqual(DefaultDataFrameStore.objects.filter(client_id=self.client_id, key=self.key).count(), 1)
        obj2.refresh_from_db()
        self.assertEqual(obj2.meta["v"], 2)
        self.assertIn("store_index", obj2.meta)
        self.assertTrue(obj2.meta["store_index"])

    # ---------- Edge cases ----------

    def test_payload_is_valid_parquet(self):
        df = self._df_basic()
        obj, _ = DefaultDataFrameStore.update_or_create_df(self.client_id, self.key, df, meta={"v": 1})

        # On vérifie que pandas sait relire le payload directement
        roundtrip = pd.read_parquet(io.BytesIO(obj.payload), engine="pyarrow")
        self.assertEqual(len(roundtrip), len(df))

    def test_separate_clients_can_share_same_key(self):
        df = self._df_basic()
        obj1, created1 = DefaultDataFrameStore.update_or_create_df(1, self.key, df, meta={"c": 1})
        obj2, created2 = DefaultDataFrameStore.update_or_create_df(2, self.key, df, meta={"c": 2})

        self.assertTrue(created1)
        self.assertTrue(created2)
        self.assertEqual(DefaultDataFrameStore.objects.filter(key=self.key).count(), 2)
