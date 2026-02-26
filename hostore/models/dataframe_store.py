import io
import json
import logging

import pandas as pd
from django.db.models import UniqueConstraint

logger = logging.getLogger(__name__)

from django.db import models, transaction, IntegrityError


class DataFrameStore(models.Model):
    client_id = models.IntegerField(db_index=True)
    key = models.CharField(max_length=255, db_index=True)  # ex: "features_users_v1"
    meta = models.JSONField(default=dict, blank=True)      # tags, version, etc.
    payload = models.BinaryField()                         # bytea

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        abstract=True
        indexes = [
            models.Index(fields=["client_id", "key"]),
        ]
        constraints = [UniqueConstraint(fields=["client_id", "key"], name="unique_client_key")]

    @classmethod
    def update_or_create_df(cls, client_id: int, key: str, df: pd.DataFrame, meta=None, *, store_index: bool = True):
        if not isinstance(df, pd.DataFrame):
            raise TypeError("df must be a pandas.DataFrame")
        if not key or not isinstance(key, str):
            raise ValueError("key must be a non-empty string")

        if meta is None:
            meta = {}
        elif not isinstance(meta, dict):
            raise TypeError("meta must be a dict")
        json.dumps(meta)  # juste pour vérifier si meta est sérialisable
        meta.setdefault("store_index", store_index)

        buf = io.BytesIO()
        df.to_parquet(buf, engine="pyarrow", compression="zstd", index=store_index)
        payload = buf.getvalue()
        with transaction.atomic():
            try:
                obj, created = cls.objects.update_or_create(
                    client_id=client_id,
                    key=key,
                    defaults={
                        "meta": meta,
                        "payload":payload,
                    }
                )
            except IntegrityError:
                # rare race: re-fetch and update
                obj = cls.objects.select_for_update().get(client_id=client_id, key=key)
                obj.meta = meta
                obj.payload = payload
                obj.save(update_fields=["meta", "payload"])
                created = False

        logger.info("DF upsert client_id=%s key=%s rows=%s bytes=%s", client_id, key, len(df), len(payload))

        return obj, created

    @classmethod
    def load_df(cls, client_id:int, key: str) -> pd.DataFrame | None:
        try:
            obj = cls.objects.get(client_id=client_id, key=key)
        except cls.DoesNotExist:
            return None
        return pd.read_parquet(io.BytesIO(obj.payload), engine="pyarrow")