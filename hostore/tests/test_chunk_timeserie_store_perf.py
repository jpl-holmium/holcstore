# tests/test_timeseries_chunk_store_load.py
import time
import itertools as it
from unittest import skip

import numpy as np
import pandas as pd
from django.db import models, connection
from django.test import TransactionTestCase

from hostore.models import TimeseriesChunkStore


# ---------------------------------------------------------------------------
# 1)  Deux magasins de test
# ---------------------------------------------------------------------------

class LoadStoreMonth15(TimeseriesChunkStore):
    """Chunk (year, month)"""
    version = models.IntegerField()
    kind    = models.CharField(max_length=20)
    CHUNK_AXIS = ("year", "month")
    STORE_FREQ   = '15min'

    class Meta:
        app_label = "ts_inline"
        managed   = True
        constraints = [models.UniqueConstraint(fields=["version", "kind", "chunk_index"], name='hostore_LoadStoreMonth15_unq'), ]
        indexes = [
            models.Index(fields=['version', 'kind', 'chunk_index']),
            models.Index(fields=['updated_at']),
        ]

class LoadStoreYear(TimeseriesChunkStore):
    """Chunk (year)"""
    version = models.IntegerField()
    kind    = models.CharField(max_length=20)
    CHUNK_AXIS = ("year",)

    class Meta:
        app_label = "ts_inline"
        managed   = True
        constraints = [models.UniqueConstraint(fields=["version", "kind", "chunk_index"], name='hostore_LoadStoreYear_unq'), ]
        indexes = [
            models.Index(fields=['version', 'kind', 'chunk_index']),
            models.Index(fields=['updated_at']),
        ]

# ---------------------------------------------------------------------------
# 2)  Base abstraite « load »
# ---------------------------------------------------------------------------
class BaseLoadTsStoreTestCase(TransactionTestCase):
    """
    Classe mère pour les tests de charge.
    Surcharger : STORE_CLASS, N_SERIES, SERIES_LEN_H.
    """
    __unittest_skip__ = True

    STORE_CLASS   = None            # → LoadStoreMonth / LoadStoreYear
    N_SERIES      = 50              # combien de courbes
    SERIES_LEN_H  = 24 * 365 * 2    # taille de chaque courbe (heures)
    RANDOM_SEED   = 0

    # ---------- infra ----------

    def _ensure_tables(self):
        existing = connection.introspection.table_names()
        with connection.schema_editor(atomic=False) as se:
            if self.STORE_CLASS._meta.db_table not in existing:
                se.create_model(self.STORE_CLASS)
        self.STORE_CLASS.objects.all().delete()

    def setUp(self):
        self._ensure_tables()

    # ---------- helpers ----------

    @classmethod
    def _make_serie(cls, start_ts, seed_offset):
        np.random.seed(cls.RANDOM_SEED + seed_offset)
        idx = pd.date_range(
            start=start_ts, periods=cls.SERIES_LEN_H,
            freq=cls.STORE_CLASS.STORE_FREQ, tz=cls.STORE_CLASS.STORE_TZ
        )
        return pd.Series(np.random.randn(cls.SERIES_LEN_H), index=idx)

    # ---------- test de charge ----------

    def test_bulk_insert_and_read(self):
        """
        1. Génère N_SERIES séries aléatoires.
        2. Les insère en bulk.
        3. Relit chaque courbe et vérifie l’intégrité.
        4. Mesure le temps global (simple indicateur).
        """
        Store = self.STORE_CLASS

        # 1) Construction du mapping à insérer
        mapping = {}
        for i in range(self.N_SERIES):
            attrs = (i, f"k{i:03d}")   # (version, kind)
            start = f"2020-01-01"
            mapping[attrs] = self._make_serie(start, i)

        # 2) Insertion bulk
        t0 = time.perf_counter()
        Store.set_many_ts(mapping, keys=("version", "kind"))
        insert_dur = time.perf_counter() - t0

        # 3) Lecture / contrôle get
        t1 = time.perf_counter()
        for (v, k), src in mapping.items():
            dst = Store.get_ts({"version": v, "kind": k})
            # assert_series_equal(src, dst)
        read_dur = time.perf_counter() - t1

        # 4) Lecture / contrôle yield_many
        t1 = time.perf_counter()
        data = Store.yield_many_ts({})
        data = list(data)
        read_many_dur = time.perf_counter() - t1

        # 4) Affiche un petit récap (pas d’assert stricte : ajustable)
        print(
            # f"\n[LOAD] {self.N_SERIES} séries × {self.SERIES_LEN_H/24}j "
            f"→ insert {insert_dur:.2f}s, read {read_dur:.2f}s, read many {read_many_dur:.2f}s "
            f"({Store.__name__})"
        )

@skip
class LoadMonth_heavy(BaseLoadTsStoreTestCase):
    """
    ./manage.py test hostore.tests.test_chunk_timeserie_store_perf.LoadMonth_heavy
    """
    __unittest_skip__ = False
    STORE_CLASS = LoadStoreMonth15
    N_SERIES    = 1000
    SERIES_LEN_H = 24 * 365 * 3 * 4

@skip
class LoadYear_light(BaseLoadTsStoreTestCase):
    """
    ./manage.py test hostore.tests.test_chunk_timeserie_store_perf.LoadYear_light
    """
    __unittest_skip__ = False
    STORE_CLASS = LoadStoreYear
    N_SERIES    = 20
    SERIES_LEN_H = 24 * 365 * 5