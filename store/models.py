import io
import logging
from collections import defaultdict

import pandas as pd
from django.core.exceptions import ObjectDoesNotExist
from django.db import models
from pyarrow import BufferReader

from .manager import StoreQuerySet

logger = logging.getLogger(__name__)


class Store(models.Model):
    client_id = models.IntegerField()
    prm = models.CharField(max_length=30)
    last_modified = models.DateTimeField(auto_now=True)
    data = models.BinaryField(blank=True, null=True)

    objects = StoreQuerySet.as_manager()

    class Meta:
        abstract = True
        unique_together = ('prm', 'client_id',)
        indexes = [models.Index(fields=['prm', 'client_id']), ]

    @classmethod
    def count(cls, client_id: int):
        return cls.objects.filter(client_id=client_id).count()

    @classmethod
    def get_lc(cls, prm: str, client_id: int) -> dict | None:
        """
        Get on prm from cache
        Args:
            prm: str
            client_id: int, client's id
        Returns:
            dict
        """
        logger.info(f'Getting load curve with prm {prm} for client {client_id}')
        try:
            cache_entry = cls.objects.get(prm=prm, client_id=client_id)
        except ObjectDoesNotExist:
            return None

        result = vars(cache_entry)
        reader = BufferReader(result['data'])
        ds = pd.read_feather(reader)
        if 'index' in ds.columns:
            ds.set_index('index', inplace=True)
        else:
            logger.info(f'data is malformed, remove key {prm}')
            result.delete()
            return
        result['data'] = ds.iloc[:, 0]
        return result

    @classmethod
    def get_many_lc(cls, prms: [str], client_id: int) -> dict:
        """
        Get many prms from cache
        Args:
            prms: list of prms
            client_id: int, client's id
        Returns:
            dict(prm, dict)
        """
        ho_caches = cls.objects.filter(prm__in=prms, client_id=client_id)
        results = defaultdict(lambda: {})
        invalid_prms = []
        for ho_cache in ho_caches:
            reader = BufferReader(ho_cache.data)
            ds = pd.read_feather(reader)
            try:
                ds.set_index('index', inplace=True)
            except KeyError:
                invalid_prms.append(ho_cache.prm)
                continue
            results[ho_cache.prm] = vars(ho_cache)
            results[ho_cache.prm]['data'] = ds.iloc[:, 0]

        if len(invalid_prms) > 0:
            logger.info(f'data is malformed, for prms {invalid_prms}')
            cls.objects.filter(prms__in=invalid_prms, client_id=client_id).delete()
        return results

    @classmethod
    def set_lc(cls, prm: str, value: pd.Series, client_id: int) -> None:
        """
        Set one prm to cache
        Args:
            prm: str
            value: pd.Series
            client_id: int, client's id

        Returns:
            None
        """
        logger.info(f'SET key {prm} in cache')
        if isinstance(value, pd.Series):
            if value.isnull().all():
                logger.warning(f'CACHE : Key {prm} is ignored because data is null')
                return
            df = value.to_frame(name=prm)
            df.reset_index(inplace=True, names=['index'])
            buf = io.BytesIO()
            df.to_feather(buf, compression='lz4')
            v = buf.getvalue()
            cls(client_id=client_id, data=v, prm=prm).save(force_insert=True)
            logger.info(f'SET prm {prm} in cache DONE')
        else:
            raise ValueError(f'Cannot cache value of type {type(value)}, only dataframe are accepted')

    @classmethod
    def set_many_lc(cls, dataseries: dict, client_id: int) -> None:
        """
        Update prms contained in dataseries dict
        Args:
            dataseries: dict(key: pd.Series)
            client_id: int, client's id
        Returns:
            bool
        """
        if len(dataseries) > 0:
            logger.info(f'Updating cache for {list(dataseries.keys())}')
        for prm, data in dataseries.items():
            if data is not None and not data.empty:
                cls.set_lc(prm, data, client_id)

    @classmethod
    def clear(cls, prms: [str], client_id: int):
        """
        Method to clear multiple prm from cache
        Args:
            prms: list of prms
            client_id: client's id
        Returns:
            None
        """
        cls.objects.filter(prm__in=prms, client_id=client_id).delete()

    @classmethod
    def clear_all(cls, client_id: int = None):
        """
        Method to clear multiple prm from cache
        Args:
            client_id: client's id
        Returns:
            None
        """
        if client_id is not None:
            cls.objects.filter(client_id=client_id).delete()
        else:
            cls.objects.all().delete()


class TestDataStore(Store):
    class Meta(Store.Meta):
        abstract = False
