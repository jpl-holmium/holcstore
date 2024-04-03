import io
import logging
from collections import defaultdict
from typing import List, Dict

import pandas as pd
from django.db import models
from django.db.models import Max
from pyarrow import BufferReader

from .manager import StoreQuerySet
from .utils.timeseries import ts_combine_first

logger = logging.getLogger(__name__)


class Store(models.Model):
    client_id = models.IntegerField()
    prm = models.CharField(max_length=30)
    last_modified = models.DateTimeField(auto_now=True)
    data = models.BinaryField(blank=True, null=True)
    version = models.IntegerField(default=0)

    objects = StoreQuerySet.as_manager()

    class Meta:
        abstract = True
        unique_together = ('prm', 'client_id', 'version')
        indexes = [models.Index(fields=['prm', 'client_id']), ]

    @classmethod
    def count(cls, client_id: int):
        return cls.objects.filter(client_id=client_id).count()

    @classmethod
    def get_lc(cls, prm: str, client_id: int, combined_versions=True, version: int = None) -> List[Dict]:
        """
        Get the prm load curve
        Args:
            prm: str
            client_id: int, client's id
            combined_versions: bool, if multiple versions exists we combine them (default True)
            version: return only the selected version
        Returns:
            List[Dict]
        """
        logger.info(f'Getting load curve with prm {prm} for client {client_id}')
        qs = cls.objects.filter(prm=prm, client_id=client_id).order_by('-version')
        if version is not None:
            qs = qs.filter(version=version)

        entries = []
        for entry in qs:
            entry_dict = vars(entry)
            reader = BufferReader(entry_dict['data'])
            ds = pd.read_feather(reader)
            if 'index' in ds.columns:
                ds.set_index('index', inplace=True)
            else:
                logger.info(f'data is malformed, remove key {prm}')
                entry.delete()
            entry_dict['data'] = ds.iloc[:, 0]
            entries.append(entry_dict)

        if len(entries) > 0 and combined_versions:
            ds_combined = ts_combine_first([e['data'] for e in entries])
            entries[0]['data'] = ds_combined
            return entries[0:1]
        return entries

    @classmethod
    def get_many_lc(cls, prms: [str], client_id: int, combined_versions=True) -> Dict[str, List[Dict]]:
        """
        Get many prms from cache
        Args:
            prms: list of prms
            client_id: int, client's id
            combined_versions: bool, if multiple versions exists we combine them (default True)
        Returns:
            Dict[str, List[Dict]]
        """
        qs = cls.objects.filter(prm__in=prms, client_id=client_id).order_by('-version')
        results = defaultdict(lambda: [])
        invalid_ids = []
        for entry in qs:
            reader = BufferReader(entry.data)
            ds = pd.read_feather(reader)
            try:
                ds.set_index('index', inplace=True)
            except KeyError:
                invalid_ids.append(entry.id)
                continue
            entry_dict = vars(entry)
            entry_dict['data'] = ds.iloc[:, 0]
            results[entry.prm].append(entry_dict)

        if len(invalid_ids) > 0:
            logger.info(f'data is malformed, for ids {invalid_ids}')
            cls.objects.filter(id__in=invalid_ids).delete()

        if len(results) > 0 and combined_versions:
            for prm, entries in results.items():
                ds_combined = ts_combine_first([e['data'] for e in entries])
                entries[0]['data'] = ds_combined
                results[prm] = entries[0:1]

        return results

    @classmethod
    def set_lc(cls, prm: str, value: pd.Series, client_id: int, versionning=False) -> None:
        """
        Set one prm to cache
        Args:
            prm: str
            value: pd.Series
            client_id: int, client's id
            versionning: bool, if True, a new version will be saved else the load curve is replaced (default False)

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
            if not versionning:
                cls.objects.update_or_create(client_id=client_id, prm=prm, defaults=dict(data=v))
            else:
                latest_version = cls.objects.filter(client_id=client_id, prm=prm).aggregate(Max('version'))[
                    'version__max']
                if latest_version is not None:
                    # If there's at least one object with the same prm, increment the version
                    version = latest_version + 1
                else:
                    # If there are no objects with the same prm, start with version 0
                    version = 0
                logger.info(f"version to insert {version}")
                cls.objects.create(client_id=client_id, prm=prm, data=v, version=version)
            logger.info(f'SET prm {prm} in cache DONE')
        else:
            raise ValueError(f'Cannot cache value of type {type(value)}, only dataframe are accepted')

    @classmethod
    def set_many_lc(cls, dataseries: Dict[str, pd.Series], client_id: int, versionning=False) -> None:
        """
        Update prms contained in dataseries dict
        Args:
            dataseries: dict(key: pd.Series)
            client_id: int, client's id
            versionning: bool, if True, a new version will be saved for each prm else the load curve is replaced
        Returns:
            bool
        """
        if len(dataseries) > 0:
            logger.info(f'Updating cache for {list(dataseries.keys())}')
        for prm, data in dataseries.items():
            if data is not None and not data.empty:
                cls.set_lc(prm, data, client_id, versionning=versionning)

    @classmethod
    def clear(cls, prms: [str], client_id: int, version=None) -> None:
        """
        Method to clear multiple prm from cache
        Args:
            prms: list of prms
            client_id: client's id
            version: version id to delete
        Returns:
            None
        """
        qs = cls.objects.filter(prm__in=prms, client_id=client_id)
        if version is not None:
            qs = qs.filter(version=version)
        qs.delete()

    @classmethod
    def clear_all(cls, client_id: int = None) -> None:
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
        app_label = 'hostore'
