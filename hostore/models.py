import datetime as dt
import io
import logging
from collections import defaultdict
from typing import List, Dict, Union

import pandas as pd
from django.db import models
from django.db.models import Max
from packaging.version import Version
from pyarrow import BufferReader

from .manager import StoreQuerySet
from .utils.timeseries import ts_combine_first, check_ts_completeness
from .utils.utils import chunks, slice_with_delay

logger = logging.getLogger(__name__)
# below this version of pandas we must remove datetime index from the serie to be feathered
MIN_PANDAS_VERSION_FEATHER_SAVE_DATETIME_INDEX = '2.2.0'


class Store(models.Model):
    client_id = models.IntegerField()
    prm = models.CharField(max_length=70)
    last_modified = models.DateTimeField(auto_now=True)
    created_at = models.DateTimeField(auto_now_add=True, null=True)
    data = models.BinaryField(blank=True, null=True)
    version = models.IntegerField(default=0)

    objects = StoreQuerySet.as_manager()

    class Meta:
        abstract = True
        unique_together = ('prm', 'client_id', 'version')
        indexes = [models.Index(fields=['prm', 'client_id']), ]

    @classmethod
    def count(cls, client_id: int, custom_filters=None):
        if custom_filters is None:
            custom_filters = {}
        return cls.objects.filter(client_id=client_id, **custom_filters).count()

    @classmethod
    def find_holes(cls, client_id: int, sd: dt.datetime, ed: dt.datetime, freq='30min', prms: List[str] = None,
                   chunk_size=50,
                   freq_margin=pd.Timedelta(minutes=0),
                   custom_filters=None,
                   combined_by=('prm',), order_by=('-version',)) -> Dict[str, List]:
        """
        Find holes (missing data periods) for multiple prms within a specified datetime range.

        This method retrieves data from a database for each parameter associated with a client_id,
        checks each dataset for missing data within the specified datetime range, and identifies the start
        and end of each missing data period.

        :param client_id: ID of the client for whom the data is being checked.
        :param sd: Start datetime of the range to check for holes.
        :param ed: End datetime of the range to check for holes.
        :param freq: Frequency at which the data is expected, defaulting to '30min'.
        :param prms: Optional list of prms to check; if None, all parameters for the client are checked.
        :param chunk_size: The size of chunks to break the parameter list into for batch processing.
                           This helps in managing memory and processing large datasets efficiently, defaulting to 50.
        :param freq_margin: Freq margin for null sequences
        :param custom_filters: (optionnal) None or dict
        :param combined_by: set of attributes for ordering load_curves before combine_first
        :return: A dictionary with prm as keys and a list of tuples (start, end) indicating
                 the missing data periods for each parameter. Start and end are included
        """
        if custom_filters is None:
            custom_filters = {}
        # If no prm are specified, retrieve all prm for the given client from the database
        if prms is None:
            prms = list(cls.objects.filter(client_id=client_id, **custom_filters).values_list('prm', flat=True))

        # Iterate over prms in chunks to manage memory usage and efficiency
        for prm_chunk in chunks(prms, chunk_size):
            data_by_prm = cls.get_many_lc(prm_chunk, client_id,
                                          combined_versions=True,
                                          combined_by=combined_by,
                                          order_by=order_by,
                                          **custom_filters)

            # Check each parameter's data for completeness
            for prm in prm_chunk:
                nulls_seqs = []
                data = data_by_prm[prm]
                if len(data) == 0:
                    # If there's no data for a prm, the entire range is considered as a hole
                    nulls_seqs = [(sd, ed)]
                else:
                    # Data is wrapped in a list, and there's only one dataset per prm (combined_versions=True)
                    assert len(data) == 1
                    nulls_seqs = check_ts_completeness(data[0]['data'], sd, ed, freq=freq, freq_margin=freq_margin)

                # Yield the output for this prm
                # This allows processing part by part without waiting for the entire operation to complete
                yield prm, nulls_seqs

    @classmethod
    def get_lc(cls, prm: str, client_id: int, combined_versions=True, version: int = None, custom_filters=None,
               combined_by=('prm',),
               order_by=('-version',),
               combined_delay=None) -> List[Dict]:
        """
        Get the prm load curve
        Args:
            prm: str
            client_id: int, client's id
            combined_versions: bool, if multiple versions exists we combine them (default True)
            version: return only the selected version
            custom_filters: (optionnal) None or dict
            combined_by: set of attributes to bombined by
            order_by: set of attributes for ordering load_curves before combine_first
            combined_delay : pd.Timedelta object, optional to slice the beginning of the time series
        Returns:
            List[Dict]
        """
        logger.info(f'Getting load curve with prm {prm} for client {client_id}')
        if custom_filters is None:
            custom_filters = {}

        qs = cls.objects.filter(prm=prm, client_id=client_id, **custom_filters).order_by(*order_by)
        if version is not None:
            qs = qs.filter(version=version)

        entries = []
        for entry in qs:
            entry_dict = vars(entry)
            reader = BufferReader(entry_dict['data'])
            ds = pd.read_feather(reader)
            # fix feather index
            if 'index' in ds.columns:
                ds.set_index('index', inplace=True)

            entry_dict['data'] = ds.iloc[:, 0]
            entries.append(entry_dict)

        cleared_combined_by = set([attr for attr in combined_by])
        ds_combined_dict = defaultdict(lambda: None)
        if len(entries) > 0 and combined_versions:
            for e in entries:
                key = str([e[attr] for attr in cleared_combined_by])
                if combined_delay is not None:
                    e['data'] = slice_with_delay(e['data'], combined_delay)
                if ds_combined_dict[key] is None:
                    ds_combined_dict[key] = e
                else:
                    ds_combined_dict[key]['data'] = ts_combine_first([ds_combined_dict[key]['data'], e['data']])
            entries = list(ds_combined_dict.values())
        return entries

    @classmethod
    def get_many_lc(cls, prms: [str], client_id: int, combined_versions=True,
                    custom_filters=None,
                    combined_by=('prm',),
                    order_by=('-version',),
                    combined_delay=None) -> Dict[str, List[Dict]]:
        """
        Get many prms from cache
        Args:
            prms: list of prms
            client_id: int, client's id
            combined_versions: bool, if multiple versions exists we combine them (default True)
            custom_filters: dict used in filter
            combined_by: set of attributes to bombined by
            order_by: set of attributes for ordering load_curves before combine_first
            combined_delay : pd.Timedelta object, optional to slice the beginning of the time series
        Returns:
            Dict[str, List[Dict]]
        """
        if custom_filters is None:
            custom_filters = {}
        qs = cls.objects.filter(prm__in=prms, client_id=client_id, **custom_filters).order_by(*order_by)
        results = defaultdict(lambda: [])
        for entry in qs:
            reader = BufferReader(entry.data)
            ds = pd.read_feather(reader)
            # fix feather index
            if 'index' in ds.columns:
                ds.set_index('index', inplace=True)
            entry_dict = vars(entry)
            entry_dict['data'] = ds.iloc[:, 0]
            results[entry.prm].append(entry_dict)

        cleared_combined_by = set([attr for attr in combined_by])
        ds_combined_dict = defaultdict(lambda: defaultdict(lambda: None))
        if len(results) > 0 and combined_versions:
            for prm, entries in results.items():
                for e in entries:
                    key = str([e[attr] for attr in cleared_combined_by])
                    if combined_delay is not None:
                        e['data'] = slice_with_delay(e['data'], combined_delay)
                    if ds_combined_dict[prm][key] is None:
                        ds_combined_dict[prm][key] = e
                    else:
                        ds_combined_dict[prm][key]['data'] = ts_combine_first([ds_combined_dict[prm][key]['data'], e['data']])
            for prm in results:
                results[prm] = list(ds_combined_dict[prm].values())

        return results

    @classmethod
    def set_lc(cls, prm: str,
               value: pd.Series,
               client_id: int,
               versionning=False,
               versionning_by=('prm',),
               attributes_to_set=None) -> None:
        """
        Set one prm to cache
        Args:
            prm: str
            value: pd.Series
            client_id: int, client's id
            versionning: bool, if True, a new version will be saved else the load curve is replaced (default False)
            versionning_by: set of attributes used for versionning (prm is mandatory)
            attributes_to_set: (optionnal) None or dict
        Returns:
            None
        """
        if attributes_to_set is None:
            attributes_to_set = {}
        logger.info(f'SET key {prm} in cache')
        if isinstance(value, pd.Series):
            if value.isnull().all():
                logger.warning(f'CACHE : Key {prm} is ignored because data is null')
                return
            df = value.to_frame(name=prm)
            # fix feather index
            if Version(pd.__version__) < Version(MIN_PANDAS_VERSION_FEATHER_SAVE_DATETIME_INDEX):
                df.reset_index(inplace=True, names=['index'])
            buf = io.BytesIO()
            df.to_feather(buf, compression='lz4')
            v = buf.getvalue()
            if not versionning:
                cls.objects.update_or_create(client_id=client_id, prm=prm, defaults=dict(data=v), **attributes_to_set)
            else:
                assert 'prm' in versionning_by
                _filters = {'prm': prm, **{k: v for k, v in attributes_to_set.items() if k in versionning_by}}
                latest_version = cls.objects.filter(client_id=client_id, **_filters).aggregate(Max('version'))['version__max']
                if latest_version is not None:
                    # If there's at least one object with the same prm, increment the version
                    version = latest_version + 1
                else:
                    # If there are no objects with the same prm, start with version 0
                    version = 0
                logger.info(f"version to insert {version}")
                cls.objects.create(client_id=client_id, prm=prm, data=v, version=version, **attributes_to_set)
            logger.info(f'SET prm {prm} in cache DONE')
        else:
            raise ValueError(f'Cannot cache value of type {type(value)}, only dataframe are accepted')

    @classmethod
    def set_many_lc(cls, dataseries: Dict[str, pd.Series], client_id: int, versionning=False,
                    versionning_by=('prm',),
                    attributes_to_set=None) -> None:
        """
        Update prms contained in dataseries dict
        Args:
            dataseries: dict(key: pd.Series)
            client_id: int, client's id
            versionning: bool, if True, a new version will be saved for each prm else the load curve is replaced
            versionning_by: set of attributes used for versionning (prm is mandatory)
            attributes_to_set: (optionnal) None or dict
        Returns:
            None
        """
        if len(dataseries) > 0:
            logger.info(f'Updating cache for {list(dataseries.keys())}')
        for prm, data in dataseries.items():
            if data is not None and not data.empty:
                cls.set_lc(prm, data, client_id,
                           versionning=versionning,
                           versionning_by=versionning_by,
                           attributes_to_set=attributes_to_set)

    @classmethod
    def clear(cls, prms: [str], client_id: int, version=None, custom_filters=None) -> None:
        """
        Method to clear multiple prm from cache
        Args:
            prms: list of prms
            client_id: client's id
            version: version id to delete
            custom_filters: (optionnal) None or dict
        Returns:
            None
        """
        if custom_filters is None:
            custom_filters = {}
        qs = cls.objects.filter(prm__in=prms, client_id=client_id, **custom_filters)
        if version is not None:
            qs = qs.filter(version=version)
        qs.delete()

    @classmethod
    def clear_all(cls, client_id: int = None, custom_filters=None) -> None:
        """
        Method to clear multiple prm from cache
        Args:
            client_id: client's id
            custom_filters: (optionnal) None or dict
        Returns:
            None
        """
        if custom_filters is None:
            custom_filters = {}
            qs = cls.objects.filter(**custom_filters)
        else:
            qs = cls.objects.all()
        if client_id is not None:
            qs.filter(client_id=client_id).delete()
        else:
            qs.delete()


class TestDataStore(Store):
    class Meta(Store.Meta):
        abstract = False
        app_label = 'hostore'


class TestDataStoreWithAttribute(Store):
    year = models.IntegerField()
    class Meta(Store.Meta):
        abstract = False
        app_label = 'hostore'
        unique_together = ('prm', 'client_id', 'year', 'created_at')


class TimeseriesStore(models.Model):
    data = models.BinaryField(blank=True, null=True)

    class Meta:
        abstract = True
        unique_together = ('data', )  # must be customized
        indexes = [models.Index(fields=['data']), ]  # must be customized

    @classmethod
    def get_ts(cls, ts_attributes: dict, flat=False, _qs=None) -> Union[pd.Series, List[Dict]]:
        """
        Get the timeseries matching ts_attributes

        Args:
            ts_attributes: dict : specify attributes to get
            flat: bool : whether the query must return only one results (and return only data serie). when using flat
            option, there must be one and only one matching result
            _qs : private argument to pass ts_attributes queryset
        Returns:
            List[Dict]
        """
        full_key = repr(ts_attributes)
        logger.debug(f'GET key {full_key} in cache')

        if _qs is not None:
            qs = _qs
        else:
            qs = cls.objects.filter(**ts_attributes)

        entries = []
        for entry in qs:
            entry_dict = vars(entry)
            reader = BufferReader(entry_dict['data'])
            df = pd.read_feather(reader)
            # fix feather index
            if 'index' in df.columns:
                df.set_index('index', inplace=True)

            entry_dict['data'] = df.iloc[:, 0]
            entries.append(entry_dict)

        logger.debug(f'GET key {full_key} in cache DONE')
        if flat:
            if len(entries) == 1:
                return entries[0]['data']
            elif len(entries) == 0:
                raise ValueError(f'No serie found for key {full_key}')
            else:
                raise ValueError(f'Multiple series found for key {full_key}')
        else:
            return entries

    @classmethod
    def set_ts(cls, ts_attributes: dict, ds_ts: pd.Series, update=False, replace=False) -> None:
        """
        Set one timeserie with ts_attributes keys

        Args:
            ts_attributes: dict : specify attributes to set
            ds_ts: pd.Series
            update: bool : if True, allow to update an existing ts (combine first existing and new one)
            replace: bool : if True, allow to replace an existing ts
        Returns:
            None
        """
        if update and replace:
            raise ValueError('update and replace are mutually exclusive')

        if ts_attributes is None:
            raise ValueError(f'ts_attributes is None')

        full_key = repr(ts_attributes)
        logger.debug(f'SET key {full_key} in cache')

        if not isinstance(ds_ts, pd.Series):
            raise ValueError(f'Cannot cache value of type {type(ds_ts)}, only pd.Series are accepted')

        if ds_ts.isnull().all():
            logger.warning(f'CACHE : Key {full_key} is ignored because data is null')
            return

        qs = cls.objects.filter(**ts_attributes)
        if qs.exists():
            if update:
                ds_ts_existing = cls.get_ts(ts_attributes, flat=True, _qs=qs)
                ds_ts_saved = ts_combine_first([ds_ts, ds_ts_existing])
            elif replace:
                cls.clear(ts_attributes)
                ds_ts_saved = ds_ts
            else:
                raise ValueError(f'Trying save over existing ts without update or replace option: {ts_attributes}')
        else:
            ds_ts_saved = ds_ts

        # pandas to feather
        df = ds_ts_saved.to_frame(name='name')

        # fix feather index
        if Version(pd.__version__) < Version(MIN_PANDAS_VERSION_FEATHER_SAVE_DATETIME_INDEX):
            # remove index from serie
            df.reset_index(inplace=True, names=['index'])

        buf = io.BytesIO()
        df.to_feather(buf, compression='lz4')
        v = buf.getvalue()
        cls.objects.update_or_create(defaults=dict(data=v), **ts_attributes)
        logger.debug(f'SET key {full_key} in cache DONE')

    @classmethod
    def clear(cls, custom_filters) -> None:
        """
        Method to clear multiple prm from cache
        Args:
            custom_filters: dict
        Returns:
            None
        """
        qs = cls.objects.filter(**custom_filters)
        qs.delete()


class TestTimeseriesStoreWithAttribute(TimeseriesStore):
    year = models.IntegerField()
    kind = models.CharField(max_length=100)

    class Meta(TimeseriesStore.Meta):
        abstract = False
        app_label = 'hostore'
        unique_together = ('year', 'kind')
