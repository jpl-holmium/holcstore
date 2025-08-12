<h1 id="readme-top">HoLcStore</h1>

HoLcStore is a Django app for creating a simple TimeSeries store in your database.

<!-- TOC -->
# Table of Contents
- [Getting Started](#getting-started)
- [Choose the appropriate store](#choose-the-appropriate-store)
- [Basic Usage : Store class](#basic-usage--store-class)
  - [Saving a timeserie to database](#saving-a-timeserie-to-database)
  - [Saving multiple timeseries to database](#saving-multiple-timeseries-to-database)
  - [Getting a load curve from the database](#getting-a-load-curve-from-the-database)
- [Basic Usage : TimeseriesStore class](#basic-usage-timeseriesstore-class)
  - [Define your class in models.py](#define-your-class-in-modelspy)
  - [Usage samples](#usage-samples)
- [Basic Usage : TimeseriesChunkStore class](#basic-usage-timeserieschunkstore-class)
<!-- /TOC -->

# Getting Started

1. Add "holcstore" to your INSTALLED_APPS setting like this
```python
INSTALLED_APPS = [
    ...,
    "holcstore",
]
```


2. Start using the abstract model ``Store`` by importing it
```python
from hostore.models import Store

class YourStore(Store):
    # add new fields

    class Meta(Store.Meta):
        abstract = False
        # add your meta
```
# Choose the appropriate store

## Store class
This class is used to store timeseries, using a key:value pattern. A prm key is used to reference the saved series.

Handle update and replace features.

## TimeseriesStore class
This class is used to store timeseries, using a user provided pattern through its model.

## TimeseriesChunkStore class
This class is used to store timeseries, using a user provided pattern through its model.

Handle update and replace features.

Store timeseries by chunk for a better performance with large timeseries.

User friendly API to perform a client-server sync.

# Basic Usage: Store class
This store is appropriate if you want to store using a "key - value" pattern.

#### Saving a timeserie to database

```python
from path.to.YourStore import YourStore
import pandas as pd

key = "3000014324556"
client_id = 0
idx = pd.date_range('2024-01-01 00:00:00+00:00', '2024-01-02 00:00:00+00:00', freq='30min')
ds = pd.Series(data=[1]*len(idx), index=idx)
# Put the load curve to the store without versionning
YourStore.set_lc(key, ds, client_id)
# If you want to activate the versionning 
YourStore.set_lc(key, ds, client_id, versionning=True)
# Each time you call set_lc with your id, a new version will be saved in the database
```

#### Saving multiple timeseries to database

```python
from path.to.YourStore import YourStore
import pandas as pd

key = "3000014324556"
client_id = 0
idx = pd.date_range('2024-01-01 00:00:00+00:00', '2024-01-02 00:00:00+00:00', freq='30min')
df = pd.Series(data={'key1': [1]*len(idx), 'key2': [2]*len(idx), }, index=idx)
# Put the load curve to the store without versionning
YourStore.set_many_lc(df, client_id)
# If you want to activate the versionning 
YourStore.set_many_lc(df, client_id, versionning=True)
```

#### Getting a load curve from the database

```python
from path.to.YourStore import YourStore

key = "3000014324556"
client_id = 0
# Get the load curve from the database
# I multiple versions exists, they will be combined beginning with the version 0 and using 
# https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.combine_first.html
datas = YourStore.get_lc(key, client_id)

if not datas:
    my_timeserie = datas[0]['data']
    last_updated = datas[0]['last_modified']

# If you want to retrieve all versions 
datas = YourStore.get_lc(key, client_id, combined_versions=False)
# datas contains all timeseries linked to key

# If you want to retrieve a specific version
datas = YourStore.get_lc(key, client_id, version=1)
```

# Basic Usage: TimeseriesStore class
This store is the easiest to use, but has less features than TimeseriesChunkStore

### Define your class in models.py
```python
class MyTimeseriesStore(TimeseriesStore):
    year = models.IntegerField()
    kind = models.CharField(max_length=100)

    class Meta(TimeseriesStore.Meta):
        unique_together = ('year', 'kind')
```


### Usage samples
```python
# specify attributes to set
ts_attrs = dict(year=2020, kind='a')

# build a timeserie 
ds_ts = pd.Series(...)  

# set timeserie to db
MyTimeseriesStore.set_ts(ts_attrs, ds_ts)

# update existing timeserie in db (will combine ds_ts + ds_ts_v2, giving priority to ds_ts_v2 datas)
MyTimeseriesStore.set_ts(ts_attrs, ds_ts_v2, update=True)

# get timeserie from db if unique match
ds_ts = MyTimeseriesStore.get_ts(ts_attrs, flat=True)

# get timeseries from db if multiple match
datas = MyTimeseriesStore.get_ts(ts_attrs)
ds_ts1 = datas[0]['data']

# admin.py
@admin.register(MyTimeseriesStore)
class MyTimeseriesStoreAdmin(admin.ModelAdmin):
  from hostore.admin_actions import download_timeseries_from_store
  actions = [download_timeseries_from_store]
    
```

# Basic usage: TimeseriesChunkStore class
## Compact time-series storage for Django + PostgreSQL

**TimeseriesChunkStore** is an abstract Django model that lets you persist
high-resolution time-series efficiently as compressed LZ4 blobs, while still
querying them through the ORM.

Main features
=============

| Feature                                    | Description                                                                                                                    |
|--------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------|
| **Chunking**                               | Split each series by `('year',)` or `('year','month')` so blobs stay small.                                                    |
| **Compression**                            | Data saved as LZ4-compressed NumPy buffers; index is rebuilt on the fly.                                                       |
| **Dense layout**                           | Each chunk is re-indexed on a regular grid (`STORE_FREQ`, `STORE_TZ`).                                                         |
| **Smart upsert**                           | `set_ts(..., update=True)` merges with existing data via `combine_first`.                                                      |
| **Bulk helpers**                           | `set_many_ts()` / `yield_many_ts()` for mass insert / streaming read.                                                          |
| **Sync ready**                             | `updates_queryset`, `list_updates`, `export_chunks`, `import_chunks` enable cheap client ↔ server replication.                 |
| **REST scaffolding**                       | `TimeseriesChunkStoreSyncViewSet` + `TimeseriesChunkStoreSyncClient` give you plug-and-play API endpoints and a Python client. |

Quick start
===========
### 1/ Define your store class
```python
# models.py
class MyChunkedStore(TimeseriesChunkStore):
    # Custom fields (can be seen as "axis keys")
    version = models.IntegerField()
    kind    = models.CharField(max_length=20)

    # Store settings
    CHUNK_AXIS = ('year', 'month')   # Chunking axis for timeseries storage. Configs : ('year',) / ('year', 'month')
    STORE_TZ   = 'Europe/Paris' # Chunking timezone (also timeseries output tz)
    STORE_FREQ   = '1h' # Timeseries storage frequency. (the store reindex input series but never resample)
    ALLOW_CLIENT_SERVER_SYNC = False # if True, enable the sync features
    CACHED_INDEX_SIZE = 120  # max number of date indexes kept in cache
```
Use `CACHED_INDEX_SIZE` to tune the size of the LRU cache used when rebuilding
indexes:
```python
class MyChunkedStore(TimeseriesChunkStore):
    CACHED_INDEX_SIZE = 360
```
The Custom fields are **strictly indexation axis** : you **must not** use them to store metadata or redundant data.

During django's setup, any class that inherits from TimeseriesChunkStore will have its Meta properties unique_together and indexes automatically edited, such as it contains all your keys and the mandatory keys from the abstract store.

**Do not** : 
 * define your own Meta.unique_together or Meta.indexes.
 * define those fields : 
`start_ts, data, dtype, updated_at, chunk_index, is_deleted`.
 * edit "Store settings" : `CHUNK_AXIS, STORE_TZ, STORE_FREQ, ALLOW_CLIENT_SERVER_SYNC` once the table has been created through migration. This **will** lead to data corruption.



### 2/ Use your store class
This summarize use cases with legal and illegal usages.
```python
# my_timeseries_usage.py
# Set one
import pandas as pd

attrs = {"version": 1, "kind": "kind1"}
MyChunkedStore.set_ts(attrs, my_series1)  # first write
MyChunkedStore.set_ts(attrs, my_series2, update=True)  # update (combine_first)
MyChunkedStore.set_ts(attrs, my_series3, replace=True)  # replace
MyChunkedStore.set_ts(attrs, my_series4)  # FAIL : attrs exists

# Get one
full = MyChunkedStore.get_ts(attrs)
window = MyChunkedStore.get_ts(attrs, start=pd.Timestamp("2024-01-01 00:00:00+01:00"), end=pd.Timestamp("2024-06-01 02:00:00+02:00"))
fail = MyChunkedStore.get_ts({"version": 1})  # FAIL : must specify all attrs
none = MyChunkedStore.get_ts({"version": 1, "kind": "nonexisting"})  # returns None : does not exists

# Set many
MyChunkedStore.set_many_ts(
  mapping={
    (5, "kind1"): my_series1,
    (5, "kind2"): my_series2,
  },
  keys=("version", "kind")
)

# Yield many
series_generator = MyChunkedStore.yield_many_ts({"version": 5})  # contains the 2 (serie, key_dict) from mapping
max_horodate = MyChunkedStore.get_max_horodate({"version": 5})  # maximum horodate of the 2 (serie, key_dict) from mapping

# CANNOT set many over existing
MyChunkedStore.set_many_ts(mapping, keys=("version", "kind"))  # FAIL : at least one value from mapping already exists
```

### 3/ Setup sync tools
Two TimeseriesChunkStore can be easily synchronized between a server and a client. Client and server models must be the same.

You must set ALLOW_CLIENT_SERVER_SYNC=True if you want to use those features. In that case please note some particular behaviour :
 * If you need to delete objects on server side, you must pass keep_tracking=True to delete method. Eg : ```ServerStore.objects.filter(version=1).delete(keep_tracking=True)```. Otherwise a ValueError will be raised.
 * You **cannot** use the set_ts(replace=False, update=False) or set_many_ts. Otherwise a ValueError will be raised.

#### 3.1/ Define the ViewSet (server side : serve data)
You can set throttle, auth etc. through as_factory kwargs.

```python
# views.py
from hostore.utils.ts_sync import TimeseriesChunkStoreSyncViewSet

YearSync = TimeseriesChunkStoreSyncViewSet.as_factory(MyChunkStoreServerSide, throttle_classes=[], **kwargs_views)  # pass ViewSet kwargs
router.register("ts/myendpoint", YearSync, basename="ts-myendpoint")
```

The `/updates/` endpoint uses DRF's limit/offset pagination. Requests can
specify `limit` and `offset` parameters and responses include standard
pagination metadata:

```
GET /ts/myendpoint/updates/?since=2024-01-01T00:00:00Z&limit=100

{
  "count": 1234,
  "next": "/ts/myendpoint/updates/?since=2024-01-01T00:00:00Z&limit=100&offset=100",
  "previous": null,
  "results": [
    { "attrs": {"id": 1}, "chunk_index": 0, ... },
    ...
  ]
}
```

Results are ordered by `updated_at`.


#### 3.2/ Define the API (client side : pull new data)
```python
# my_client_sync_module.py
from hostore.utils.ts_sync import TimeseriesChunkStoreSyncClient

client = TimeseriesChunkStoreSyncClient(
    endpoint="https://api.example.com/ts/myendpoint",
    store_model=MyChunkStoreClientSide,
)
client.pull(batch=100, page_size=500)      # fetch new/updated chunks, requesting 500 updates per page
```

`TimeseriesChunkStoreSyncClient.pull` follows the `next` links returned by
the server and continues fetching pages until there are no more results.

#### 3.3/ Use admin features
This allows you to download a zip file containing all the time series chunk selected
```python
# admin.py
@admin.register(MyChunkedStore)
class MyChunkedStoreAdmin(admin.ModelAdmin):
  from hostore.admin_actions import download_timeseries_from_chunkstore
  actions = [download_timeseries_from_chunkstore]
```

<p align="right">(<a href="#readme-top">back to top</a>)</p>


