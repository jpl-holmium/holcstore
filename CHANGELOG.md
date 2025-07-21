### v0.5.3a - 2025-07-18
- Added method get_max_horodate
- Fixed migration auto constraint and indexes
- Safe API to avoid data corruption with sync tools
- Perf optim
- Removed test models

### v0.5.2 - 2025-07-15
- Fixed issue with client sync + replace of existing serie
- Fixed django setup issue
- Thread safe (atomic) update
- Removed ITER_CHUNK_SIZE, BULK_CREATE_BATCH_SIZE properties from store definition

### v0.5.1 - 2025-07-09
- TimeseriesChunkStore handle synchronization of deleted items
- Fix requirement lz4

### v0.5.0 - 2025-07-04
- Added TimeseriesChunkStore : store efficiently dense timeseries, with automatic chunking and compression. Easy client-server syncronisation through a dedicated client class and viewset.

### v0.4.1 - 2024-09-16
- Correct file extension for Pypi description

### v0.4.0 - 2024-09-16
- Add classmethod find_groups to the model Store. Returns all missing intervals and the prms/keys concerned.

### v0.3.4 - 2024-09-12
- Remove automatic registering of test model + add packaging in requirements.txt

### v0.3.3 - 2024-09-06
- Added download_timeseries_from_store admin action : download zipped timeseries content from django admin

### v0.3.2 - 2024-09-06
- Added TimeseriesStore encode_serie and decoded_ts_data : allow to access to decoded data from object

### v0.3.1 - 2024-08-30
- Added TimeseriesStore.set_ts replace kwarg : allow to replace existing serie

### v0.3.0 - 2024-08-26
- Added TimeseriesStore : simple store for generic time series (generic multiple keys indexing)

### 0.2.5 - 2024-06-19
- Store class : store (double keys client_id+prm indexing)