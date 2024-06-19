# HoLcStore

HoLcStore is a Django app for creating a simple TimeSeries store in your database.

## Getting Started

### Prerequisites

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

### Basic Usage

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
    
    # If you want to retreive all versions 
    datas = YourStore.get_lc(key, client_id, combined_versions=False)
    # datas contains all timeseries linked to key
    
    # If you want to retreive a specific version
    datas = YourStore.get_lc(key, client_id, version=1)
```

<p align="right">(<a href="#readme-top">back to top</a>)</p>


