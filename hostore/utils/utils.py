# Yield successive n-sized
# chunks from l.
import pandas as pd


def chunks(l, n):
    # looping till length l
    for i in range(0, len(l), n):
        yield l[i:i + n]

def slice_with_delay(ds: pd.Series, delay:pd.Timedelta):
    if ds is not None and not ds.empty:
        return ds.copy()[ds.index.min() + delay:]
    else:
        return ds
