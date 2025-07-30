#--- run with viirs_to_awips environment
import gzip
import xarray as xr
import numpy as np
import io

file = "to_ldm_recent/RAMMB_VIIRS_I05_20250730_0703_022.nc.gz"

with gzip.open(file, 'rb') as f:
    uncompressed = f.read()

ds = xr.open_dataset(io.BytesIO(uncompressed))

for var in ds.data_vars:
    data = ds[var].values
    nan_ratio = np.isnan(data).sum() / data.size
    print(f"{var}: {nan_ratio*100:.2f}% NaNs")