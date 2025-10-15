#--- run with viirs_to_awips environment
import gzip
import xarray as xr
import numpy as np
import io

file_dropout = "RAMMB_VIIRS_I05_20250930_0649_010.nc.gz"
file_complete = "RAMMB_VIIRS_I05_20250930_0649_010.nc.gz"
#--- Using the following:
#  scp ldm@cira-ldm1:/home/ldm/products_sent/viirs_i/I05/RAMMB_VIIRS_I05_20250930_0649_010.nc.gz .
file_ldm = "RAMMB_VIIRS_I05_20250930_0649_010.nc.gz"


def open_file(file):
    with gzip.open(file, 'rb') as f:
        uncompressed = f.read()
    ds = xr.open_dataset(io.BytesIO(uncompressed))
    return ds

ds_dropout = open_file(file_dropout)
ds_complete = open_file(file_complete)
ds_ldm = open_file(file_ldm)

print(ds_dropout.data.values)
print(ds_complete.data.values)
print(ds_ldm.data.values)