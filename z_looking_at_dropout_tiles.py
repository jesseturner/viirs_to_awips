#--- run with viirs_to_awips environment
import gzip
import xarray as xr
import numpy as np
import io

file_dropout = "to_ldm_recent/RAMMB_VIIRS_I05_20250805_0651_011.nc.gz"
file_complete = "to_ldm_recent/RAMMB_VIIRS_I05_20250805_0832_010.nc.gz"
file_ldm = "ldm_file/RAMMB_VIIRS_I05_20250805_0651_012.nc.gz"

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