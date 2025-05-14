#--- Likely needs VENV setup to run with these packages
import netCDF4
import matplotlib.pyplot as plt
import gzip
import os
import shutil
import subprocess

#--- File from jpss-cloud4
file_pattern = 'RAMMB_VIIRS_M15_20250425_1910_002.nc.gz'

remote_user = 'jturner'
remote_host = 'jpss-cloud4'
remote_path = '/data2/mniznik/viirs_awips/'
local_path = 'awips_example_data_original/'

scp_command = ['scp', f'{remote_user}@{remote_host}:{remote_path}{file_pattern}', f'{local_path}{file_pattern}']
result = subprocess.run(scp_command)
if result.returncode != 0:
    print("SCP failed")

name_without_gz = file_pattern.replace('.nc.gz', '')  # 'RAMMB_VIIRS_M16_20250422_0758_002'
nc_file = os.path.join('awips_example_data_original', f'{name_without_gz}.nc')
save_name = '_'.join(name_without_gz.split('_')[2:])  # 'M16_20250422_0758_002'
save_file = os.path.join('awips_example_data_original', save_name)

# First decompress the .gz
with gzip.open(f'{local_path}{file_pattern}', 'rb') as f_in, open(nc_file, 'wb') as f_out:
    shutil.copyfileobj(f_in, f_out)

# Then open the uncompressed NetCDF file
dataset = netCDF4.Dataset(nc_file, 'r')

# List all variable names
print(dataset.variables.keys())

# Inspect a specific variable
var_name = 'data'  # Replace with the variable you want to inspect
variable = dataset.variables[var_name]

# Print variable details
print(f"Dimensions: {variable.dimensions}")
print(f"Shape: {variable.shape}")
print(f"Data: {variable[:]}")  # Print the entire array (or subset it)

print("Global Attributes:")
for attr in dataset.ncattrs():
    print(f"{attr}: {getattr(dataset, attr)}")

print(f"x: {dataset.variables['x'][0]} to {dataset.variables['x'][-1]}") 
print(f"y: {dataset.variables['y'][0]} to {dataset.variables['y'][-1]}") 

# Plot the data
plt.imshow(variable, cmap='viridis')
plt.title("NetCDF Visualization (jpss-cloud4)")
plt.savefig(save_file, dpi=200, bbox_inches='tight')

# Close the dataset
dataset.close()