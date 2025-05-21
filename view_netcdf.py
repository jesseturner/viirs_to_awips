#--- Likely needs VENV setup to run with these packages
import netCDF4
import matplotlib.pyplot as plt
import gzip
import shutil

#--- File from jpss-cloud4
file_path = 'to_ldm/RAMMB_VIIRS_I01_20250521_1902_075.nc.gz'

nc_file = file_path.replace('.nc.gz', '')  # 'RAMMB_VIIRS_M16_20250422_0758_002'
save_name = '_'.join(nc_file.split('_')[2:])  # 'M16_20250422_0758_002'

# First decompress the .gz
with gzip.open(file_path, 'rb') as f_in, open(nc_file, 'wb') as f_out:
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
plt.savefig(save_name, dpi=200, bbox_inches='tight')

# Close the dataset
dataset.close()