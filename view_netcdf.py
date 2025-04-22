import netCDF4
import matplotlib.pyplot as plt
import gzip

#--- Local file to view
nc_file = 'SSEC_AII_npp_viirs_m08_LCC_T001_20250325_1033.nc'
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


# Plot the data
plt.imshow(variable, cmap='viridis')
plt.title("NetCDF Visualization")
plt.savefig(f"netCDF_image_example_2", dpi=200, bbox_inches='tight')

# Close the dataset
dataset.close()


#--- File from jpss-cloud4
nc_file = 'old_setup_example/RAMMB_VIIRS_M16_20250422_0758_002.nc.gz'
with gzip.open(nc_file, 'rb') as f:
    dataset = netCDF4.Dataset(f, 'r')

# List all variable names
print(dataset.variables.keys())

# Inspect a specific variable
var_name = 'data'  # Replace with the variable you want to inspect
variable = dataset.variables[var_name]

# Print variable details
print(f"Dimensions: {variable.dimensions}")
print(f"Shape: {variable.shape}")
print(f"Data: {variable[:]}")  # Print the entire array (or subset it)


# Plot the data
plt.imshow(variable, cmap='viridis')
plt.title("NetCDF Visualization (jpss-cloud4)")
plt.savefig(f"netCDF_image_example_jpss_cloud4", dpi=200, bbox_inches='tight')

# Close the dataset
dataset.close()