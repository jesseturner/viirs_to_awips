#--- Likely needs VENV setup to run with these packages
import netCDF4
import matplotlib.pyplot as plt
import os

#--- Local file to view
nc_file = 'awips_example_data/SSEC_AII_npp_viirs_m08_LCC_T001_20250325_1033.nc'

name_without_nc = nc_file.replace('.nc', '')
save_name = '_'.join(name_without_nc.split('_')[4:])
save_file = os.path.join('awips_example_data', save_name)
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
plt.savefig(save_file, dpi=200, bbox_inches='tight')

# Close the dataset
dataset.close()
