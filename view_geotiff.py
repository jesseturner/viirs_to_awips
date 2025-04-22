import rasterio
import matplotlib.pyplot as plt

# Open the GeoTIFF
with rasterio.open("npp_viirs_m08_20250325_103315_us_viirs2awips_m.tif") as src:
    data = src.read(1)  # Read the first band
    plt.imshow(data, cmap='viridis')
    plt.title("GeoTIFF Visualization")
    plt.savefig(f"GeoTIFF_image_example_2", dpi=200, bbox_inches='tight')
