import rasterio
import matplotlib.pyplot as plt

# Open the GeoTIFF
with rasterio.open("your_file.tif") as src:
    data = src.read(1)  # Read the first band
    plt.imshow(data, cmap='viridis')
    plt.colorbar(label='Value')
    plt.title("GeoTIFF Visualization")
    plt.xlabel("Pixel X")
    plt.ylabel("Pixel Y")
    plt.show()
