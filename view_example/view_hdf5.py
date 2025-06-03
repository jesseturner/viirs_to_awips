import h5py
import sys

def print_hdf5_structure(file_path):
    with h5py.File(file_path, 'r') as f:
        def visitor(name, obj):
            if isinstance(obj, h5py.Group):
                print(f"[Group]    {name}")
            elif isinstance(obj, h5py.Dataset):
                print(f"[Dataset]  {name}")
                print(f"           shape: {obj.shape}, dtype: {obj.dtype}")
                try:
                    sample = obj[()]  # get the data
                    if sample.size > 5:
                        preview = sample.flat[:5]
                        print(f"           sample: {preview} ...")
                    else:
                        print(f"           sample: {sample}")
                except Exception as e:
                    print(f"           Could not read sample: {e}")
        f.visititems(visitor)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python view_hdf5.py your_file.h5")
    else:
        print_hdf5_structure(sys.argv[1])
