import subprocess
import glob
import yaml
from pathlib import Path
import re


def main():

    #--- Output desired
    config_file = "config_geotiff.yaml"
    #config_file = "config_awips.yaml"

    #--- Load in the config parameters
    with open(config_file, "r") as f:
        config = yaml.safe_load(f)
    base_dir = config["base_dir"]
    viirs_pattern = config["viirs_pattern"]
    p2g_source = config["p2g_source"]
    reader_name = config["reader_name"]
    writer_name = config["writer_name"]
    products_list = config["products_list"]

    #--- Retrieve the VIIRS data
    def extract_time_str(path):
        match = re.search(r"_t(\d{2})(\d{2})(\d{2})", path.name)
        if match:
            hour, minute, _ = match.groups()
            return f"{hour}:{minute} UTC"
        return "unknown time"

    file_list = glob.glob(viirs_pattern)
    first_path, last_path = Path(file_list[0]), Path(file_list[-1])

    region = first_path.parts[3]
    year, month, day = first_path.parts[6:9]
    date_str = f"{year}-{month}-{day}"

    time_str = extract_time_str(first_path)
    last_time_str = extract_time_str(last_path)

    print(f"{date_str}, from {time_str} to {last_time_str}")
    print(f"{region}")
    print(f"Number of VIIRS files: {len(file_list)}")

    #--- Run polar2grid
    cmd = [
        p2g_source,
        '-r', reader_name,
        '-w', writer_name,
        '-p', products_list,
        '-f'
    ] + file_list
    subprocess.call(cmd, cwd=base_dir)

if __name__ == '__main__':
    main()

