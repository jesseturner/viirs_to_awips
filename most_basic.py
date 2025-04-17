import subprocess
import glob



def main():
    base_dir = '/home/jturner/VIIRS_to_AWIPS/'

    filepath_pattern = '/mnt/viirs/WI-CONUS/NPP/SDR-MBand/2025/03/25/?????_npp_d20250325_t2206*'

    file_list = glob.glob(filepath_pattern)

    print(file_list)

    cmd = [
        '/local/polar2grid_v_3_1/bin/polar2grid.sh',
        '-r', 'viirs_sdr',
        '-w', 'geotiff',
        '-p', 'm08',
        '-f'
    ] + file_list

    subprocess.call(cmd, cwd=base_dir)

if __name__ == '__main__':
    main()

