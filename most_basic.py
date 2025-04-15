import subprocess
import glob



def main():
    base_dir = '/home/jturner/VIIRS_to_AWIPS/'

    #filepath = '/mnt/viirs/WI-CONUS/NPP/SDR-MBand/2025/03/25/SVM16_npp_d20250325_t1658462_e1700104_b69481_c20250325172959067496_cspp_dev.h5'
    #filepath_pattern = '/mnt/viirs/NOAA21/CONUS/SVI01/SVI01_j02_d20250314_t0523370_e0525016_b12129_c20250314060001301000_oebc_ops.h5'
    filepath_pattern = '/mnt/viirs/WI-CONUS/J02/SDR-MBand/2025/04/14*'
    #filepath_pattern = '/mnt/viirs/WI-CONUS/NPP/SDR-MBand/2025/03/25/?????_npp_d20250325_t2206*'

    file_list = glob.glob(filepath_pattern)

    cmd = [
        '/local/polar2grid_v_3_1/bin/polar2grid.sh',
        '--grid-configs', 'niznik_grids.conf',
        '-r', 'viirs_sdr',
        '-w', 'geotiff',
        '-p', 'm08',
        '-g', 'us_viirs2awips_m',
        '-f'
    ] + file_list

    subprocess.call(cmd, cwd=base_dir)

if __name__ == '__main__':
    main()

