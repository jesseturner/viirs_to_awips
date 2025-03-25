import subprocess

def main(raw_args=None):

    base_dir = '/home/jturner/VIIRS_to_AWIPS/'
    copy_to_ldm_dir = base_dir + 'to_ldm/'
    processing_dir = ''

    filepath = '/mnt/viirs/WI-CONUS/NPP/SDR-MBand/2025/03/25/SVM16_npp_d20250325_t1658462_e1700104_b69481_c20250325172959067496_cspp_dev.h5'

    band = 'm'
    raw_files_dir = filepath

    p2g_status = subprocess.call(
        ['bash', base_dir + 'process_viirs_for_awips_' + band + '.sh', raw_files_dir], cwd=processing_dir)



if __name__ == '__main__':
    main()