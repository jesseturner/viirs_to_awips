#--- My version, cleaning up and fixing issues
#------ run on polarbear3 with python3
#------ rename to make it more clear this is the main processing code

from datetime import datetime, timedelta
import argparse, glob, gzip, logging, math, os, shutil, subprocess, time


def main(raw_args=None):
    
    #--- setting up variables
    base_dir = '/home/jturner/VIIRS_to_AWIPS/'
    recent_file_threshold_default = 50 * 60 #--- time range to process, files from most recent minutes
    recent_file_threshold = recent_file_threshold_default
    orbit_to_process = None
    bands_to_process = ['m', 'i']
    sats_to_process = ['NPP', 'J01', 'J02']

    current_time = time.time()
    current_dt = datetime.now()
    year = current_dt.year
    month = current_dt.month
    day = current_dt.day

    #--- putting current datetime into str format
    file_year = str(year)
    file_month = '%02d' % month
    file_day = '%02d' % day

    datestamp = str(year) + '%02d' % month + '%02d' % day
    last_time_datestamp = datestamp
    utc_timestamp = current_dt.strftime('%H%M%S')

    julian_day = datetime(int(file_year), int(file_month), int(file_day)).timetuple().tm_yday


    #--- checking for incoming arguments that would change processing
    
    parser = argparse.ArgumentParser(description='Process incoming data from /mnt/viirs/WI-CONUS/NPP for AWIPS ingestion')
    parser.add_argument('-t', '--time', type=int,
                        help='Only bands with a new file in the last t seconds will be processed')
    parser.add_argument('-b', '--orbit', type=str,
                        help='Ignore the time search and only process files for orbit b (can omit the leading b)')
    parser.add_argument('-f', '--freq-band', type=str,
                        help='Only process the files for the indicated band groupings (valid inputs are "i" and "m"')
    parser.add_argument('-s', '--satellite', type=str,
                        help='Only process the files for the indicated band groupings (valid inputs are "i" and "m"')
    parser.add_argument('-d', '--file-date', type=str,
                        help='Necessary if the file folder to search is not today')
    
    args = parser.parse_args(raw_args)
    if args.time:
        recent_file_threshold = args.time
    if args.orbit:
        orbit_to_process = args.orbit if args.orbit[0] == 'b' else ('b' + args.orbit)
    if args.freq_band:
        if args.freq_band not in bands_to_process:
            logging.error('Invalid single-band input; valid options are "i" and "m"')
            exit(1)
        bands_to_process = [args.freq_band]
    if args.satellite:
        if args.satellite not in sats_to_process:
            logging.error('Invalid satellite input; valid options are "NPP", "J01", and "J02"')
            exit(1)
        sats_to_process = [args.satellite]
    if args.file_date:
        file_year = str(args.file_date[0:3 + 1])
        file_month = str(args.file_date[4:5 + 1])
        file_day = str(args.file_date[6:7 + 1])


#--- checking for when to process based on log files
    logging_dir = base_dir + 'logs/'
    if not os.path.exists(logging_dir):
        os.makedirs(logging_dir)
    elif recent_file_threshold == recent_file_threshold_default: #--- if no argument passed for time threshold
        calculate_recent_file_threshold = True
        last_time_log = last_time_datestamp + '.log' #--- create log file for current datetime
        if not os.path.exists(logging_dir + last_time_log): #--- if there is no log for current datetime, check yesterday
            yesterday_dt = datetime.now() - timedelta(days=1)
            last_time_datestamp = (str(yesterday_dt.year) + '%02d' % yesterday_dt.month + '%02d' % yesterday_dt.day)
            last_time_log = last_time_datestamp + '.log'
            if not os.path.exists(logging_dir + last_time_log): #--- if there is no log for yesterday, use default time threshold
                calculate_recent_file_threshold = False

        # FINLEY NOTES:  I don't understand why folks do this - look for a 'bread crumb' from the last time the
        #                processing worked correctly.  I think it is better to just look to see if the current
        #                end products have been created or not yet. If the data feed has been down for a while,
        #                the processing loop should start with the most recent data and then backfill what is
        #                missing (within reason).
        ##########################################################
        if calculate_recent_file_threshold: #--- determining how long to step back in time using the log
            with open(logging_dir + last_time_log, 'r') as f:
                contents = f.readlines()
                #--- grabbing the last datetime in the log
                info_lines = [l for l in contents if 'LOCK:' not in l]
                last_utc_time = info_lines[-1].split('Z')[0][-6:]
                last_utc_dt = datetime.strptime(last_time_datestamp + last_utc_time, "%Y%m%d%H%M%S")
                #--- cap the time threshold at an hour back
                recent_file_threshold = min(int(math.ceil((current_dt - last_utc_dt).total_seconds())), 3600)

    #--- adding the logging information
    logging.basicConfig(filename=logging_dir + datestamp + '.log', level=logging.INFO)
    log_prefix = ' ' + utc_timestamp + 'Z - '
    logging.info(log_prefix + 'Starting at ' + time.ctime())
    logging.info(log_prefix + 'Checking last ' + str(recent_file_threshold) + ' seconds for new files')

    #--- creating a YYYYMMDD_hhmmss dir for an isolated workspace
    dtstamp_dir = base_dir + datestamp + '_' + utc_timestamp + '/'
    if not os.path.exists(dtstamp_dir):
        os.makedirs(dtstamp_dir)

    #--- creating the output directories
    final_dir = base_dir + 'viirs_awips/'
    if not os.path.exists(final_dir):
        os.makedirs(final_dir)
    copy_to_ldm_dir = base_dir + 'to_ldm/'
    if not os.path.exists(copy_to_ldm_dir):
        os.makedirs(copy_to_ldm_dir)

    #--- list of products processed
    #------ (previous note) when you add/remove products, you need to update the ldm injection script on the LDM server (cira-ldm1)
    m_ldm_file_tags = {'m08': 'M08', 'm11': 'M11', 'm13': 'M13', 'm14': 'M14', 'm15': 'M15', 'm16': 'M16'}
    i_ldm_file_tags = {'i01': 'I01', 'i02': 'I02', 'i03': 'I03', 'i04': 'I04', 'i05': 'I05'}

    #--- set up dictionaries for each band
    band_params = {
        'm': {
            'band_dir': '/mnt/viirs/WI-CONUS/_replacewithsat_/SDR-MBand/' + file_year + '/' + str(julian_day) + '/',
            'prod_prefixes': ['GMTCO'] + ['SV' + tag for tag in list(m_ldm_file_tags.values())],
            'ldm_file_tags': m_ldm_file_tags,
            'output_prod_name': 'VIIRS'
        },
        'i': {
            'band_dir': '/mnt/viirs/WI-CONUS/_replacewithsat_/SDR-IBand/' + file_year + '/' + str(julian_day) + '/',
            'prod_prefixes': ['GITCO'] + ['SV' + tag for tag in list(i_ldm_file_tags.values())],
            'ldm_file_tags': i_ldm_file_tags,
            'output_prod_name': 'VIIRS'
        }
    }

    raw_sat_names = {'NPP': 'npp', 'J01': 'noaa20' , 'J02': 'noaa21'}

    for sat in sats_to_process:    #--- NPP, J01, J02
        raw_sat_name = raw_sat_names[sat]
        for band in bands_to_process:   #--- m, i
            band_dir = band_params[band]['band_dir'].replace('_replacewithsat_', sat)
            prod_prefixes = band_params[band]['prod_prefixes']
            ldm_file_tags = band_params[band]['ldm_file_tags']
            p2g_file_tags = list(ldm_file_tags.keys())
            output_prod_name = band_params[band]['output_prod_name']

            orbits = []

            if orbit_to_process:    #--- if argument added for orbit
                orbits = [orbit_to_process]
            else:
                recent_files = [] if not os.path.exists(band_dir) else \
                    [f for f in os.listdir(band_dir) if #--- if band dir exists, get files within time threshold
                     (current_time - os.path.getctime(band_dir + f)) < recent_file_threshold]

                for filename in recent_files: #--- create orbits list from recent files
                    orbit = filename.split('_')[5]
                    if orbit not in orbits:
                        orbits.append(orbit)

            for orbit in orbits: 
                #--- create list of recent orbit files
                orbit_files_recent = [f for f in os.listdir(band_dir) if
                                      f[0:5] in prod_prefixes and '_' + orbit + '_' in f]

                #--- count number of orbit files, warning if one of the bands has a different number
                #------ I bet there is a simpler way of doing this
                num_files = len([f for f in orbit_files_recent if f[0:5] == prod_prefixes[0]])
                orbit_not_ready = False
                for prefix in prod_prefixes:
                    if num_files != len([f for f in orbit_files_recent if f[0:5] == prefix]):
                        orbit_not_ready = True
                        break
                if orbit_not_ready:
                    logging.warning(
                        log_prefix + sat + ' Orbit ' + orbit + ' has unequal number of files for ' + band + ' bands - not processing')
                    continue

                #--- making the processing directory which will be inside YYYYMMDD_hhmmss
                logging.info(log_prefix + sat + ' Orbit ' + orbit + ' meets criteria for processing ' + band + ' bands')
                processing_dir = dtstamp_dir + sat + '_' + band + '_' + orbit + '/'
                os.makedirs(processing_dir)
                raw_files_dir = processing_dir + 'raw_files/'
                os.makedirs(raw_files_dir)
                for prefix in prod_prefixes:
                    for filepath in glob.glob(band_dir + prefix + '*_' + orbit + '_*'):
                        shutil.copy(filepath, raw_files_dir)

                #--- running the polar2grid package
                #------ this is the very core of the processing
                print(log_prefix + 'Running p2g for ' + sat + ' ' + band + ' band ')
                p2g_status = subprocess.call(
                    ['bash', base_dir + 'process_viirs_for_awips_' + band + '.sh', raw_files_dir], cwd=processing_dir)
                print(log_prefix + 'Finished running p2g for ' + sat + ' ' + band + ' band with exit status ' + str(
                    p2g_status))

                #--- check if channels are missing
                #------ there is definitely a simpler way of doing this
                missing_p2g_tags = ''
                for p2g_tag in p2g_file_tags:
                    if len(glob.glob(processing_dir + 'SSEC_AII_' + raw_sat_name + '_viirs_' + p2g_tag + '*.nc')) == 0:
                        missing_p2g_tags += p2g_tag
                #--- all files are missing tags, likely due to <10% grid coverage
                if len(missing_p2g_tags) == len(p2g_file_tags):
                    logging.warning(
                        log_prefix + 'No files generated for ' + sat + ' ' + orbit + ' ' + band + ' band; this is most often due to less than 10% of the grid covered')
                #--- some files are missing tags, likely due to lack of sun
                elif len(missing_p2g_tags) > 0:
                    logging.warning(
                        log_prefix + 'Not keeping files for ' + sat + ' ' + orbit + ' ' + band + ' band because ' + p2g_tag + ' files are missing; this is most often due to no sun')

                #--- 
                for p2g_tag in p2g_file_tags:
                    for filepath in glob.glob(
                            processing_dir + 'SSEC_AII_' + raw_sat_name + '_viirs_' + p2g_tag + '*.nc'):
                        filename = os.path.basename(filepath)
                        filename_pieces = filename.split('_')

                        # polar2grid default is SSEC_AII_[raw_sat_name]_viirs_m##_LCC_Tttt_yyyymmdd_hhmm.nc
                        # desired output is RAMMB_ppppp_bbb_yyyymmdd_hhmm_ttt.nc.gz
                        # (ppppp = VIIRS, others later?), (bbb = band, e.g. M08, I03)
                        new_filename = ('RAMMB_' + output_prod_name + '_' + ldm_file_tags[p2g_tag] + '_' +
                                        filename_pieces[7] + '_' + filename_pieces[8][:-3] + '_' +
                                        filename_pieces[6][1:] + '.nc.gz')

                        if not missing_p2g_tags:
                            with open(filepath, 'rb') as f_in, gzip.open(copy_to_ldm_dir + new_filename, 'wb') as f_out:
                                f_out.writelines(f_in)
                            shutil.copy(copy_to_ldm_dir + new_filename, final_dir + new_filename)
                        os.remove(filepath) #--- remove files from processing directory

                # Move the virrs2scmi logfile(s) (fairly certain they'll only ever be one, but to be safe...)
                for filepath in glob.glob(processing_dir + 'viirs2scmi*.log'):
                    file = os.path.basename(filepath)
                    shutil.copy(filepath, final_dir + file)
                    os.remove(filepath)

                # Leave the directory behind if files we don't expect exist (e.g. we didn't finish copy .nc files out)
                num_h5_files = len(glob.glob(raw_files_dir + '/*.h5'))
                num_all_files = len(glob.glob(raw_files_dir + '/*'))
                if (num_all_files - num_h5_files) == 0:
                    shutil.rmtree(raw_files_dir)

                if not os.listdir(processing_dir):
                    shutil.rmtree(processing_dir)

    if not os.listdir(dtstamp_dir):
        shutil.rmtree(dtstamp_dir)

    logging.info(log_prefix + 'Finished at ' + time.ctime())


if __name__ == '__main__':
    main()
