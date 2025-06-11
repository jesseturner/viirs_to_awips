#--- My version, cleaning up and fixing issues
#------ run on polarbear3 with python3
#------ rename to make it more clear this is the main processing code

from datetime import datetime
import argparse, glob, gzip, logging, os, shutil, subprocess, re

#-----------------------------------------------------

def parseArguments(raw_args, recent_file_threshold):
    #--- checking for incoming arguments that would change processing
    parser = argparse.ArgumentParser(description='Process incoming data from /mnt/viirs/WI-CONUS/NPP for AWIPS ingestion')
    parser.add_argument('-t', '--time', type=int,
                        help='Only bands with a new file in the last t seconds will be processed')
    parser.add_argument('-b', '--orbit', type=str,
                        help='Ignore the time search and only process files for orbit b (can omit the leading b)')
    parser.add_argument('-f', '--freq-band', type=str,
                        help='Only process the files for the indicated band groupings (valid inputs are "i" and "m"')
    parser.add_argument('-s', '--satellite', type=str,
                        help='Only process the selected satellite ("NPP", "J01", and "J02")')
    parser.add_argument('-d', '--file-date', type=str,
                        help='Run for the full 24 hours of a specific date (YYYYMMDD format) or for a specific hour (YYYYMMDDhh format)')
    
    args = parser.parse_args(raw_args)
    if args.time:
        recent_file_threshold = args.time

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
    
    return recent_file_threshold, file_year, file_month, file_day, args

#-----------------------------------------------------

def checkForDateArgument(args, log_prefix, recent_file_threshold, file_year, file_month, file_day):
    
    #--- if running for current date for past date
    if args.file_date:
        if len(args.file_date) == 10:
            logging.info(f'{log_prefix} Looking for data from {file_year}-{file_month}-{file_day} {args.file_date[-2:]}:00 UTC')
        elif len(args.file_date) == 8:
            logging.info(f'{log_prefix} Looking for all data from {file_year}-{file_month}-{file_day}')
        else:
            raise ValueError("file_date must be in YYYYMMDD or YYYYMMDDhh format")
    else: 
        logging.info(f'{log_prefix} Checking last {recent_file_threshold/60} minutes for new files')

    return

#-----------------------------------------------------

def makeListOrbits(args, band_dir):
    
    #--- check for specified orbit
    orbit_to_process = None
    if args.orbit:
        orbit_to_process = args.orbit if args.orbit[0] == 'b' else ('b' + args.orbit)

    orbits = []

    if orbit_to_process:
        orbits = [orbit_to_process]

    elif args.file_date:  #--- if running for specific date or date+hour
        if len(args.file_date) == 10:  #--- format: YYYYMMDDhh
            file_year = args.file_date[:4]
            file_month = args.file_date[4:6]
            file_day = args.file_date[6:8]
            file_hour = args.file_date[8:10]
            file_date_str = f"d{file_year}{file_month}{file_day}_t{file_hour}"
        elif len(args.file_date) == 8:  #--- format: YYYYMMDD
            file_year = args.file_date[:4]
            file_month = args.file_date[4:6]
            file_day = args.file_date[6:8]
            file_date_str = f"d{file_year}{file_month}{file_day}"
        else:
            raise ValueError("file_date must be in YYYYMMDD or YYYYMMDDhh format")

        matching_files = [
            os.path.basename(f) for f in glob.glob(os.path.join(band_dir, f"*{file_date_str}*"))
            if file_date_str in f
        ]

        for filename in matching_files:  #--- create orbits list from matching files
            orbit = filename.split('_')[5]
            if orbit not in orbits:
                orbits.append(orbit)

    return orbits

#-----------------------------------------------------

def setSatellitesAndBands(file_year, julian_day):
    #--- list of products processed
    #------ (previous note) when you add/remove products, you need to update the ldm injection script on the LDM server (cira-ldm1)
    m_ldm_file_tags = {'m08': 'M08', 'm10': 'M10', 'm11': 'M11', 'm12': 'M12', 'm13': 'M13', 'm14': 'M14', 'm15': 'M15', 'm16': 'M16'}
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

    return band_params, raw_sat_names

def gettingFilesFromOrbit(orbit_files_recent, prod_prefixes, sat, band_dir, band, orbit):
    
    #--- count number of orbit files, warning if one of the bands has a different number
    #------ I bet there is a simpler way of doing this
    num_files = len([f for f in orbit_files_recent if f[0:5] == prod_prefixes[0]])
    orbit_not_ready = False
    for prefix in prod_prefixes:
        if num_files != len([f for f in orbit_files_recent if f[0:5] == prefix]):
            orbit_not_ready = True
            break
        if orbit_not_ready:
            logging.info(f'{sat} orbit {orbit} bands are not fully filled in yet, not processing')
            continue
    #--- logging the files used
        filepaths = glob.glob(os.path.join(band_dir, prefix + '*_' + orbit + '_*'))
        if filepaths:
            filepaths.sort()  #--- sort to follow time order
            first_file = os.path.basename(filepaths[0])
            match = re.search(r'd(\d{8})_t(\d{2})(\d{2})', first_file)
            if match:
                date_str, hour, minute = match.groups()
                dt = datetime.strptime(f"{date_str}{hour}{minute}", "%Y%m%d%H%M")
                datetime_str = dt.strftime("%Y-%m-%d %H:%M UTC")
            else:
                datetime_str = "Unknown date/time"
        logging.info(f'Processing {len(filepaths)} VIIRS files for {sat} orbit {orbit} {band}-band at {datetime_str}')
        
        return datetime_str
    
#-----------------------------------------------------

def processingViirsFiles(dtstamp_dir, sat, band, orbit, prod_prefixes, band_dir):
    #--- processing the files
    processing_dir = dtstamp_dir + sat + '_' + band + '_' + orbit + '/'
    os.makedirs(processing_dir)
    raw_files_dir = processing_dir + 'raw_files/'
    os.makedirs(raw_files_dir)
    for prefix in prod_prefixes:
        for filepath in glob.glob(band_dir + prefix + '*_' + orbit + '_*'): 
            #--- copying the file
            shutil.copy(filepath, raw_files_dir)
    return processing_dir, raw_files_dir
    
#-----------------------------------------------------

def runningPolar2Grid(log_prefix, sat, band, base_dir, raw_files_dir, orbit, processing_dir):
    #--- running the polar2grid package
    #------ this is the very core of the processing
    print(log_prefix + 'Running p2g for ' + sat + ' ' + band + ' band ')
    p2g_status = subprocess.call(
        ['bash', base_dir + 'process_viirs_for_awips_' + band + '.sh', raw_files_dir], cwd=processing_dir)
    print(log_prefix + 'Finished running p2g for ' + sat + ' (orbit ' + orbit + ') ' + band + ' band with exit status ' + str(
        p2g_status))
    
#-----------------------------------------------------

def checkForMissingData(p2g_file_tags, processing_dir, raw_sat_name, sat, orbit, band, datetime_str):
    #--- check if channels are missing
    #------ there is definitely a simpler way of doing this
    missing_p2g_tags = []
    for p2g_tag in p2g_file_tags:
        if len(glob.glob(processing_dir + 'SSEC_AII_' + raw_sat_name + '_viirs_' + p2g_tag + '*.nc')) == 0:
            missing_p2g_tags.append(p2g_tag)
    #--- all files are missing tags, likely due to <10% grid coverage
    if len(missing_p2g_tags) == len(p2g_file_tags):
        logging.info(f'P2G returned no files for {sat} orbit {orbit} {band}-band at {datetime_str}')

    #--- some files are missing tags, likely due to lack of sun
    elif len(missing_p2g_tags) > 0:
        logging.info(f'P2G rejected {missing_p2g_tags} for {sat} orbit {orbit} {band}-band at {datetime_str}')
    
    return missing_p2g_tags

#-----------------------------------------------------

def nameAndFillFiles(p2g_file_tags, processing_dir, raw_sat_name, output_prod_name, ldm_file_tags, missing_p2g_tags, final_dir):
    #--- for each file type, names properly and fills with gzip-compressed data
    file_count = 0
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

            if p2g_tag not in missing_p2g_tags:
                with open(filepath, 'rb') as f_in, gzip.open(final_dir + new_filename, 'wb') as f_out:
                    f_out.writelines(f_in)
                file_count += 1 #--- counting files created

            #shutil.copy(copy_to_ldm_dir + new_filename, final_dir + new_filename)
            os.remove(filepath) #--- remove files from processing directory
        
    return file_count
    
#-----------------------------------------------------

def removeTempFiles(raw_files_dir, processing_dir):
    # Leave the directory behind if files we don't expect exist (e.g. we didn't finish copy .nc files out)
    num_h5_files = len(glob.glob(raw_files_dir + '/*.h5'))
    num_all_files = len(glob.glob(raw_files_dir + '/*'))
    if (num_all_files - num_h5_files) == 0:
        shutil.rmtree(raw_files_dir)

    if not os.listdir(processing_dir):
        shutil.rmtree(processing_dir)

    return

#=====================================================

def main(raw_args=None):
    
    #--- setting up variables
    base_dir = '/home/jturner/VIIRS_to_AWIPS/'
    recent_file_threshold_default = 30 * 60 #--- time range to process, files from most recent minutes
    recent_file_threshold = recent_file_threshold_default
    bands_to_process = ['m', 'i']
    sats_to_process = ['NPP', 'J01', 'J02']

    current_dt = datetime.now()
    year = current_dt.year
    month = current_dt.month
    day = current_dt.day

    #--- putting current datetime into str format
    file_year = str(year)
    file_month = '%02d' % month
    file_day = '%02d' % day
    file_date = file_year + file_month + file_day 
    current_datetime = current_dt.strftime('%Y%m%d%H%M%S')
    current_datetime_colons = current_dt.strftime('%Y-%m-%d %H:%M:%S')


    recent_file_threshold, file_year, file_month, file_day, args = parseArguments(raw_args, recent_file_threshold)

    #--- adding the logging information
    logging_dir = base_dir + 'logs/'
    if not os.path.exists(logging_dir):
        os.makedirs(logging_dir)
    logging.basicConfig(filename=logging_dir + file_date + '.log', level=logging.INFO)
    log_prefix = f'{current_datetime_colons} Z - '

    #--- creating dates to search for
    julian_day = datetime(int(file_year), int(file_month), int(file_day)).timetuple().tm_yday

    checkForDateArgument(args, log_prefix, recent_file_threshold, file_year, file_month, file_day)

    #--- creating a YYYYMMDD_hhmmss dir for an isolated workspace
    dtstamp_dir = base_dir + current_datetime + '/'
    if not os.path.exists(dtstamp_dir):
        os.makedirs(dtstamp_dir)

    #--- creating the output directories
    final_dir = base_dir + 'viirs_awips/'
    if not os.path.exists(final_dir):
        os.makedirs(final_dir)

    band_params, raw_sat_names = setSatellitesAndBands(file_year, julian_day)

    for sat in sats_to_process:    #--- NPP, J01, J02
        raw_sat_name = raw_sat_names[sat]
        for band in bands_to_process:   #--- m, i
            band_dir = band_params[band]['band_dir'].replace('_replacewithsat_', sat)
            prod_prefixes = band_params[band]['prod_prefixes']
            ldm_file_tags = band_params[band]['ldm_file_tags']
            p2g_file_tags = list(ldm_file_tags.keys())
            output_prod_name = band_params[band]['output_prod_name']

            orbits = makeListOrbits(args, band_dir)

            for orbit in orbits: 
                
                #--- create list of recent orbit files
                orbit_files_recent = [f for f in os.listdir(band_dir) if
                                      f[0:5] in prod_prefixes and '_' + orbit + '_' in f]

                datetime_str = gettingFilesFromOrbit(orbit_files_recent, prod_prefixes, sat, band_dir, band, orbit)

                processing_dir, raw_files_dir = processingViirsFiles(dtstamp_dir, sat, band, orbit, prod_prefixes, band_dir)
                        
                runningPolar2Grid(log_prefix, sat, band, base_dir, raw_files_dir, orbit, processing_dir)

                missing_p2g_tags = checkForMissingData(p2g_file_tags, processing_dir, raw_sat_name, sat, orbit, band, datetime_str)

                file_count = nameAndFillFiles(p2g_file_tags, processing_dir, raw_sat_name, output_prod_name, ldm_file_tags, missing_p2g_tags, final_dir)

                #--- logging files created for date
                pattern = os.path.join(final_dir, f'*{file_year}{file_month}{file_day}*.nc.gz')
                file_count_total = len(glob.glob(pattern))
                logging.info(f'Created {file_count} AWIPS files. Total for {file_year}-{file_month}-{file_day} is now {file_count_total}.')

                # Move the viirs2scmi logfile(s) (fairly certain they'll only ever be one, but to be safe...)
                for filepath in glob.glob(processing_dir + 'viirs*.log'):
                    file = os.path.basename(filepath)
                    shutil.copy(filepath, final_dir + file)
                    os.remove(filepath)

                removeTempFiles(raw_files_dir, processing_dir)

    #--- if directory is empty, delete it
    if not os.listdir(dtstamp_dir):
        shutil.rmtree(dtstamp_dir)

    logging.info(log_prefix + 'Finished at ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))


if __name__ == '__main__':
    main()
