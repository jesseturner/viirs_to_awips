#------ run on polarbear3 with python3
#------ rename to make it more clear this is the main processing code

from datetime import datetime
import argparse, glob, gzip, logging, os, shutil, subprocess, re, sys
from dataclasses import dataclass
from pprint import pprint

#=====================================================

@dataclass
class State:
    base_dir: str = None
    sats_to_process: list[str] = None
    bands_to_process: list[str] = None
    orbits_to_process: list[str] = None
    current_dt: datetime = None
    file_dt: datetime = None
    log_prefix: str = None
    dtstamp_dir: str = None
    final_dir: str = None
    band_params: dict[str, str] = None
    raw_sat_names: dict[str, str] = None
    filepaths: list[str] = None
    raw_files_dir: str = None
    processing_dir: str = None
    missing_p2g_tags: list[str] = None

def main(raw_args=None):

    state = State()
    setUpVariables(state)
    startLogging(state)
    parseArguments(raw_args, state)
    createTempAndOutputDir(state)
    setSatellitesAndBands(state)
    getOrbits(state)
    gettingFilesFromOrbit(state)
    grabbingViirsFiles(state)
    runningPolar2Grid(state)
    checkForMissingData(state)
    nameAndFillFiles(state)
    removeTempFiles(state)
    finishAndClean(state)
    pprint(state)

    
#=====================================================

def setUpVariables(state: State):

    state.base_dir = os.getcwd()
    state.bands_to_process = ['m', 'i']
    state.sats_to_process = ['NPP', 'J01', 'J02']
    state.current_dt = datetime.now() #datetime(2025, 7, 1, 10, 33, 33)
    state.file_dt = datetime.now() #datetime(2025, 7, 1, 10, 33, 33)

    return

#-----------------------------------------------------

def startLogging(state: State): 

    logging_dir = state.base_dir + '/logs/'
    if not os.path.exists(logging_dir):
        os.makedirs(logging_dir)
    logging.basicConfig(filename=logging_dir + state.current_dt.strftime('%Y%m%d') + '.log', level=logging.INFO)
    state.log_prefix = f"{state.current_dt.strftime('%Y-%m-%d %H:%M:%S')} Z - "

    return

#-----------------------------------------------------

def parseArguments(raw_args, state: State):

    #--- arguments are in the form of a list of strings, i.e. ['-d', '20250611']
    parser = argparse.ArgumentParser(description='Process incoming data from /mnt/viirs/WI-CONUS/NPP for AWIPS ingestion')
    parser.add_argument('-b', '--orbit', type=str,
                        help='Ignore the time search and only process files for orbit b (can omit the leading b)')
    parser.add_argument('-f', '--freq-band', type=str,
                        help='Only process the files for the indicated band groupings (valid inputs are "i" and "m"')
    parser.add_argument('-s', '--satellite', type=str,
                        help='Only process the selected satellite ("NPP", "J01", and "J02")')
    parser.add_argument('-d', '--file-date', type=str,
                        help='Run for the full 24 hours of a specific date (YYYYMMDD format) or for a specific hour (YYYYMMDDhh format)')
    
    args = parser.parse_args(raw_args)

    #--- No arguments
    if len(sys.argv) == 1:
        logging.info(f"{state.log_prefix} Looking for data from {state.file_dt.strftime('%Y-%m-%d %H')}:00 UTC")

    #--- Specified orbit
    if args.orbit:
        state.orbits_to_process = [args.orbit if args.orbit[0] == 'b' else ('b' + args.orbit)]
        logging.info(f"{state.log_prefix} Running for orbit {state.orbits_to_process}")

    #--- Specified band
    if args.freq_band:
        if args.freq_band not in state.bands_to_process:
            logging.error('Invalid single-band input; valid options are "i" and "m"')
            exit(1)
        state.bands_to_process = [args.freq_band]

    #--- Specified satellite
    if args.satellite:
        if args.satellite not in state.sats_to_process:
            logging.error('Invalid satellite input; valid options are "NPP", "J01", and "J02"')
            exit(1)
        state.sats_to_process = [args.satellite]

    #--- Specified date
    if args.file_date:
        #--- Depending on if hour is specified or not
        if len(args.file_date) == 10:  #--- format: YYYYMMDDhh
            state.file_dt = datetime.strptime(args.file_date, "%Y%m%d%H")
            logging.info(f"{state.log_prefix} Looking for data from {state.file_dt.strftime('%Y-%m-%d %H')}:00 UTC")
        elif len(args.file_date) == 8:  #--- format: YYYYMMDD
            state.file_dt = datetime.strptime(args.file_date, "%Y%m%d")
            logging.info(f"{state.log_prefix} Looking for all data from {state.file_dt.strftime('%Y-%m-%d')}")
        else:
            raise ValueError("file_date must be in YYYYMMDD or YYYYMMDDhh format")
        
    
    return

#-----------------------------------------------------

def getOrbits(state: State):

    if not state.orbits_to_process: #--- initialize, if no argument for orbits
        state.orbits_to_process = []
    
    for sat in state.sats_to_process:    #--- NPP, J01, J02
        for band in state.bands_to_process:   #--- m, i
            
            #--- set band parameters
            band_dir = state.band_params[band]['band_dir'].replace('_replacewithsat_', sat)

            #--- get files that match time
            file_date_str = f"d{state.file_dt.year}{state.file_dt.month:02d}{state.file_dt.day:02d}_t{state.file_dt.hour:02d}"
            matching_files = [
                os.path.basename(f) for f in glob.glob(os.path.join(band_dir, f"*{file_date_str}*"))
                if file_date_str in f
            ]
            if not matching_files:
                continue

            #--- create orbits list from matching files
            for filename in matching_files:  
                orbit = filename.split('_')[5]
                if orbit not in state.orbits_to_process:
                    state.orbits_to_process.append(orbit)
    
    return

#-----------------------------------------------------

def createTempAndOutputDir(state: State):
    #--- creating a YYYYMMDD_hhmmss dir for an isolated workspace
    state.dtstamp_dir = f"{state.base_dir}/{state.current_dt.strftime('%Y%m%d%H%M%S')}/"
    if not os.path.exists(state.dtstamp_dir):
        os.makedirs(state.dtstamp_dir)

    #--- creating the output directories
    final_dir = state.base_dir + '/viirs_awips/'
    if not os.path.exists(final_dir):
        os.makedirs(final_dir)
    
    return

#-----------------------------------------------------

def setSatellitesAndBands(state: State):
    #--- list of products processed
    #------ (previous note) when you add/remove products, you need to update the ldm injection script on the LDM server (cira-ldm1)
    m_ldm_file_tags = {'m08': 'M08', 'm10': 'M10', 'm11': 'M11', 'm12': 'M12', 'm13': 'M13', 'm14': 'M14', 'm15': 'M15', 'm16': 'M16'}
    i_ldm_file_tags = {'i01': 'I01', 'i02': 'I02', 'i03': 'I03', 'i04': 'I04', 'i05': 'I05'}

    #--- set up dictionaries for each band
    state.band_params = {
        'm': {
            'band_dir': f"/mnt/viirs/WI-CONUS/_replacewithsat_/SDR-MBand/{state.file_dt.year}/{state.file_dt.timetuple().tm_yday}/",
            'prod_prefixes': ['GMTCO'] + ['SV' + tag for tag in list(m_ldm_file_tags.values())],
            'ldm_file_tags': m_ldm_file_tags,
            'output_prod_name': 'VIIRS'
        },
        'i': {
            'band_dir': f"/mnt/viirs/WI-CONUS/_replacewithsat_/SDR-IBand/{state.file_dt.year}/{state.file_dt.timetuple().tm_yday}/",
            'prod_prefixes': ['GITCO'] + ['SV' + tag for tag in list(i_ldm_file_tags.values())],
            'ldm_file_tags': i_ldm_file_tags,
            'output_prod_name': 'VIIRS'
        }
    }

    state.raw_sat_names = {'NPP': 'npp', 'J01': 'noaa20' , 'J02': 'noaa21'}

    return

#-----------------------------------------------------

def gettingFilesFromOrbit(state: State):

    state.filepaths = [] #--- Initialize

    for sat in state.sats_to_process:
        for band in state.bands_to_process:
            prod_prefixes = state.band_params[band]['prod_prefixes']

            for orbit in state.orbits_to_process:
                #--- create list of recent orbit files
                band_dir = state.band_params[band]['band_dir'].replace('_replacewithsat_', sat)
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
                        logging.info(f"{sat} orbit {orbit} bands are not fully filled in yet, not processing")
                        continue
                    
                    #--- logging the files used
                    state.filepaths = glob.glob(os.path.join(band_dir, prefix + '*_' + orbit + '_*'))
                    if not state.filepaths:
                        continue
                    else: 
                        state.filepaths.sort()  #--- sort to follow time order
                        first_file = os.path.basename(state.filepaths[0])
                        match = re.search(r'd(\d{8})_t(\d{2})(\d{2})', first_file)
                        if match:
                            date_str, hour, minute = match.groups()
                            dt = datetime.strptime(f"{date_str}{hour}{minute}", "%Y%m%d%H%M")
                            datetime_str = dt.strftime("%Y-%m-%d %H:%M UTC")
                        else:
                            datetime_str = "Unknown datetime"
                
    if state.filepaths:
        logging.info(f"Processing {len(state.filepaths)} VIIRS files for {sat} orbit {orbit} {band}-band at {datetime_str}")  
    
    return
    
#-----------------------------------------------------

def grabbingViirsFiles(state: State):

    for sat in state.sats_to_process:
        for band in state.bands_to_process:
            for orbit in state.orbits_to_process:

                state.processing_dir = state.dtstamp_dir + sat + '_' + band + '_' + orbit + '/'
                os.makedirs(state.processing_dir)
                state.raw_files_dir = state.processing_dir + 'raw_files/'
                os.makedirs(state.raw_files_dir)

                for filepath in state.filepaths:
                    shutil.copy(filepath, state.raw_files_dir) #--- copying the file
            
    return
    
#-----------------------------------------------------

def runningPolar2Grid(state: State):
    #--- running the polar2grid package
    #------ this is the very core of the processing

    for sat in state.sats_to_process:
        for band in state.bands_to_process:
            for orbit in state.orbits_to_process:
        
                print(state.log_prefix + 'Running p2g for ' + sat + ' ' + band + ' band ')
                p2g_status = subprocess.call(
                    ['bash', state.base_dir + '/call_p2g_' + band + '.sh', state.raw_files_dir], cwd=state.base_dir)
                print(state.log_prefix + 'Finished running p2g for ' + sat + ' (orbit ' + orbit + ') ' + band + ' band with exit status ' + str(
                    p2g_status))
                
    return
    
#-----------------------------------------------------

def checkForMissingData(state: State):
    
    for sat in state.sats_to_process:
        raw_sat_name = state.raw_sat_names[sat]
        
        for band in state.bands_to_process:
            for orbit in state.orbits_to_process:
    
                #--- check if channels are missing
                #------ there is definitely a simpler way of doing this
                
                ldm_file_tags = state.band_params[band]['ldm_file_tags']
                p2g_file_tags = list(ldm_file_tags.keys())

                state.missing_p2g_tags = []
                for p2g_tag in p2g_file_tags:
                    if len(glob.glob(state.processing_dir + 'SSEC_AII_' + raw_sat_name + '_viirs_' + p2g_tag + '*.nc')) == 0:
                        state.missing_p2g_tags.append(p2g_tag)
                #--- all files are missing tags, likely due to <10% grid coverage
                if len(state.missing_p2g_tags) == len(p2g_file_tags):
                    logging.info(f"P2G returned no files for {sat} orbit {orbit} {band}-band at {state.file_dt.strftime('%Y-%m-%d %H:%M UTC')}")

                #--- some files are missing tags, likely due to lack of sun
                elif len(state.missing_p2g_tags) > 0:
                    logging.info(f"P2G rejected {state.missing_p2g_tags} for {sat} orbit {orbit} {band}-band at {state.file_dt.strftime('%Y-%m-%d %H:%M UTC')}")
    
    return

#-----------------------------------------------------

def nameAndFillFiles(state: State):
    #--- for each file type, names properly and fills with gzip-compressed data
    
    for sat in state.sats_to_process:
        raw_sat_name = state.raw_sat_names[sat]
        
        for band in state.bands_to_process:
            ldm_file_tags = state.band_params[band]['ldm_file_tags']
            p2g_file_tags = list(ldm_file_tags.keys())
            output_prod_name = state.band_params[band]['output_prod_name']
            
            for orbit in state.orbits_to_process:

                file_count = 0
                for p2g_tag in p2g_file_tags:
                    for filepath in glob.glob(
                            state.processing_dir + 'SSEC_AII_' + raw_sat_name + '_viirs_' + p2g_tag + '*.nc'):
                        filename = os.path.basename(filepath)
                        filename_pieces = filename.split('_')

                        # polar2grid default is SSEC_AII_[raw_sat_name]_viirs_m##_LCC_Tttt_yyyymmdd_hhmm.nc
                        # desired output is RAMMB_ppppp_bbb_yyyymmdd_hhmm_ttt.nc.gz
                        # (ppppp = VIIRS, others later?), (bbb = band, e.g. M08, I03)
                        new_filename = ('RAMMB_' + output_prod_name + '_' + ldm_file_tags[p2g_tag] + '_' +
                                        filename_pieces[7] + '_' + filename_pieces[8][:-3] + '_' +
                                        filename_pieces[6][1:] + '.nc.gz')

                        if p2g_tag not in state.missing_p2g_tags:
                            with open(filepath, 'rb') as f_in, gzip.open(state.final_dir + new_filename, 'wb') as f_out:
                                f_out.writelines(f_in)
                            file_count += 1 #--- counting files created

                        #shutil.copy(copy_to_ldm_dir + new_filename, final_dir + new_filename)
                        os.remove(filepath) #--- remove files from processing directory

                        #--- logging files created for date
                        pattern = os.path.join(state.final_dir, f"*{state.file_dt.strftime('%Y%m%d')}*.nc.gz")
                        file_count_total = len(glob.glob(pattern))
                        logging.info(f"Created {file_count} AWIPS files. Total for {state.file_dt.strftime('%Y-%m-%d')} is now {file_count_total}.")
        
    return
    
#-----------------------------------------------------

def removeTempFiles(state: State):
    # Leave the directory behind if files we don't expect exist (e.g. we didn't finish copy .nc files out)
    num_h5_files = len(glob.glob(state.raw_files_dir + '/*.h5'))
    num_all_files = len(glob.glob(state.raw_files_dir + '/*'))
    if (num_all_files - num_h5_files) == 0:
        shutil.rmtree(state.raw_files_dir)

    if len(glob.glob(state.processing_dir)) == 1: #--- only log file is left
        shutil.rmtree(state.processing_dir)

    return

#-----------------------------------------------------

def finishAndClean(state: State):
    #--- if directory is empty, delete it
    if not os.listdir(state.dtstamp_dir):
        shutil.rmtree(state.dtstamp_dir)

    logging.info(state.log_prefix + 'Finished at ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    return

#=====================================================

if __name__ == '__main__':
    main()
