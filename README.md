# Processing VIIRS data to AWIPS format

Currently setup on `jturner@polarbear3:/mnt/data1/jturner`
* requires conda environment `viirs_to_awips`

Cron job runs every 15 minutes to collect any new data and produce imagery
* restart using: `*/15 * * * * /mnt/data1/jturner/run_v2a.sh >> /mnt/data1/jturner/v2a_logs/cron.log 2>&1`
* just activates the conda env and runs the main processing script: `v2a_processing.py`

Processing script (`v2a_processing.py`)
* Can be tested manually for a specific time using `v2a_processing_testing.py`
* Functions exist in `v2a_utils/`
* Directories `1_viirs_for_p2g/`, `2_viirs_awips_format/`, `3_to_ldm_recent/` are used for processing

Explanation of each function in processing: 
* `create_env()` and `create_logging()` initiates the processing environment and status logging dictionary
* `time_window_selector()` selects the most recent 24 minutes or a manual window of time (usually for testing)
* `get_files_by_mod_time()` gets the filenames of anything within the time window selected
* `copy_files_locally()` copies selected files to `1_viirs_for_p2g/`, tracking their associated satellite, orbit, and bands
* `run_p2g()` calls the polar2grid installed at `polarbear3:/local/`, this application was written by David Hoese at Univ of Wisconsin
* `name_and_move_files()` applies the naming convention expected by AWIPS and puts them in `2_viirs_awips_format/`
* `move_files_to_ldm()` sends files to `ldm@cira-ldm1` 
* `clean_up_to_ldm_recent()` moves processed files to `3_to_ldm_recent` and removes any files older than seven days
* `calc_total_run_time()` and `summarize_lists_for_pprint()` just outputs logging information