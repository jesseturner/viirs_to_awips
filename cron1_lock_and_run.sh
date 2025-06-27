#!/bin/bash

# Set up logging
mkdir -p /mnt/data1/jturner/cron_logs
echo "=== Run at $(date) ==="

# Set variables
working_dir=/mnt/data1/jturner
cd $working_dir

today=$(date +%Y%m%d)
error_time=$(date +%H%M%S)Z

# Set lock while running
lock="$working_dir/lock-viirs-to-awips"

if [ -e "$lock" ]; then
    echo "Lock file exists - checking age: $lock"
    
    if /bin/find "$lock" -mmin +20 | grep -q .; then
        echo "Lock file is stale. Removing: $lock"
        /bin/rm -f "$lock"
    else
        echo "Lock is still active. Exiting."
        exit 1
    fi
fi

/bin/touch "$lock"

# Main processing
/usr/bin/env python3 $working_dir/cron1-1_process_viirs_to_awips.py
/bin/bash $working_dir/cron1-2_move_files_to_ldm.sh
/bin/mv $working_dir/viirs_awips/*.nc.gz $working_dir/to_ldm_recent/. 2> /dev/null

/bin/rm -rf $lock

exit 0

