#!/bin/bash

#--- Set up Python environment
#------ Cron job had issues with these not manually set
export LANG=en_US.UTF-8
export PATH=/usr/bin:/bin:$PATH
export ORGANIZATION="CIRA"

#--- Set up logging
mkdir -p /mnt/data1/jturner/cron_logs
echo "=== Run at $(date) ==="

#--- Set variables
working_dir=/mnt/data1/jturner
cd $working_dir

#--- Set lock while running
lock="$working_dir/lock-viirs-to-awips"

if [ -e "$lock" ]; then
    echo "Lock file exists - checking age: $lock"
    
    if /bin/find "$lock" -mmin +60 | grep -q .; then
        echo "Lock file is stale. Removing: $lock"
        /bin/rm -f "$lock"
    else
        echo "Lock is still active. Exiting."
        exit 1
    fi
fi

/bin/touch "$lock"

#--- Main processing
#/usr/bin/python3.11 $working_dir/p2g_processing_utils_testing.py
/usr/bin/python3.11 $working_dir/cron1_1_p2g_processing.py
/bin/bash $working_dir/cron1_2_move_files_to_ldm.sh
/bin/mv $working_dir/viirs_awips/*.nc.gz $working_dir/to_ldm_recent/. 2> /dev/null

/bin/rm -rf $lock

exit 0

