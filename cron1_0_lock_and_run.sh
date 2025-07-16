#!/bin/bash

#--- Testing issue with finding /mnt/viirs/
echo "Checking mount: $(mount | grep /mnt/viirs)" >> /mnt/data1/jturner/cron_logs/cron1.log
ls /mnt/viirs > /dev/null 2>&1
echo "Checking mount: $(mount | grep /mnt/viirs)" >> /mnt/data1/jturner/cron_logs/cron1.log

echo "Before ls: $(findmnt /mnt/viirs -o TARGET -n 2>/dev/null)" >> /mnt/data1/jturner/cron_logs/cron1.log
ls /mnt/viirs > /dev/null 2>&1
echo "After ls: $(findmnt /mnt/viirs -o TARGET -n 2>/dev/null)" >> /mnt/data1/jturner/cron_logs/cron1.log

#--- Wait and retry for slow mnt
for i in {1..5}; do
    if [ -d /mnt/viirs ]; then
        echo "Mount found" >> /mnt/data1/jturner/cron_logs/cron1.log
        break
    else
        echo "Mount not found, retrying..." >>  /mnt/data1/jturner/cron_logs/cron1.log
        sleep 5
    fi
done

#------------------------------------------------

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
/usr/bin/python3.11 $working_dir/cron1_1_process_viirs_to_awips.py
/bin/bash $working_dir/cron1_2_move_files_to_ldm.sh
/bin/mv $working_dir/viirs_awips/*.nc.gz $working_dir/to_ldm_recent/. 2> /dev/null

/bin/rm -rf $lock

exit 0

