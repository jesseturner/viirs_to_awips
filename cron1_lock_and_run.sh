#!/bin/bash

working_dir=/mnt/data1/jturner

cd $working_dir

today=$(date +%Y%m%d)
error_time=$(date +%H%M%S)Z

lock=$working_dir/lock-viirs-to-awips

if [ -e $lock ]
   then
   /bin/echo "lock exists - previous processing still running"
        if [ /bin/find $lock -mmin +20 ]
           then /bin/rm $lock
        fi
   exit 1
else
   /bin/touch $lock
fi

mkdir -p "$working_dir/processing_logs"

/usr/bin/env python3 $working_dir/cron1-1_process_viirs_to_awips.py
#/bin/bash $working_dir/cron1-2_move_files_to_ldm.sh
/bin/mv $working_dir/to_ldm/*.nc.gz $working_dir/to_ldm_recent/. 2> /dev/null

/bin/rm -rf $lock

exit 0

