#!/bin/bash

cd /home/jturner/VIIRS_to_AWIPS

today=$(date +%Y%m%d)
error_time=$(date +%H%M%S)Z

lock=/tmp/viirs-to-awips.svf

if [ -e $lock ]
   then
   /bin/echo "lock exists - previous processing still running - exiting!"
        if [ /bin/find $lock -mmin +20 ]
           then /bin/rm $lock
        fi
   exit 1
else
   /bin/touch $lock
fi

/bin/python /home/jturner/VIIRS_to_AWIPS/monitor_for_viirs_awips.py >> /home/jturner/VIIRS_to_AWIPS/logs/mfva_${today}.log 2>&1
/bin/bash /home/jturner/VIIRS_to_AWIPS/move_files_to_ldm.sh
/bin/mv /home/jturner/VIIRS_to_AWIPS/to_ldm/*.nc.gz /home/jturner/VIIRS_to_AWIPS/to_ldm_recent/. 2> /dev/null

/bin/rm -rf $lock

exit 0
