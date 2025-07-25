#!/bin/bash
# Originally this script was using scp - now using rsync - FINLEY - APR 2024
# NOTE: If you add or subtract products you will need to modify the ldm script
#       on the ldm server (cira-ldm1)

working_dir=/mnt/data1/jturner

/usr/bin/rsync -a $working_dir/viirs_awips/*VIIRS_M*.nc.gz ldm@cira-ldm1:viirs_m_incoming 2> /dev/null
/usr/bin/rsync -a $working_dir/viirs_awips/*VIIRS_I*.nc.gz ldm@cira-ldm1:viirs_i_incoming 2> /dev/null

/usr/bin/ssh ldm@cira-ldm1 "bash add_viirs_to_ldm.sh -b M"
/usr/bin/ssh ldm@cira-ldm1 "bash add_viirs_to_ldm.sh -b I"
