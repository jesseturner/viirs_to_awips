#!/bin/bash
# Originally this script was using scp - now using rsync - FINLEY - APR 2024
# NOTE: If you add or subtract products you will need to modify the ldm script
#       on the ldm server (cira-ldm1)

working_dir=/mnt/data1/jturner
output_dir=2_viirs_awips_format

/usr/bin/rsync -a $working_dir/$output_dir/*VIIRS_M*.nc.gz ldm@cira-ldm1:viirs_m_incoming 2> /dev/null
/usr/bin/rsync -a $working_dir/$output_dir/*VIIRS_I*.nc.gz ldm@cira-ldm1:viirs_i_incoming 2> /dev/null

/usr/bin/ssh ldm@cira-ldm1 "bash add_viirs_to_ldm.sh -b M"
/usr/bin/ssh ldm@cira-ldm1 "bash add_viirs_to_ldm.sh -b I"

/usr/bin/mv $working_dir/$output_dir/*.nc.gz $working_dir/3_to_ldm_recent/. 2> /dev/null
