#!/bin/bash

# maximum age in seconds - 7200 = 5 days

working_dir=/mnt/data1/jturner

MAXAGE="7200"
DIR="$working_dir/viirs_awips"

for f in `/bin/find $DIR -type f -mmin +$MAXAGE`
do
   /bin/rm -f "$f" > /dev/null 2>&1 &
done

exit 0