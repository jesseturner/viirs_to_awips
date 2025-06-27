#!/bin/bash

echo "=== Run at $(date) ==="

# Clean logging

MAXSIZE=1000000  # 1 MB maximum
for LOGFILE in /mnt/data1/jturner/cron_logs/cron{1,2,3,4}.log; do
    if [ -f "$LOGFILE" ] && [ $(stat -c%s "$LOGFILE") -ge $MAXSIZE ]; then
        echo "Log exceeded $MAXSIZE bytes. Truncating." > "$LOGFILE"
    fi
done

# Clean viirs_awips

working_dir=/mnt/data1/jturner

MAXAGE="7200" # maximum age in seconds - 7200 = 5 days
DIR="$working_dir/viirs_awips"

for f in `/bin/find $DIR -type f -mmin +$MAXAGE`
do
   /bin/rm -f "$f" > /dev/null 2>&1 &
done

exit 0