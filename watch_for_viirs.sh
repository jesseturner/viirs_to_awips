#!/bin/bash

#--- Inotifywait is not installed. Either install it or switch to watchdog. 

#--- Run with: 
#--- nohup ./watch_for_viirs.sh >> /mnt/data1/jturner/watch_for_viirs.log 2>&1 &
#--- pkill -f watch_for_viirs.sh


YEAR=$(date +%Y)
JULIAN_DAY=$(date +%j)

WATCH_DIR="/mnt/jpssnas9/WI-CONUS/J01/SDR-IBand/${YEAR}/${JULIAN_DAY}"

if [ ! -d "$WATCH_DIR" ]; then
    echo "Watch directory does not exist: $WATCH_DIR"
    exit 1
fi

echo "[$(date)] Watching: $WATCH_DIR"

# Watch recursively (-r), output full file path (%w%f)
inotifywait -m -r -e create --format '%w%f' "$WATCH_DIR" | while read FILE
do
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] New file detected: $FILE"
    
    # Optional: run a script when specific files arrive
done