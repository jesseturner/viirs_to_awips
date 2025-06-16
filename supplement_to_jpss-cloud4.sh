#!/bin/bash

# After processing files locally, this will supplement any
# files we have that are missing from the jpss-cloud4 system.
# This works, but is slow and doesn't integrate well with the system over there. 

LOCAL_DIR="viirs_awips"
REMOTE_HOST="jpss-cloud4"
REMOTE_CHECK_DIR="/data2/mniznik/to_ldm_recent"
REMOTE_COPY_DIR="/data2/mniznik/to_ldm"

TMP_REMOTE_FILE_LIST=$(mktemp)

# Get the list of filenames from the remote machine
ssh "$REMOTE_HOST" "ls $REMOTE_CHECK_DIR" > "$TMP_REMOTE_FILE_LIST"

copied=0

for file in "$LOCAL_DIR"/*; do
    filename=$(basename "$file")

    if ! grep -Fxq "$filename" "$TMP_REMOTE_FILE_LIST"; then
        # File not found on remote, copy it
        scp "$file" "$REMOTE_HOST:$REMOTE_COPY_DIR" && ((copied++))
    fi
done

rm "$TMP_REMOTE_FILE_LIST"

echo "$copied file(s) copied to $REMOTE_HOST:$REMOTE_COPY_DIR"
