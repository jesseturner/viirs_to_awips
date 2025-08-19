#!/bin/bash

# Default is no date filter
# Date argument is in the format of `-d YYYYMMDD_hh`
date_filter=""

while getopts "d:" opt; do
  case ${opt} in
    d)
      date_filter="${OPTARG}"
      ;;
    *)
      echo "Usage: $0 [-d yyyymmdd_hh]" >&2
      exit 1
      ;;
  esac
done

for band in I01 I02 I03 I04 I05 M08 M10 M11 M12 M13 M14 M15 M16; do
    count=$(find to_ldm_recent/ -maxdepth 1 -type f -name "*${band}*${date_filter}*" | wc -l)
    echo "$band: $count"
done