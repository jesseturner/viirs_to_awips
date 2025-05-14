Run the main code: `python3 monitor_for_viirs_awips.py`
- Only runs new files since last log, delete log if necessary
- If no log, runs files within last 55 minutes (change on line 24, originally 5 minutes)
- Puts results in `to_ldm` and `viirs_awips`
 