The main processing runs every 9 minutes (cron1): 
- checks for new viirs files in the last 12 minutes
- processes viirs files and uses Polar2Grid to output AWIPS files
- moves to LDM

Every 18 minutes (cron2):
- remove anything from `viirs_awips` that is over 2 hours old

Every 36 minutes (cron3): 
- email if there is a data gap longer than 10 hours

Every day (cron4): 
- clean up `to_ldm_recent` so we only retain the files from the last 7 days

