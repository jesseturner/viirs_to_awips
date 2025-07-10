import cron1_1_process_viirs_to_awips as v2a
from pprint import pprint
from datetime import datetime, timedelta
import os

print("--------------- Testing ---------------")

base = v2a.BaseState()
v2a.setUpVariables(base)

#--- testing logging
base.base_dir = '/mnt/data1/jturner/TESTING'
v2a.startLogging(base)

#--- testing argument logic
test = []

v2a.parseArguments(['-b', 'b39596'], base)
test.append(base.orbits_to_process == ['b39596'] or print("FAIL at -b"))

v2a.parseArguments(['-f', 'i'], base)
test.append(base.bands_to_process == ['i'] or print("FAIL at -f"))

v2a.parseArguments(['-s', 'J01'], base)
test.append(base.sats_to_process == ['J01'] or print("FAIL at -s"))

v2a.parseArguments(['-d', '20250710'], base)
test.append(base.file_dt == datetime(2025, 7, 10, 0, 0, 0, 0) or print("FAIL at -d"))

v2a.parseArguments(['-d', '2025071006'], base)
test.append(base.file_dt == datetime(2025, 7, 10, 6, 0, 0, 0) or print("FAIL at -d"))

passed_test = all(test)
if passed_test: print("PASSED arg tests")
else: print("FAILED arg tests")

#--- testing make output directories
test = []

v2a.createTempAndOutputDir(base)
test.append(os.path.exists(f"/mnt/data1/jturner/TESTING/{base.current_dt.strftime('%Y%m%d%H%M%S')}") or print("FAIL at dtstamp_dir"))
test.append(os.path.exists(f"/mnt/data1/jturner/TESTING/viirs_awips") or print("FAIL at final_dir"))

passed_test = all(test)
if passed_test: print("PASSED output directory tests")
else: print("FAILED output directory tests")

#--- testing set satellites and bands
test = []

v2a.setSatellitesAndBands(base)

test.append(base.raw_sat_names != None or print("FAIL at raw_sat_names"))
test.append(base.band_params != None or print("FAIL at band_params"))

passed_test = all(test)
if passed_test: print("PASSED set satellites and bands")
else: print("FAILED set satellites and bands")

#--- testing get orbits
test = []
base.orbits_to_process = None #--- no set orbit
base.bands_to_process = ['m', 'i']
base.sats_to_process = ['NPP', 'J01', 'J02']

dt = datetime.now() - timedelta(days=1) #--- orbits for full day yesterday (should be 23)
dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
base.file_dt = dt
v2a.setSatellitesAndBands(base)
v2a.getOrbits(base)
test.append(len(base.orbits_to_process) == 23 or 
            print(f"WARNING at full day getOrbits, {len(base.orbits_to_process)} orbits"))

base.orbits_to_process = None
dt = datetime.now() - timedelta(days=1) #--- orbits yesterday at 6 UTC (should be 23)
dt = dt.replace(hour=6, minute=0, second=0, microsecond=0)
base.file_dt = dt
v2a.setSatellitesAndBands(base)
v2a.getOrbits(base)
test.append(len(base.orbits_to_process) == 3 or 
            print(f"WARNING at full day getOrbits, {len(base.orbits_to_process)} orbits"))

passed_test = all(test)
if passed_test: print("PASSED get orbits")
else: print("FAILED get orbits")


#pprint(base)
