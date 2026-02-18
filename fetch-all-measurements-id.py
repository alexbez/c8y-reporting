# Fetching all measurements for the last month for given UPSs and storing them in CSV files,
# every measurement type in a separate file.
#
# Usage: python3 measurements.py <DEVICE_ID> <DATE_FROM> <DATE_TO>
#
# <DEVICE_ID> is the internal Cumulocity UPS ID, taken from Device Management app
# <DATE_FROM> and <DATE_TO> mudt be in YYYY-MM-DD format
#
# Note, that fetching measurements for 24 hours takes 1-2 minutes. 
# Fetching measurements for the whole month woud take more than one hour.
#
# Data is stored in files '<DEVICE_ID>_<MEASUREMENT_TYPE>_<DATE_FROM>_<DATE_TO>.csv'
#
# Cumulocity credentials are read from .env file in the current directorywith the following content:
#   C8Y_BASE_URL="<CUMULOCITY_BASE_URL>"
#   C8Y_TENANT_ID="<TENANT_ID>"
#   C8Y_USERNAME="<LOGIN>"
#   C8Y_PASSWORD="<PASSWORD>"
#
# DO NOT run this script from the computer connected via VPN, as the TLS connection would not be established!
#
# Copyright 2025 by Alexander Bezprozvanny, Eaton
#
from c8y_api import CumulocityApi
from dotenv import load_dotenv
from datetime import date, timedelta, datetime, timezone
import pandas as pd
import sys
import os

time_col = "TIME"
type_col = "TYPE"
value_col = "VALUE"
unit_col = "UNIT"
header = time_col + "," + type_col + "," + value_col + "," + unit_col + '\n' 


if len(sys.argv) != 4:
  print("\nUsage: python3 fetch-all-measurements-id.py <DEVICE_ID> <DATE_FROM> <DATE_TO>\n")
  exit(1)

id = sys.argv[1]
start_date = sys.argv[2]
end_date = sys.argv[3]

load_dotenv()   # load c8y credential from .env file

BASE_URL = os.getenv('C8Y_BASE_URL')
TENANT_ID = os.getenv('C8Y_TENANT_ID')
USERNAME = os.getenv('C8Y_USERNAME')
PASSWORD = os.getenv('C8Y_PASSWORD')

c8y = CumulocityApi(base_url=BASE_URL, tenant_id=TENANT_ID, username=USERNAME, password=PASSWORD)

if c8y == None:
    print("Cannot instantiate the Cumulocity API")
    quit()

outfn = f"{id}_{start_date}_{end_date}.csv"
measurements = {}
count = 0

print(f"Fetching measurements for the UPS #{id} from {start_date} until {end_date}. It takes about 1-2 minutes per day of measurements. Get a cup of coffee and be patient...")

data_measurements = c8y.measurements.get_all(source=id, after=start_date, before=end_date)
num_measurements = len(data_measurements)
print(f"{num_measurements} measurements fetched")
if num_measurements == 0:
  print("Nothing to proceed. Exiting.")
  exit(3)

of = open(outfn, "w")
of.write(header)
print(f"File '{outfn}' is created for the device #{id}")

count = 0
for measurement in data_measurements:
  count += 1
  mtype = measurement.type
  mtime = measurement.time
  (key,) = measurement[mtype].keys()
  mvalue = measurement[mtype][key]['value']
  munit = measurement[mtype][key]['unit']
  if not munit:
    munit="None"

  strarg = mtime + "," + key + "," + str( mvalue ) + "," + munit + '\n'
  of.write(strarg)
  of.flush()
of.close()    

print(f"Initial collection of data for UPS #{id} completed")
print(f"Structuring data, writing each type of measurements into its own CSV file...")

df = pd.read_csv(outfn)

for mtype, group in df.groupby(type_col):
  safe_mtype = str(mtype).strip().replace("/", "_")
  out_file = f"{id}_{safe_mtype}_{start_date}_{end_date}.csv"
  group[[time_col, value_col, unit_col]].to_csv(out_file, index=False)
  print(f"Output CSV file created: {out_file}")

print(f"Processing of UPS #{id} completed")
  


