# Fetching all measurements for the last month for given UPSs and storing them in CSV files
# Usage: python3 measurements.py <DEVICE_ID> [<DEVICE_ID>...]
# Data is stored in files '<DEVICE_ID>-measurements.csv'
# Cumulocity credentials are read from .env file in the current directorywith the following content:
#   C8Y_BASEURL="<CUMULOCITY_BASE_URL>"
#   C8Y_TENANT="<TENANT_ID>"
#   C8Y_USER="<LOGIN>"
#   C8Y_PASSWORD="<PASSWORD>"
#
# DO NOT run this script from the computer connected via VPN, as the TLS connection would not be established!
#
# Copyright 2025 by Alexander Bezprozvanny, Eaton
#
from c8y_api import CumulocityApi
from dotenv import load_dotenv
from datetime import date, timedelta, datetime
import sys
import os


of_template = "-measurements.csv"

# --------------------------------------------------------------
def getIDs():
    result = set()
    if len(sys.argv) == 1:
        return result
    for i in range(1, len(sys.argv)):
        result.add(sys.argv[i])
    return result
# --------------------------------------------------------------

load_dotenv()   # load c8y credential from .env file

BASE_URL = os.getenv('C8Y_BASEURL')
TENANT_ID = os.getenv('C8Y_TENANT')
USERNAME = os.getenv('C8Y_USER')
PASSWORD = os.getenv('C8Y_PASSWORD')

c8y = CumulocityApi(base_url=BASE_URL, tenant_id=TENANT_ID, username=USERNAME, password=PASSWORD)

if c8y == None:
    print("Cannot instantiate the Cumulocity API")
    quit()


ids = getIDs()
if len(ids) == 0:
    print("No ID(s) provided. Quit.")
    quit()

print("Fetching measurements for given UPSs and storing them in CSV files")
print(f"This is done for IDs: {ids}")


end_date = date.today()
start_date = end_date - timedelta(days=1)

for id in ids:
    ofname = id + of_template
    of = open(ofname, "w")
    print(f"\nFile '{ofname} is open for storing measurements for device #{id} from {start_date} to {end_date}")
    of.write("TIME, TYPE, VALUE, UNIT")
    measurements = {}
    count = 0
   
    data_measurements = c8y.measurements.get_all(source=id, after=start_date, before=end_date)
    print(f"{len(data_measurements)} measurements fetched")

    count = 0
    for measurement in data_measurements:
        count += 1
        mtype = measurement.type
        mtime = measurement.time
        (key,) = measurement[mtype].keys()
        strarg = mtime + ", " + key + ", " + str( measurement[mtype][key]['value'] ) + ", " + measurement[mtype][key]['unit'] + '\n'
        of.write(strarg)
    of.flush()
    of.close()
    print(f"Measurements stored, output file '{ofname}' closed")
    print()
    print("==================================================================================================")
    print()

print(f"Completed")