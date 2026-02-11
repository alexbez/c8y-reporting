# Usage: python3 bad-contracts-per-country.py [Country]
# if Country parameter is not present, then the report is global
# If Country parameter is present, then the report is done only for selected country
#
# Copyright 2024 by Alexander Bezprozvanny
# Apache license 2.0
#
from c8y_api import CumulocityApi
from collections import OrderedDict
from datetime import date, timedelta, datetime
from dotenv import load_dotenv
import sys
import os

# =============================================================================
def getCountries():
  set_countries = set()
  if len(sys.argv) == 1:
    return set_countries
  for i in range(1, len(sys.argv)):
    set_countries.add(sys.argv[i])
  return set_countries
# =============================================================================
def getDate(strobj):
  if strobj == '' or strobj == None:
    return None
  if type(strobj) == str:
    if '-' in strobj:
      return datetime.strptime(strobj[:10], '%Y-%m-%d')
    else:
      return datetime.fromtimestamp(int(strobj)/1000)
    
  if type(strobj) == int:
    return datetime.fromtimestamp(strobj/1000)
  
  return None 
# =============================================================================
def getReason(end_date):
  now = datetime.now()
  if end_date == None or end_date == '':
    return 'End date is empty'
  if end_date - now < timedelta(0):
    return 'End date has expired'
  else:
    return None
# =============================================================================  

load_dotenv()   # load c8y credential from .env file

BASE_URL = os.getenv('C8Y_BASE_URL')
TENANT_ID = os.getenv('C8Y_TENANT_ID')
USERNAME = os.getenv('C8Y_USERNAME')
PASSWORD = os.getenv('C8Y_PASSWORD')

c8y = CumulocityApi(base_url=BASE_URL, tenant_id=TENANT_ID, username=USERNAME, password=PASSWORD)

if c8y == None:
    print("Cannot instantiate the Cumulocity API")
    quit()

print(f"Report on units with missing or incorrect metadata\n")
num_bad_contracts = 0
bad_ups = {}
countries = {}
ser_nums = {}  # {serial_number, reason}

set_countries = getCountries()
if len(set_countries) == 0:
  print("The report is NOT filtered by country")
  filtered = False
else:
  print("The report is filtered by:")
  print(set_countries)
  filtered = True

for device in c8y.device_inventory.select():
  if not device.type == 'UPS_Device':
    continue
  bad_flag = False
  problems = {}
  id = device.id

  try:
    country = device.fragments['Country']
    if len(country) == 0:
      problems['Country'] = "Empty"
      bad_flag = True
    else:
      problems['Country'] = ""
  except KeyError:
    country = '<No country>'
    problems['Country'] = "Not defined"
    bad_flag = True

  if filtered and country not in set_countries:
    continue

  try:
    company = device.fragments['CompanyName']
    if len(company) == 0:
      problems['Customer'] = "Empty"
      bad_flag = True
    else:
      problems['Customer'] = ""
  except KeyError:
    company = '<No customer>'
    problems['Customer'] = "Not defined"
    bad_flag = True

  try:
    serial_number = device.fragments['c8y_Hardware']['serialNumber']
    if len(serial_number) == 0:
      problems['Serial Number'] = "Empty"
      bad_flag = True
    else:
      problems['Serial Number'] = ""
  except KeyError:
    serial_number = "<No S/N>"
    problems['Serial Number'] = "Not defined"
    bad_flag = True

  try:
    location = device['Location']
    if len(location) == 0:
      problems['Location'] = "Empty"
      bad_flag = True
    else:
      problems['Location'] = ""
  except KeyError:
    location = "<No location>"
    problems['Location'] = "Not defined"
    bad_flag = True

  try:
    email = device['CompanyEmail']
    if len(email) == 0:
      problems['Company Email'] = "Empty"
      bad_flag = True
    else:
      problems['Company Email'] = ""
  except KeyError:
    email = "<No email>"
    problems['Company Email'] = "Not defined"
    bad_flag = True

  try:
    monitoring_start = device['MonitoringStartDate']
    problems['Monitoring Start'] = ""
  except KeyError:
    monitoring_start = ''
    problems['Monitoring Start'] = "Not defined"
    bad_flag = True

  try:
    monitoring_end = device['MonitoringEndDate']
    problems['Monitoring End'] = ""
  except KeyError:
    monitoring_end = ''
    problems['Monitoring End'] = "Not defined"
    bad_flag = True

  monitoring_start_date = getDate(monitoring_start)
  monitoring_end_date = getDate(monitoring_end)

  reason = getReason(monitoring_end_date)
  if reason != None:
    problems['Monitoring End'] = reason
    bad_flag = True

  if bad_flag:
    num_bad_contracts += 1
    #print(f"#{id} Country: '{country}', S/N: '{serial_number}', Location: '{location}', Customer: '{company}',  Email: '{email}', Warranty from {warranty_start_date} to {warranty_end_date}, Monitoring from {monitoring_start} to {monitoring_end}, Problems: {problems}")
    #print(" ")

    # adding new bad ups to the dictionary
    if country not in bad_ups:
      bad_ups[country] = {}
    customers = bad_ups[country]
    if company not in customers:
      bad_ups[country][company] ={}

    bad_ups[country][company][serial_number] = problems

print(f"Total number of bad UPSs: {num_bad_contracts}")

#bad_ups_sorted = OrderedDict(sorted(bad_ups.items()))
bad_ups_sorted = dict(sorted(bad_ups.items()))

for country in bad_ups_sorted:
  num_bad_contracts = 0
  print("----------------------------------------------------------------------------")
  print(f"Country: '{country}'")
  for company in bad_ups_sorted[country]:
    print(f"    Customer: '{company}'")
    for ser_num in bad_ups_sorted[country][company]:
      print(f"        S/N: {ser_num:<18}")
      problems = bad_ups_sorted[country][company][ser_num]
      for key in problems.keys():
        if len(problems[key]) > 0:
          print(f"          {key:<24} {problems[key]}")
      num_bad_contracts += 1
  print(f"Number of bad UPSs per country: {num_bad_contracts}")
  print("  ")
print(f"Report completed on {datetime.now()}")
print(f"(c) Eaton 2024")