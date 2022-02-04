# This script returs the lenght of the timing cable from
# the timing distribution box to the CRT module
#
# Author: mvicenzi
#
# Inputs: crtbarcode
# Output: total cable length

from DataLoader3 import DataLoader, DataQuery
import os, sys
import csv

#---------------------------------------------------------------#
# Main configuration parameters to query database
#---------------------------------------------------------------#
queryUrl = "https://dbdata0vm.fnal.gov:9443/QE/hw/app/SQ/query"
dBname = "icarus_hardware_prd"
crtTable = "crtmodule"
cableTable = "crtcable"
dataQuery = DataQuery(queryUrl)
#---------------------------------------------------------------#
# RATED DELAY for timing cables
rated_delay = 5.06 # ns/m
#---------------------------------------------------------------#

def usage():
    print ("Missing required parameters!")
    print ("python get_cable_length.py [crt_number] [cable_type]")

# find feb barcode from crt barcode
def find_feb(crt_barcode):

    feb_barcode = dataQuery.query(database=dBname, table=crtTable, columns="feb_barcode",where='crt_barcode:eq:'+crt_barcode)
    return feb_barcode[0]

# find cable id of given timing_type arriving at FEB
def find_cable(feb_barcode, timing_type):

    cable_id = dataQuery.query(database=dBname, table=cableTable, columns="cable_id",
                                where='end_feb:eq:'+feb_barcode+'&cable_function:eq:'+timing_type)
    #print(cable_id)
    return cable_id[0]

# get legth of cable from cable_id
def get_cable_length(cable_id):

    length = dataQuery.query(database=dBname, table=cableTable, columns="cable_length", where="cable_id:eq:"+cable_id)
    #print(cable_id, length)
    return float(length[0])

# compute length of daisy chain
def compute_length_up_to(cable_id):

    line, type, pos = cable_id.split("-");
    total_length = 0
    for i in range(1,int(pos)+1):
        cable = line + "-" + type + "-" + str(i).zfill(2)
        total_length += get_cable_length(cable) #in cm

    return total_length/100. #in meters

# convert length into delay
def compute_total_delay(length):
    return format(rated_delay*length, '.2f') #in ns


#----------------------------------------------------------------#
def main():

    args = sys.argv
    if (len(args) != 3):
        usage()
        sys.exit()

    crt_barcode = "CRT" + str(args[1]).zfill(5)
    timing_type = args[2]

    print(crt_barcode, timing_type)

    feb_barcode = find_feb(crt_barcode)

    cable_id = find_cable(feb_barcode, timing_type)
    length = compute_length_up_to(cable_id)
    delay = compute_total_delay(length)

    print("Rated delay: " + str(rated_delay) + " ns/m")
    print("Total length is " + str(length) + " m")
    print("Total delay is " + str(delay) + " ns")

if __name__ == "__main__":
    main()
