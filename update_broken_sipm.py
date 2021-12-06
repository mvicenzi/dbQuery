# This script is used to update SiPM entries in Scintillator table
# Author: mvicenzi
#
# Input: csv file with "crtbarcode,channel,newsipm,Vop"
# Output: csv file with update scintillator entry to upload to db
#
# TODO: add creation of sipm entry if new sipm is not in table
#
from DataLoader3 import DataLoader, DataQuery
import os, sys
import csv

#---------------------------------------------------------------#
# Main configuration parameters to query database
#---------------------------------------------------------------#
queryUrl = "https://dbdata0vm.fnal.gov:9443/QE/hw/app/SQ/query"
 #queryUrl = "https://dbdata0vm.fnal.gov:9090/hw/app/SQ/query"
dBname = "icarus_hardware_prd"
crtTable = "crtmodule"
scinTable = "scintillator"
sipmTable = "topcrt_sipm"
dataQuery = DataQuery(queryUrl)
#---------------------------------------------------------------#

# find scintillator barcode in crtTable corresponding to bar number
def find_scintillator(crt_barcode,bar_number):

    bar = "scin" + str(bar_number)
    scin_barcode = dataQuery.query(database=dBname, table=crtTable, columns=bar,where='crt_barcode:eq:'+crt_barcode)
    return scin_barcode[0]

# get full scintillator data from scintillator table
def get_scintillator_data(scin_barcode):

    scin = dataQuery.query(database=dBname, table=scinTable,
                           columns='scin_barcode,scin_prod,scin_sipm_r,scin_sipm_l,scin_test,light_y,attenu_t,attenu_l',
                           where='scin_barcode:eq:'+scin_barcode)
    return scin[0].split(",")

# check if sipm exists in sipm table
def is_sipm_in_table(serial_number):

    result = dataQuery.query(database=dBname, table=sipmTable, columns="*", where="serial_num:eq:"+str(serial_number))
    if len(result)>1:
        return True
    print(serial_number, "is not in topcrt_sipm table")
    return False

# convert channel to bar number
def get_bar_from_channel(channel):

    if (channel%2 == 0):
        return int(channel/2+1)
    elif (channel%2>0):
        return int(channel/2+0.5)

# update sipm serial number in scintillator entry
def update_sipm_in_scintillator(scintillator, channel, new_sipm):

    # even channel, right sipm
    if (channel%2 == 0):
        scintillator[2] = new_sipm
        return scintillator
    # odd channel, left sipm
    scintillator[3] = new_sipm
    return scintillator

#----------------------------------------------------------------#
def main():

    #open input file
    infile = open(sys.argv[1], "r")

    #Take the dir path of the input file
    pathfile = os.path.abspath(sys.argv[1])
    dirpath, filename = os.path.split(pathfile)
    outpath = os.path.join(dirpath,"updated_scin_entries.csv")

    # open output file
    outfile = open(outpath ,"w")

    reader = csv.reader(infile, delimiter=",", quoting=csv.QUOTE_MINIMAL)
    writer = csv.writer(outfile, quoting=csv.QUOTE_MINIMAL)

    # write header in output file
    first_row = "scin_barcode,scin_prod,scin_sipm_r,scin_sipm_l,scin_test,light_y,attenu_t,attenu_l"
    first_row = first_row.split(",")
    writer.writerow(first_row)

    # iterate over all lines in input file
    for row in reader:
        crt_barcode = row[0]
        channel = int(row[1])
        new_sipm = int(row[2])
        Vop = float(row[3])

        bar = get_bar_from_channel(channel)
        scin_barcode = find_scintillator(crt_barcode,bar)
        scintillator = get_scintillator_data(scin_barcode)

        if is_sipm_in_table(new_sipm):
            update_sipm_in_scintillator(scintillator, channel, new_sipm)
            writer.writerow(scintillator)
        else:
            #you should create a new csv to add to the sipm table
            pass 

if __name__ == "__main__":
    main()
