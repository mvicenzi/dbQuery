# This script is used to create a list of all the SiPMs that were
# damaged during shipping and replaced at FNAL
#
# Author: mvicenzi
#
# Inputs: csv file with "crtbarcode,channel,newsipm,Vop" + backup scintillator table
# Output: csv file with entries of broken SiPM
#

from DataLoader3 import DataLoader, DataQuery
import os, sys
import csv

#---------------------------------------------------------------#
# Main configuration parameters to query database
#---------------------------------------------------------------#
queryUrl = "https://dbdata0vm.fnal.gov:9443/QE/hw/app/SQ/query"
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

# get old sipm information from backup table
def get_sipm_serialnum(scin_barcode, channel, backup_data):

    serial_num = 0
    for item in backup_data:
        #print(row[0])
        if item[0] == scin_barcode:
                if (channel%2==0): # even channel is R
                    serial_num = item[2]
                    break
                else:              # odd channel is L
                    serial_num = item[3]
                    break
    return serial_num

# convert channel to bar number
def get_bar_from_channel(channel):

    if (channel%2 == 0):
        return int(channel/2+1)
    elif (channel%2>0):
        return int(channel/2+0.5)

# prepare a new sipm entry
def get_sipm_info(serial_number):

    sipm = dataQuery.query(database=dBname, table=sipmTable, columns="serial_num,voltage,dark_curr,comment",
                           where="serial_num:eq:"+str(serial_number))
    return sipm[0].split(",")

# get backup data (not to read too many times the file)
def extract_backup_data(backupfile):

    data = []
    backup_reader = csv.reader(backupfile, delimiter=",", quoting=csv.QUOTE_MINIMAL)
    for row in backup_reader:
        data.append(row[:4])
    return data

#----------------------------------------------------------------#
def main():

    #open input file
    infile = open(sys.argv[1], "r")
    backupfile = open(sys.argv[2],"r")

    #Take the dir path of the input file
    pathfile = os.path.abspath(sys.argv[1])
    dirpath, filename = os.path.split(pathfile)
    outpath = os.path.join(dirpath,"broken_sipms.csv")

    # open output file
    outfile = open(outpath ,"w")

    reader = csv.reader(infile, delimiter=",", quoting=csv.QUOTE_MINIMAL)
    writer = csv.writer(outfile, quoting=csv.QUOTE_MINIMAL)

    # write header in output files
    first_sipm_row = "serial_num,voltage,dark_curr,comment"
    first_sipm_row = first_sipm_row.split(",")
    writer.writerow(first_sipm_row)

    backup_data = extract_backup_data(backupfile)

    # iterate over all lines in input file
    for row in reader:
        crt_barcode = row[0]
        channel = int(row[1])

        bar = get_bar_from_channel(channel)
        scin_barcode = find_scintillator(crt_barcode,bar)
        broken_serial_num = get_sipm_serialnum(scin_barcode, channel, backup_data)

        broken_sipm = get_sipm_info(broken_serial_num)
        writer.writerow(broken_sipm)

if __name__ == "__main__":
    main()
