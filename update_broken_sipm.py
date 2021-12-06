# This script is used to update SiPM entries in Scintillator table
# Author: mvicenzi
#
# TODO LIST:
# take as input csv file with list of "crtbarcode,channel,newsipm"
# write to updated scintillator rows to output (or use directly DataLoader??)

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
    print(scin_barcode)
    return scin_barcode[0]

# get full scintillator data from scintillator table
def get_scintillator_data(scin_barcode):

    scin = dataQuery.query(database=dBname, table=scinTable, columns='*', where='scin_barcode:eq:'+scin_barcode)
    return scin[0].split(",")

# check if sipm exists in sipm table
def is_sipm_in_table(serial_number):

    result = dataQuery.query(database=dBname, table=sipmTable, columns="*", where="serial_num:eq:"+str(serial_number))
    if len(result)>1:
        return True
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

    #test example
    crt_barcode = "CRT00125"
    channel = 1
    new_sipm = 14324

    bar = get_bar_from_channel(channel)
    scin_barcode = find_scintillator("CRT00125",bar)
    scintillator = get_scintillator_data(scin_barcode)

    print(scintillator)

    if is_sipm_in_table(new_sipm):
        update_sipm_in_scintillator(scintillator, channel, new_sipm)
    print(scintillator)

if __name__ == "__main__":
    main()
