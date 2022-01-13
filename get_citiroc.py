# This script write the Citiroc configuration file for a given FEB,
# using SiPM info from ICARUS hardware database
#
# Author: Umut Kose
# (converted into python by mvicenzi)
#
# Inputs: feb_barcode
# Output: config file

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
febTable = "crtfeb"
sipmTable = "topcrt_sipm"
dataQuery = DataQuery(queryUrl)
#---------------------------------------------------------------#

# returns MAC address and HV voltage of FEB
def find_feb_info(feb_barcode):

    feb_data = dataQuery.query(database=dBname, table=febTable, columns="mac_add,voltage",where='feb_barcode:eq:'+feb_barcode)
    return feb_data[0].split(",")

# get channel from scintillator bar + position
def get_channel_from_bar(scin_bar, position):

    left_channel = scin_bar*2-1;
    right_channel = left_channel-1;
    if(position == "L"):
        return left_channel
    elif(position == "R"):
        return right_channel

# returns dictionary with SiPM operating voltage per channel
def find_sipm_info(feb_barcode):

    sipm_channels = {}
    for i in range(1, 17):

        # get scintillator
        scin_column = "scin"+str(i)
        scin_bar = dataQuery.query(database=dBname, table=crtTable, columns=scin_column,where='feb_barcode:eq:'+feb_barcode)
        scin_barcode = scin_bar[0]

        # get sipms
        sipms = dataQuery.query(database=dBname, table=scinTable, columns="scin_sipm_r,scin_sipm_l", where='scin_barcode:eq:'+scin_barcode)
        sipm_r, sipm_l = sipms[0].split(",")

        # get sipm operating voltage
        voltage_r = dataQuery.query(database=dBname, table=sipmTable, columns="voltage", where='serial_num:eq:'+sipm_r)
        voltage_r = voltage_r[0]
        voltage_l = dataQuery.query(database=dBname, table=sipmTable, columns="voltage", where='serial_num:eq:'+sipm_l)
        voltage_l = voltage_l[0]

        #save in dictionary
        channel_r = get_channel_from_bar(i,"R")
        channel_l = get_channel_from_bar(i,"L")
        sipm_channels[channel_r] = voltage_r
        sipm_channels[channel_l] = voltage_l

    return sipm_channels

#----------------------------------------------------------------#
def main():

    feb = 1;
    feb_barcode = "FEB"+str(feb).zfill(5)
    feb_mac, feb_voltage = find_feb_info(feb_barcode)

    print ("-------------------------------------------------------")
    print ("Creating CITIROC configuration file for " + feb_barcode)
    print ("MAC address: " + str(feb_mac) + "   HV: " + str(feb_voltage) + " V")
    print ("-------------------------------------------------------\n")

    channel_voltages = find_sipm_info(feb_barcode)

    print("The following info will be used to create CITIROC configuration file\n")
    for channel, voltage in channel_voltages.items():
        print("Ch "+ str(channel) + " : " + str(voltage) + " V")


    #timing_type = "T0"  #or T1
    #feb_barcode = find_feb_info(feb_barcode)


    #print("Total length is " + str(length) + " mm")

if __name__ == "__main__":
    main()
