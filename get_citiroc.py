# This script writes the CITIROC configuration file for a given FEB,
# using SiPM info from ICARUS hardware database
#
# Author: Umut Kose (converted in Python by mvicenzi)
#
# Inputs: feb_number,dac,dac_tune,HGPreamp,LGPreamp,Amplifier,OR32,enabled_chs
# Output: config file

from DataLoader3 import DataLoader, DataQuery
import sys

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
#---------------------------------------------------------------#

def usage():
    print("Missing required parameters:")
    print("get_citiroc.py [feb_number] [dac] [dac_tune] [HGPreamp] [LGPreamp] [OR32] [Amplifier] [ChA ChB ..]")
    print("       'feb_number' - FEB number, eg. 1")
    print("              'dac' - DAC [default: 230]")
    print("         'dac_tune' - Adding to SiPM overvoltage in mV [default: 0]")
    print("         'HGPreamp' - High-Gain Preamplifier [default: 51]")
    print("         'LGPreamp' - Low-Gain Preamplifier [default: 51]")
    print("             'OR32' - enabled/disabled [default: disabled]")
    print("        'Amplifier' - allenabled/disabled/partenabled [default: allenabled]")
    print("       'ChA ChB ..' - Enable individual channels, if Amplifier is partenabled")

#-----------------------------------------------------------------#
#-----------------------------------------------------------------#

# returns MAC address and HV voltage of FEB
def find_feb_info(feb_barcode):

    feb_data = dataQuery.query(database=dBname, table=febTable, columns="mac_add,voltage",where='feb_barcode:eq:'+feb_barcode)
    return feb_data[0].split(",")

# get channel from scintillator bar + position
def get_channel_from_bar(scin_bar, position):

    left_channel = scin_bar*2-1
    right_channel = left_channel-1
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

#-----------------------------------------------------------------#
#-----------------------------------------------------------------#

# returns a 32-bit string in case of partially enabled channel
def set_partially_enabled(enable_string, enabled_channels):

    string_list = list(enable_string)
    for channel in enabled_channels:
        string_list[channel] = "1"

    return "".join(string_list)

# compute the gain bias according to desired overvoltage
def compute_gain_bias(feb_voltage, sipm_voltage, dac_tune):

    overvoltage = 256 - int((float(feb_voltage) - float(sipm_voltage))/0.018 - dac_tune/18)
    gain_bias = format(overvoltage,"b").zfill(8)
    return gain_bias

#----------------------------------------------------------------#
#----------------------------------------------------------------#
def main():

    # parse input parameters
    inputs = sys.argv

    if(len(inputs) < 8):
        usage()
        sys.exit()

    feb = int(inputs[1])
    dac = int(inputs[2])
    dac_tune = float(inputs[3])
    hgpreamp = int(inputs[4])
    lgpreamp = int(inputs[5])

    or32 = inputs[6]
    if or32 not in ["enabled","disabled"]:
        usage()
        sys.exit()

    preamp = inputs[7]
    if preamp not in ["allenabled","disabled","partenabled"]:
        usage()
        sys.exit()

    enabled_channels = []
    if(len(inputs) > 8):
        for input in inputs[8:]:
            enabled_channels.append(int(input))


    feb_barcode = "FEB"+str(feb).zfill(5)
    feb_mac, feb_voltage = find_feb_info(feb_barcode)

    # set filename according to options
    filename = "CITIROC_SC_SN" + str(feb_mac).zfill(3)
    if ( or32 == "enabled"):
        filename += "_or32.txt"
    elif ( preamp == "partenabled"):
        filename += "_parpream.txt"
    else:
        filename += ".txt"

    print ("-------------------------------------------------------")
    print ("Creating CITIROC configuration file for " + feb_barcode)
    print ("MAC address: " + str(feb_mac) + "   HV: " + str(feb_voltage) + " V")
    print ("-------------------------------------------------------\n")

    print("The following options will be used to create CITIROC configuration file")
    print("OR32: " + or32)
    print("Amplifier: " + preamp)
    print("DAC: " + str(dac))
    print("Tune SiPM voltage: " + str(dac_tune))
    print("HG Preamp: " + str(hgpreamp))
    print("LG Preamp: " + str(lgpreamp)+"\n")

    if( preamp == "partenabled" and len(enabled_channels)<1 ):
        print("WARNING: 'Partially Enabled Amplifier' selected, but list of enabled channels is empty")
        usage()
        sys.exit()

    if( len(enabled_channels) > 0 ):
        for channel in enabled_channels:
            print( "Ch-" + str(channel).zfill(2) + " enabled")
        print("\n")

    print("Channel information from DB:")
    channel_voltages = find_sipm_info(feb_barcode)
    for channel, voltage in channel_voltages.items():
        print("Ch "+ str(channel).zfill(2) + " : " + str(voltage) + " V")

    #---------------------------------------------------------------#
    file = open(filename,"w")

    for channel in channel_voltages.keys():
        file.write("0000 ' Ch" + str(channel) +" 4-bit DAC_t ([0..3])'\n")
    for channel in channel_voltages.keys():
        file.write("0000 ' Ch" + str(channel) +" 4-bit DAC ([0..3])'\n")

    file.write("1 'Enable discriminator'\n")
    file.write("1 ' Disable trigger discriminator power pulsing mode (force ON)'\n")
    file.write("0 ' Select latched (RS : 1) or direct output (trigger : 0)'\n")
    file.write("1 ' Enable Discriminator Two'\n")
    file.write("1 ' Disable trigger discriminator power pulsing mode (force ON)'\n")
    file.write("1 ' EN_4b_dac'\n")
    file.write("1 'PP: 4b_dac'\n")
    file.write("1 ' EN_4b_dac_t'\n")
    file.write("1 'PP: 4b_dac_t'\n")

    enable_string = "00000000000000000000000000000000"
    if ( preamp == "partenabled"):
        enable_string = set_partially_enabled(enable_string, enabled_channels)

    file.write(enable_string + " ' Allows to Mask Discriminator (channel 0 to 31) [active low]'\n")
    file.write("1 ' Disable High Gain Track & Hold power pulsing mode (force ON)'\n")
    file.write("1 ' Enable High Gain Track & Hold'\n")
    file.write("1 ' Disable Low Gain Track & Hold power pulsing mode (force ON)'\n")
    file.write("1 ' Enable Low Gain Track & Hold'\n")
    file.write("0 ' SCA bias ( 1 = weak bias, 0 = high bias 5MHz ReadOut Speed)'\n")
    file.write("0 ' PP: HG Pdet'\n")
    file.write("0 ' EN_HG_Pdet'\n")
    file.write("0 ' PP: LG Pdet'\n")
    file.write("0 ' EN_LG_Pdet'\n")
    file.write("1 ' Sel SCA or PeakD HG'\n")
    file.write("1 ' Sel SCA or PeakD LG'\n")
    file.write("1 ' Bypass Peak Sensing Cell'\n")
    file.write("0 ' Sel Trig Ext PSC'\n")
    file.write("1 ' Disable fast shaper follower power pulsing mode (force ON)'\n")
    file.write("1 ' Enable fast shaper'\n")
    file.write("1 ' Disable fast shaper power pulsing mode (force ON)'\n")
    file.write("1 ' Disable low gain slow shaper power pulsing mode (force ON)'\n")
    file.write("1 ' Enable Low Gain Slow Shaper'\n")
    file.write("110 ' Low gain shaper time constant commands (0..2)  [active low] 100'\n")
    file.write("1 ' Disable high gain slow shaper power pulsing mode (force ON)'\n")
    file.write("1 ' Enable high gain Slow Shaper'\n")
    file.write("110 ' High gain shaper time constant commands (0..2)  [active low] 100'\n")
    file.write("0 ' Low Gain PreAmp bias ( 1 = weak bias, 0 = normal bias)'\n")
    file.write("1 ' Disable High Gain preamp power pulsing mode (force ON)'\n")
    file.write("1 ' Enable High Gain preamp'\n")
    file.write("1 ' Disable Low Gain preamp power pulsing mode (force ON)'\n")
    file.write("1 ' Enable Low Gain preamp'\n")
    file.write("0 ' Select LG PA to send to Fast Shaper'\n")
    file.write("1 ' Enable 32 input 8-bit DACs'\n")
    file.write("1 ' 8-bit input DAC Voltage Reference (1 = external 4,5V , 0 = internal 2,5V)'\n")

    for channel, voltage in channel_voltages.items():
        gain_bias = compute_gain_bias(feb_voltage, voltage, dac_tune)
        file.write(gain_bias + " 1 ' Input 8-bit DAC Data channel " + str(channel) + " - (DAC7...DAC0 + DAC ON), higher-higher bias'\n")

    preampHG = format(hgpreamp,"b").zfill(4)
    preampLG = format(lgpreamp,"b").zfill(4)
    for channel in channel_voltages.keys():
        enable_amplifier = format(1,"b").zfill(3) #all are disabled
        if ( preamp == "allenabled"):
            enable_amplifier = format(0,"b").zfill(3)
        elif ( preamp == "partenabled" and (channel in enabled_channels) ):
            enable_amplifier = format(0,"b").zfill(3)

        file.write(preampHG+" "+preampLG+" "+enable_amplifier+" ' Ch"+str(channel)+"   PreAmp config (HG gain[5..0], LG gain [5..0], CtestHG, CtestLG, PA disabled)'\n")

    file.write("1 ' Disable Temperature Sensor power pulsing mode (force ON)'\n")
    file.write("1 ' Enable Temperature Sensor'\n")
    file.write("1 ' Disable BandGap power pulsing mode (force ON)'\n")
    file.write("1 ' Enable BandGap'\n")
    file.write("1 ' Enable DAC1'\n")
    file.write("1 ' Disable DAC1 power pulsing mode (force ON)'\n")
    file.write("1 ' Enable DAC2'\n")
    file.write("1 ' Disable DAC2 power pulsing mode (force ON)'\n")

    dac1 = format(dac,"b").zfill(10)
    dac2 = format(dac,"b").zfill(10)
    file.write(dac1[0:2]+" "+dac1[2:6]+" "+dac1[6:10]+" ' 10-bit DAC1 (MSB-LSB): 00 1100 0000 for 0.5 p.e.'\n")
    file.write(dac2[0:2]+" "+dac2[2:6]+" "+dac2[6:10]+" ' 10-bit DAC2 (MSB-LSB): 00 1100 0000 for 0.5 p.e.'\n")

    file.write("1 ' Enable High Gain OTA'  -- start byte 2\n")
    file.write("1 ' Disable High Gain OTA power pulsing mode (force ON)'\n")
    file.write("1 ' Enable Low Gain OTA'\n")
    file.write("1 ' Disable Low Gain OTA power pulsing mode (force ON)'\n")
    file.write("1 ' Enable Probe OTA'\n")
    file.write("1 ' Disable Probe OTA power pulsing mode (force ON)'\n")
    file.write("0 ' Otaq test bit'\n")
    file.write("0 ' Enable Val_Evt receiver'\n")
    file.write("0 ' Disable Val_Evt receiver power pulsing mode (force ON)'\n")
    file.write("0 ' Enable Raz_Chn receiver'\n")
    file.write("0 ' Disable Raz Chn receiver power pulsing mode (force ON)'\n")
    file.write("0 ' Enable digital multiplexed output (hit mux out)'\n")
    if( or32 =="enabled" ):
        file.write("1 ' Enable digital OR32 output'\n")
    elif( or32 == "disabled" ):
        file.write("0 ' Enable digital OR32 output'\n")
    file.write("0 ' Enable digital OR32 Open Collector output'\n")
    file.write("0 ' Trigger Polarity'\n")
    file.write("0 ' Enable digital OR32_T Open Collector output'\n")
    file.write("1 ' Enable 32 channels triggers outputs'\n")

    file.close()

if __name__ == "__main__":
    main()
