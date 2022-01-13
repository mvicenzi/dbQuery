import sys

from DataLoader3 import DataQuery

#---------------------------------------------------------------#
# Main configuration parameters to query database
#---------------------------------------------------------------#
queryUrl = "https://dbdata0vm.fnal.gov:9443/QE/hw/app/SQ/query"
dBname = "icarus_hardware_prd"
cableTable = "crtcable"
febTable = "crtfeb"
dataQuery = DataQuery(queryUrl)
#---------------------------------------------------------------#

def print_usage():
    print('Command Usage: python3 get_macadd_from_dataline.py <data line> <number of feb for data line>')
    sys.exit(1)

#find febs in each cable data line
def find_feb(data_line,line_febs):
  if line_febs > 9:
   feb_barcode = dataQuery.query(database=dBname, table=cableTable, columns="end_feb", where='cable_id:eq:L0'+str(data_line)+'-D-'+str(line_febs))
  else:
   feb_barcode = dataQuery.query(database=dBname, table=cableTable, columns="end_feb", where='cable_id:eq:L0'+str(data_line)+'-D-0'+str(line_febs))

  return feb_barcode[0]

#----------------------------------------------------------------#
def find_mac(feb_barcode):
  feb_mac = dataQuery.query(database=dBname, table=febTable, columns="mac_add", where='feb_barcode:eq:'+feb_barcode)
  return feb_mac[0]
  
#----------------------------------------------------------------#
def main():

    args = sys.argv   

    if (len(args) < 2 or len(args) > 3):
        print('number of arguments: ',len(args))
        print_usage()

   
    data_line = sys.argv[1]
    line_febs = int(sys.argv[2])

    print('data line L0'+str(data_line)+':')
    for i in range(1,line_febs+1):
      feb_barcode = find_feb(data_line,i)
      #print(feb_barcode)
      feb_mac = find_mac(feb_barcode)
      print(feb_barcode+', mac: '+feb_mac)

    

if __name__ == "__main__":
    main()
