from DataLoader import DataLoader, DataQuery
import time

forceError = False  # Set to True to force an error in the data


def createRow():
    return {'hw_id':           'hardwareId 1',
            'detector':        'NearDet',
            'hw_type':         'FEB',
            'install_date':    time.strftime('%Y-%m-%d %H:%M:%S %Z'),
            'block':           1,
            'plane':           2,
            'module_position': 3,
            'dcm_name':        'Buddy',
            'dcm_port':        6423,
            'worker':          'worker 1',
            }

password = "???"
url = "https://dbweb4.fnal.gov:8443/hardwaredb/mu2edev/HdbHandler.py/loader"
queryUrl = "http://dbdata0vm.fnal.gov:9090/QE/hw/app/SQ/query"
group = "Demo Tables"
table = "hardware_position"

dataLoader = DataLoader(password, url, group, table)

dataLoader.addRow(createRow())

if forceError is False:
    # Sleep so we don't do 2 records with the same timestamp, which will
    # cause a Postgresql primary key error for this table.
    time.sleep(2)

dataLoader.addRow(createRow())

# retVal,code,text =  dataLoader.send()


# if retVal:
#     print "DataLoader - Success!"
#     print text
# else:
#     print "Failed!"
#     print code
#     print text

# dataLoader.clearRows()

print("\n\nDataQuery Example:")
print("Select a max of 5 rows from %s where hw_id='crv00002' AND detector='NearDet'" % table)
print("order them by the create_time, in descending order")
dataQuery = DataQuery(queryUrl)
rows = dataQuery.query('mu2e_hardware_dev', table, 'hw_id,detector,install_date,block,plane',
                       'hw_id:eq:crv00002&detector:eq:NearDet', '-create_time', 5, echoUrl=True)
for row in rows:
    print(row)
