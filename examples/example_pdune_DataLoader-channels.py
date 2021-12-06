from DataLoader import DataLoader, DataQuery
import time
import sys
import os

import ssl
import random

### Highly discouraged!
ssl._create_default_https_context = ssl._create_unverified_context

# Pass this to urllib.urlopen(url, context=context)
# context = ssl._create_unverified_context()

def createSiPMs(sid):
    return {
        'sipm_sn': "sipm_{}".format(sid),            # text                      not null
        'procedure_document_version': "v1.0",        # text                      not null
        #'batch': "",                                 # text
        #'date_received_niu': "",                     # timestamp with time zone
        #'warm_test_date': "",                        # timestamp with time zone
        #'warm_test_worker': "",                      # text
        #'warm_test_temperature': "",                 # double precision
        'iv_curve': open("fractal_110.jpg", "rb"),
        #'iv_curve_thmb': "",                         # text
        'breakdown_voltage': 1.2345678901,           # double precision
        'test_bias_voltage': 1.23456789e-4,          # double precision
        'threshold': 200000./3.,                     # double precision
        'gain': 12345.1234567890123456,              # double precision
        'cross_talk': 0.0,                           # double precision
        'dark_rate': 1.2345678e-7,                   # double precision
        #'date_mounted': "",                          # timestamp with time zone
        #'date_received_csu': "",                     # timestamp with time zone
        'comments': "Some comment.......",            # text
    }


def createChannel(cid, chno, sid):
    return {
        'board_sn': "board_{}".format(cid),
        'channel_number': chno,
        # 'sipm_sn': "sipm_{}".format(sid),
        'test_trace_image': open("fractal_110.jpg", "rb"),
        'bias_voltage':   1.2345678,
        'threshold':  12345,
        'dark_rate':   12.2345678,
        'gain':    123.2345678,
        'cross_talk':    1234.2345678,
        'comments': "Some comment......."
    }


password = os.environ.get("LOADER_PWD", "xxxxxxxxx")
# url = "https://rexdb03.fnal.gov:9443/pdune/hdb/loader"
url = "https://rexdb03.fnal.gov:9543/hdb/tst/loader"

dataLoaders = DataLoader(password, url, group="PDS Tables", table="sipms")
dataLoader = DataLoader(password, url, group="PDS Tables", table="channels")
dataLoader1 = DataLoader(password, url, group="PDS Tables", table="hoverboards")

cid = random.randrange(1000)

dataLoader1.addRow(dict(hoverboard_sn="board_{}".format(cid), procedure_document_version='v1.0.1'))
(retVal, code, text) = dataLoader1.send(echoUrl=False)
if retVal:
    print "DataLoader - Success!"
    print text
else:
    print "Failed!"
    print code
    print text
    sys.exit(1)

sid = random.randrange(1000)
row = createSiPMs(sid)
dataLoaders.addRow(row)
(retVal, code, text) = dataLoaders.send(echoUrl=False)
if retVal:
    print "DataLoader - Success!"
    print text
else:
    print "Failed!"
    print code
    print text
    sys.exit(1)


row = createChannel(cid, random.randrange(4), sid)
dataLoader.addRow(row)
(retVal, code, text) = dataLoader.send(echoUrl=False)
dataLoader.clearRows()

if retVal:
    print "DataLoader - Success!"
    print text
else:
    print "Failed!"
    print code
    print text
    sys.exit(1)

if 1:
    row.pop('test_trace_image')
    dataLoader.addRow(row, mode='update')

    (retVal, code, text) = dataLoader.send(echoUrl=False)


    if retVal:
        print "DataLoader - Success!"
        print text
    else:
        print "Failed!"
        print code
        print text

    dataLoader.clearRows()

