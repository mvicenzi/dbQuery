from DataLoader3 import DataLoader, DataQuery

queryUrl = "http://dbdata0vm.fnal.gov:9090/QE/hw/app/SQ/query"
queryUrl = "https://dbdata0vm.fnal.gov:9443/QE/hw/app/SQ/query"

print("\nQuery URL: %s" % queryUrl)
print("\n\nDataQuery Example:")
print(" select .... from sipms where po_number = 'PO-1655048' and batch_number = '1' order by create_time limit 20")
dataQuery = DataQuery(queryUrl)
rows = dataQuery.query("mu2e_hardware_prd", "sipms", 'sipm_id, batch_number, location, sipm_type, pack_number, create_time',
                       where='po_number:eq:PO-1655048&batch_number:eq:1', order='-create_time', limit=20, echoUrl=True)

#rows = dataQuery.query("icarus_hardware_dev", 'sipms', 'sipm_id', 'sipm_id:eq:1', limit=10)

print('\n%s returned\n' % len(rows))
for row in rows:
    print(row)
