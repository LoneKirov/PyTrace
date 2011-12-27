import csv

def commandsToCSV(fName, cmds):
    with open(fName, 'wb') as f:
        writer = csv.writer(f, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(['Start Time', 'End Time', 'LBA', 'FUA', 'length'])

        prev = None
        for cmd in cmds:
            isFUA = 'Y' if hasattr(cmd[0], "fua") and cmd[0].fua() else 'N'
            length = cmd[0].sectorCount() if hasattr(cmd[0], "sectorCount") else 0
            writer.writerow([cmd[0].sTime(), cmd[len(cmd) - 1].eTime(), cmd[0].lba(),
                isFUA, length])
