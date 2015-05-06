import sys
import os
import logging

class CSVReader(object):
    def __init__(self, parser=None):
        # caches the different header fields
        self._headerCache = {}
        self._parser = parser
        
    # allow access to parser
    @property
    def parser(self):
        return self._parser
        
    # prepare for next CSV file to be read
    def clearCaches(self):
        del self._headerCache
        self._headerCache = {}
        self._parser.clearCaches()
        
    def processCSV(self, filename):
        with open(filename, 'r') as csvfile:
            tme = 0
            # process every line in csvfile
            for idx, line in enumerate(csvfile):
                # first check for comments in CSV line and skip
                if line[0] == "#":
                    continue
                    
                # remove newline character from line
                line = line[:-1]
                try:
                    # as long as the header has not been initialized,
                    # an exception is thrown and the header row is deteced
                    # otherwise the usual processing process starts
                    tme = line.split(",")[(self._headerCache[self.parserName()])['tme']]
                except KeyError:
                    # initialize the header cache
                    row = line.split(",")
                    # check if maybe no header is included
                    if "tme" not in row[0]:
                        self._headerCache[self.parserName()] = {"tme":0, "exit_tme": 1 ,"pid": 2, "ppid": 3, "gpid": 4, "uid": 5, "name": 6, "cmd": 7, "error_code": 8, "signal": 9, "valid": 10, "int_in_volume": 11, "int_out_volume": 12, "ext_in_volume": 13, "ext_out_volume":14}
                    else:
                        headerCache = {}
                        for index, item in enumerate(line.split(",")):
                            headerCache[item] = index
                        self._headerCache[self.parserName()] = headerCache
                else:
                    while True:
                        try:
                            row = line.split(",")
                            # check if there are too many header fields than expected
                            if len(row) > len(self._headerCache[self.parserName()]):
                                logging.info("Trying to fix wrong row length: row %d vs. header %d" %(len(row), len(self._headerCache[self.parserName()])))
                                # check if additional "," is in the command and remove
                                if len(row) == len(self._headerCache[self.parserName()])+1:
                                    # there seems to be a "," inside the cmd
                                    cmdIndex = self._headerCache[self.parserName()]["cmd"]
                                    cmdString = row[cmdIndex] + row[cmdIndex+1]
                                    del row[cmdIndex+1]
                                    row[cmdIndex] = cmdString
                                else:
                                    logging.error("wrong length of row")
                                    sys.exit(1)
                            # finally add the valid row to the parser
                            self._parser.parseRow(row=row, headerCache=self._headerCache[self.parserName()], tme=int(tme))
                        except IndexError as e:
                            line = (line + csvfile.next())[:-1]
                        except StopIteration as e:
                            logging.warn("there seems to be a wrong ending in the file for line %d (%s) in file %s" %(idx, line, filename))
                        else:
                            break;
                            
    def parserName(self):
        return self._parser.__class__.__name__
                        