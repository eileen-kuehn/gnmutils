import sys
import os
import logging
import re
import gzip

class CSVReader(object):
    def __init__(self, parser=None):
        # caches the different header fields
        self._headerCache = {}
        self._parser = parser
        self._tme = None

    # allow access to parser
    @property
    def parser(self):
        return self._parser

    @parser.setter
    def parser(self, value):
        self._parser = value

    # prepare for next CSV file to be read
    def clearCaches(self):
        del self._headerCache
        self._headerCache = {}
        self._tme = None
        if self._parser:
            self._parser.clearCaches()

    def processCSV(self, filename):
        openFunction = open
        if re.match(".*.gz$", filename):
          openFunction = gzip.open
        with openFunction(filename, 'r') as csvfile:
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
                    tme = line.split(",")[(self._headerCache[self.parserName()])['tme']] or self._tme or tme
                    self._tme = int(tme)
                except KeyError:
                    # initialize the header cache
                    row = line.split(",")
                    # check if maybe no header is included
                    if "tme" not in row[0]:
                        self._headerCache[self.parserName()] = self._parser.defaultHeader()
                    else:
                        headerCache = {}
                        for index, item in enumerate(line.split(",")):
                            headerCache[item] = index
                        self._headerCache[self.parserName()] = headerCache
                        continue
                except ValueError:
                    # current line is header line
                    headerCache = {}
                    for index, item in enumerate(line.split(",")):
                        headerCache[item] = index
                    self._headerCache[self.parserName()] = headerCache
                    continue
                while True:
                    try:
                        row = line.split(",")
                        # check if there are too many header fields than expected
                        if len(row) > len(self._headerCache[self.parserName()]):
                            logging.info("Trying to fix wrong row length: row %d (%s:%d - %s) vs. header %d" %(len(row), filename, idx, line, len(self._headerCache[self.parserName()])))
                            # check if additional "," are in command and remove
                            while len(row) > len(self._headerCache[self.parserName()]):
                                cmdIndex = self._headerCache[self.parserName()]["cmd"]
                                cmdString = row[cmdIndex] + row[cmdIndex+1]
                                del row[cmdIndex+1]
                                row[cmdIndex] = cmdString
                        # finally add the valid row to the parser
                        self._parser.parseRow(row=row, headerCache=self._headerCache[self.parserName()], tme=int(tme))
                    except IndexError as e:
                        line = (line + csvfile.next())[:-1]
                    except StopIteration as e:
                        logging.warn("there seems to be a wrong ending in the file for line %d (%s) in file %s" %(idx, line, filename))
                    else:
                        break;
        self._parser.checkCaches(tme=self._tme)

    def parserName(self):
        return self._parser.__class__.__name__
