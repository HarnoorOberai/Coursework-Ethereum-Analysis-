from mrjob.job import MRJob
import re
import time
from datetime import datetime

#this is a regular expression that finds all the words inside a String
WORD_REGEX = re.compile(r"\b\w+\b")

class NumberOTransaction(MRJob):

    def mapper(self, _, line):
        fields = line.split(",")
        try:
            if((len(fields)==7)):
                date = time.localtime(int(fields[6]))
                # time.struct_time(tm_year=2016, tm_mon=9, tm_mday=18, tm_hour=6, tm_min=0, tm_sec=6, tm_wday=6, tm_yday=262, tm_isdst=1)
                year = date[0]
                month = date[1]
                yield((year,month),1)

        except:
            pass


    def reducer(self, key, value):
        yield(key,sum(value))

    def combiner(self, key, value):
        yield(key, sum(value))

if __name__ == '__main__':
    NumberOTransaction.run()
