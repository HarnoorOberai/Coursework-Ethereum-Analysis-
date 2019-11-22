from mrjob.job import MRJob
import re
import time

#this is a regular expression that finds all the words inside a String
WORD_REGEX = re.compile(r"\b\w+\b")

class NumberOTransaction(MRJob):

    def mapper(self, _, line):
        fields = line.split(",")
        try:
            if((len(fields)==7)):
                time_epoch = int(fields[0])/1000
                print("time_epoch", time_epoch)
                day = time.strftime("%d",time.gmtime(time_epoch)) #returns day of the month
                yield(day,1)
        except:
            pass

    def reducer(self, key, value):
        yield(key,sum(value))

if __name__ == '__main__':
    NumberOTransaction.run()
