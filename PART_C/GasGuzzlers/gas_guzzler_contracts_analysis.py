from mrjob.job import MRJob
import re
import time
import statistics
from mrjob.step import MRStep
from datetime import datetime
# +------------+--------------------+--------------------+-------------------+------+-----------+---------------+
# |block_number|        from_address|          to_address|              value|   gas|  gas_price|block_timestamp|
# +------------+--------------------+--------------------+-------------------+------+-----------+---------------+
# |     6638809|0x0b6081d38878616...|0x412270b1f0f3884...| 240648550000000000| 21000| 5000000000|     1541290680|
# |     6638809|0xb43febf2e6c49f3...|0x9eec65e5b998db6...|                  0| 60000| 5000000000|     1541290680|

class gas_guzzler_contracts_analysis(MRJob):
    sector = {}

    def mapper_join_init(self):
        # load companylist into a dictionary
        # run the job with --file input/companylist.tsv
        with open("outputTop10.txt") as f:
            for line in f:
                Intialfields = line.split("\t")
                fields = Intialfields[0].split(",")
                address = fields[0][2:-1]
                self.sector[address] = fields[1]

    def mapper_repl_join(self, _, line):

        fields = line.split(",")
        try:
            if((len(fields)==7)):

                to_address = fields[2]
                if to_address in self.sector:
                    gas = int(fields[4])
                    trans_value = int(fields[3])
                    # date = time.localtime(int(fields[6]))
                    # time.struct_time(tm_year=2016, tm_mon=9, tm_mday=18, tm_hour=6, tm_min=0, tm_sec=6, tm_wday=6, tm_yday=262, tm_isdst=1)
                    time_epoch = int(fields[6])
                    YMD = time.strftime("%Y-%m-%d",time.localtime(time_epoch))
                    # yield(to_address,(YMD,gas,trans_value))
                    yield ((to_address,YMD),gas)
        except:
            pass

    # def mapper_length(self,key,value):
    #     yield(key,list(value))

    def reducer_sum(self,key,values):
        yield(key,statistics.mean(values))
        # for value in values:
        #     yield(key,value)


    def steps(self):
        return [MRStep(mapper_init=self.mapper_join_init,
                         mapper=self.mapper_repl_join),
                 MRStep(reducer=self.reducer_sum)]


if __name__ == '__main__':
    gas_guzzler_contracts_analysis.run()
