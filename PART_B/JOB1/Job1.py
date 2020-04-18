from mrjob.job import MRJob
import re
import time
from datetime import datetime
# +------------+--------------------+--------------------+-------------------+------+-----------+---------------+
# |block_number|        from_address|          to_address|              value|   gas|  gas_price|block_timestamp|
# +------------+--------------------+--------------------+-------------------+------+-----------+---------------+

class NumberOTransaction(MRJob):

    def mapper(self, _, line):
        fields = line.split(",")
        try:
            if((len(fields)==7)):
                to_address = fields[2]
                value = int(fields[3])
                yield(to_address,value)
        except:
            pass

    def reducer(self, key, value):
        yield(key,sum(value))

    def combiner(self, key, value):
        yield(key, sum(value))

if __name__ == '__main__':
    NumberOTransaction.run()
