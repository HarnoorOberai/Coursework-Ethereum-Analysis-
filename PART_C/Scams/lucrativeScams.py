from mrjob.job import MRJob
import re
import time
import statistics
from mrjob.step import MRStep
import json
from datetime import datetime
# +------------+--------------------+--------------------+-------------------+------+-----------+---------------+
# |block_number|        from_address|          to_address|              value|   gas|  gas_price|block_timestamp|
# +------------+--------------------+--------------------+-------------------+------+-----------+---------------+
# |     6638809|0x0b6081d38878616...|0x412270b1f0f3884...| 240648550000000000| 21000| 5000000000|     1541290680|
# |     6638809|0xb43febf2e6c49f3...|0x9eec65e5b998db6...|                  0| 60000| 5000000000|     1541290680|
class lucrativeScams(MRJob):
    sector = {}

    def steps(self):
        return [MRStep(mapper_init=self.mapper_join_init,
                         mapper=self.mapper_repl_join),
                 MRStep(reducer=self.reducer_sum)]

    def mapper_join_init(self):
        with open('scams.json','r') as f:
            file = json.load(f)
        for x in file['result']:
            key = x
            value = file['result'][x]['category']
            self.sector[key] = value

    def mapper_repl_join(self, _, line):
        fields = line.split('\t')
        try:
            if len(fields)==2:
                address = fields[0][1:-1] #to_address
                total_value = int(fields[1]) #value1
                for x in self.sector:
                    if x == address:
                        category = self.sector[x]
                        yield( category, total_value)
                        break
        except:
            pass

    def reducer_sum(self,key,values):
        yield(key,sum(values))


if __name__ == '__main__':
    lucrativeScams.run()
