from mrjob.job import MRJob
import re
import time
import statistics
from datetime import datetime
# +------------+--------------------+--------------------+-------------------+------+-----------+---------------+
# |block_number|        from_address|          to_address|              value|   gas|  gas_price|block_timestamp|
# +------------+--------------------+--------------------+-------------------+------+-----------+---------------+
# |     6638809|0x0b6081d38878616...|0x412270b1f0f3884...| 240648550000000000| 21000| 5000000000|     1541290680|
# |     6638809|0xb43febf2e6c49f3...|0x9eec65e5b998db6...|                  0| 60000| 5000000000|     1541290680|

class gas_guzzler_price_changed(MRJob):

    def mapper(self, _, line):
        fields = line.split(",")
        try:
            if((len(fields)==7)):
                date = time.localtime(int(fields[6]))
                # time.struct_time(tm_year=2016, tm_mon=9, tm_mday=18, tm_hour=6, tm_min=0, tm_sec=6, tm_wday=6, tm_yday=262, tm_isdst=1)
                year = date[0]
                month = date[1]
                gas_price = int(fields[5])
                yield((year,month),gas_price)
        except:
            pass


    def reducer(self, key, values):
        yield(key,statistics.mean(values))



if __name__ == '__main__':
    gas_guzzler_price_changed.run()
