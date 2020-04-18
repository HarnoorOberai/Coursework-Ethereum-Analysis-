from mrjob.job import MRJob
import re

class top_10_smart_contracts(MRJob):
     def mapper(self, _, line):

         fields = line.split('\t')
         address = fields[0][1:-1]
         values = int(fields[1])
         yield(None,(address,values))


     def reducer(self, key, values):
         sorted_values= sorted(values, reverse = True, key = lambda x: x[1])
         i = 0
         for value in sorted_values:
             i+=1
             yield((value[0],value[1]),i)
             if i>=10:
               break


     def combiner(self, _,values):
          sorted_values= sorted(values, reverse = True, key = lambda x: x[1])
          i = 0
          for value in sorted_values:
              yield("Top",value)
              i+=1
              if i>=10:
                  break


if __name__ == '__main__':
    top_10_smart_contracts.run()
