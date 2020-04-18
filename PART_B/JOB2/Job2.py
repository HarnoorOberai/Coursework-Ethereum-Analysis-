from mrjob.job import MRJob

# Contracts
# +--------------------+--------+---------+------------+--------------------+
# |             address|is_erc20|is_erc721|block_number|     block_timestamp|
# +--------------------+--------+---------+------------+--------------------+

# Transactions
# +------------+--------------------+--------------------+-------------------+------+-----------+---------------+
# |block_number|        from_address|          to_address|              value|   gas|  gas_price|block_timestamp|
# +------------+--------------------+--------------------+-------------------+------+-----------+---------------+

class repartition_transaction_contracts_join(MRJob):

    def mapper(self, _, line):
        try:
            if(len(line.split(','))==5):
                #this should be a address from Contracts
                fields = line.split(',')
                join_key = fields[0]#
                join_value = fields[3]#block_number
                yield (join_key, (join_value,1))

            elif(len(line.split('\t'))==2):
                #this should be to_address from Transactions
                fields = line.split('\t')
                join_key = fields[0][1:-1] #to_address
                join_value = int(fields[1]) #value1
                # print("transaction",join_key," ",join_value)
                yield (join_key,(join_value,2))
        except:
            pass

    def reducer(self, key, values):
        contract = 0
        transaction = 0
        aggregate_value = 0
        for value in values:
            if (value[1] == 1):
                contract = 1
            if (value[1] == 2):
                transaction = 1
                aggregate_value =value[0]

        # smart_contracts
        if(contract==1) and (transaction ==1):
            yield(key,aggregate_value)



if __name__ == '__main__':
    repartition_transaction_contracts_join.run()
