#Script name matching
import pandas as pd
import json
import os
from collections import Counter
import time

from fuzzywuzzy import fuzz
from fuzzywuzzy import process
# Distribute processing 
import dask.dataframe as dd
import dask.multiprocessing
import dask.threaded





# ask user for 3 inputs 
'''
data1 = str(input("Please point to your client (small) datafile csv. EX : 'C:/download/data.csv'"))
data2 = str(input("Please point to your reference (large) datafile csv. EX : 'C:/download/data.csv'"))
path = str(input("Please specify where you want to save your data. EX : 'C:/download/data.csv'"))
''' 

#Load data (Cleint and req)
data_client=pd.read_csv('initialV02.csv',sep=';')
data_client = data_client.rename(columns={"Nomdel'organisation":'name'})

data_req=pd.read_csv('Nom.csv')

#Testing script with small dataset (uncomment next line)

#ata_req=data_req.head(10)


#Time stats
time_s=time.time()
print('Script has started to run at :  ', time.ctime(time_s))


def clean_names(series):
    return series.str.lower().str.replace(r"\b(quebec inc.|inc.|.|inc|canada|québec|les|groupe|de|la|enr.|&|services|entreprises|tranport|service|services)\b","")
data_req['clean_name']=clean_names(data_req['NOM_ASSUJ'])


#Fuzzy function 
def findmatch(master_data, client):
    matches = process.extract(master_data, client, limit=3)
    return matches
def structure_match(origin_data):
    for i,row in origin_data.my_value.iteritems():

        exactmatch=row[0][1]==100 

        origin_data.loc[i,'Match1']=row[0][0]
        origin_data.loc[i,'Match1 score']=row[0][1]


        origin_data.loc[i,'Match2']= "" if exactmatch else row[1][0]
        origin_data.loc[i,'Match2 score']="" if exactmatch else row[1][1]
        
        origin_data.loc[i,'Match3']= "" if exactmatch else row[2][0]
        origin_data.loc[i,'Match3 score']="" if exactmatch else row[2][1]

    return origin_data

#Search for match
dmaster = dd.from_pandas(data_req, npartitions=8)



dmaster['my_value'] = dmaster.clean_name.apply(lambda x: findmatch(x, data_client['name']),meta=('result', 'object'))



iter_file = time.strftime("%Y%m%d-%H%M%S")

# Partition processing on 8 nodes 
pd_df = pd.DataFrame(dmaster.compute(scheduler='threads') )

#structure result in seperated columns ( up to 3 best matches)
pd_df=structure_match(pd_df)

del pd_df['my_value']


pd_df.to_csv('master_data_scored{}.csv'.format(iter_file))


end_time = time.time()


print('\nScript has stopted to run at :', time.ctime(time.time()))

print('Delta : ', time.strftime("%H:%M:%S",time.gmtime(int('{:.0f}'.format(float(str((end_time-time_s))))))))
