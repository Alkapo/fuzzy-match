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
from pandas.core.frame import DataFrame





# ask user for 3 inputs 
'''
data1 = str(input("Please point to your client (small) datafile csv. EX : 'C:/download/data.csv'"))
data2 = str(input("Please point to your reference (large) datafile csv. EX : 'C:/download/data.csv'"))
path = str(input("Please specify where you want to save your data. EX : 'C:/download/data.csv'"))
''' 

def clean_names(series):
    return series.str.lower().str.replace(r"\b(quebec inc.|inc.|.|inc|canada|québec|les|groupe|de|la|enr.|&|services|entreprises|tranport|service|services)\b","")


class fuzzy_match:
    def __init__(self, data_client_path='initialV02.csv', data_req_path='Nom.csv') -> None:

        self.data_client_path=data_client_path
        self.data_req_path=data_req_path
        self.time_s=time.time()
        pass
    
    def load_data(self,test=False) -> DataFrame: 
        '''Load data (Cleint and req) and eventualy we will need to ask for the target col name. '''
        #Time stats
        print('Script has started to run at :  ', time.ctime(self.time_s))


        data_client=pd.read_csv(self.data_client_path,sep=';')
        data_client = data_client.rename(columns={"Nomdel'organisation":'name'})

        data_req=pd.read_csv(self.data_req_path)


        if test:
            data_req=self.script_small_set(data_req)
        else:
            pass

            
        data_req['clean_name']=clean_names(data_req['NOM_ASSUJ'])
        return data_req, data_client

    def script_small_set(self,dataset) -> DataFrame:

        #Testing script with small dataset (uncomment next line)
        data_req=dataset.head(100)

        return data_req


   #Fuzzy function 
    def findmatch(self,master_data, client) -> list:
        matches = process.extract(master_data, client, limit=3)
        return matches


    def structure_match(self,origin_data) -> DataFrame:
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

if __name__=="__main__" :
    obj=fuzzy_match(data_req_path='reduce_data_9999.csv')
    req,client=obj.load_data()

    dmaster = dd.from_pandas(req, npartitions=8)



    dmaster['my_value'] = dmaster.clean_name.apply(lambda x: obj.findmatch(x, client['name']), meta=('result', 'object'))



    iter_file = time.strftime("%Y%m%d-%H%M%S")

    # Partition processing on 8 nodes 
    pd_df = pd.DataFrame(dmaster.compute(scheduler='threads') )

    #structure result in seperated columns ( up to 3 best matches)
    pd_df=obj.structure_match(pd_df)

    del pd_df['my_value']


    pd_df.to_csv('master_scored{}.csv'.format(iter_file))


    end_time = time.time()


    print('\nScript has stopted to run at :', time.ctime(time.time()))

    print('Delta : ', time.strftime("%H:%M:%S",time.gmtime(int('{:.0f}'.format(float(str((end_time-obj.time_s))))))))
