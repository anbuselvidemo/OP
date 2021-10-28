import unittest
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import numpy as np
import logging
Log_Format = "%(levelname)s %(asctime)s - %(message)s"
logging.basicConfig(filename = "logfile.log",
                filemode = "w",
                format = Log_Format,
                level = logging.INFO)
logger = logging.getLogger()

#table1 = pq.read_table('C:/Users/Sid/Documents/anbu/onlinepajaak/2021-10-25-dump.parquet')
# table2 = pq.read_table('C:/Users/Sid/Documents/anbu/onlinepajaak/2021-10-26-dump.parquet')


def read_file():

    print("First line for reading.")
    input_file1 = input("Enter name of input file1: ")
    input_file2 = input("Enter name of input file2: ")
    table1 = pq.read_table(input_file1)
    table2 = pq.read_table(input_file2)
    df1 = table1.to_pandas()
    df2 = table2.to_pandas()

    logger.info("Completed reading of file1")
    logger.info("Completed reading of file2")

    table1 = pa.Table.from_pandas(df1)
    table2 = pa.Table.from_pandas(df2)

    return df1, df2

#function to create Insert and delete delta records from snapshot
def diff(file_one,file_two):

    desired_width=320
    pd.set_option('display.width', desired_width)
    np.set_printoptions(linewidth=desired_width)
    pd.set_option('display.max_columns',10)
    pd.set_option('display.max_rows', None)
    df_insert = pd.concat([file_two,file_one,file_one]).drop_duplicates(keep=False)
    logger.info("Created Diff file with records to Insert")
    df_delete = pd.concat([file_two,file_one,file_two]).drop_duplicates(keep=False)
    logger.info("Created Diff file with records to delete")
    return df_delete, df_insert

# Function to merge old snapshot and diff files to create latest
def merge(df4,df_delete,df_insert):
    df_merge1 = pd.concat([df_delete,df4]).drop_duplicates(keep=False)
    df_merge = pd.concat([df_merge1,df_insert])
    logger.info("Created merged file")
    return df_merge


if __name__== '__main__':
    file_one, file_two = read_file()
    df_delete, df_insert = diff(file_one, file_two)
    df_merge = merge(file_one,df_delete,df_insert)
    df_delete.to_csv("Records_to_delete1.csv", sep=",")
    df_insert.to_csv("Records_to_insert1.csv",sep=",")
    df_merge.to_csv("Records to merge1.csv", sep=",")

def unittest():
      # #Unit testing: //
    dict1 = {'id': ['5553155136','9061249075'], 'activation_date': ['2021-06-05 11:35:27 ','2021-01-29 05:55:59'], 'activate': ['0','1']  ,'owner_id':['UD','PT'], 'company_name':['Oktaviani Kusumo','Uwais Simanjuntak'], 'company_email': ['xyz@gmail.com','x@pt.desa.id'],'contact_name':['Latika Mansur, M.Farm','Tania Firgantoro'], 'is_billing':['1','1']}
    dict2 =  {'id': ['5090908955','9061249075','5553155136'], 'activation_date': ['2021-06-05 11:35:27','2021-10-27 ','2021-01-29 05:55:59'], 'activate': ['0','0','1']  ,'owner_id':['UD','CV','PD'], 'company_name':['Oktaviani Kusumo','Habibi Sudiati','Uwais Simanjuntak'], 'company_email': ['xyz@gmail.com','xx@ud.go.id','xxxxxx@pt.desa.id'],'contact_name':['Latika Mansur, M.Farm','Dr. Virman Wahyuni','Tania Firgantoro'], 'is_billing':['0','0','1']}
    dftest1 = pd.DataFrame(data=dict1)
    dftest2 = pd.DataFrame(data=dict2)
    df_delete, df_insert = diff(dftest1, dftest2)
    df_merge = merge(dftest1,df_delete,df_insert)
    assert df_merge.equals(dftest2) ,\
        'Merged files are not matching the expected results'











