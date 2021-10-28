
import pandas as pd
import OnlinePjaak
import pyarrow.parquet as pq
import pyarrow as pa
import numpy as np
import OnlinePjaak
import logging

Log_Format = "%(levelname)s %(asctime)s - %(message)s"
logging.basicConfig(filename = "logfile.log",
                    filemode = "w",
                    format = Log_Format,
                    level = logging.INFO)
logger = logging.getLogger()

def diffmergetest(dftest1, dftest2):
    dff = OnlinePjaak.diff(dftest1, dftest2)
    return

def data_test_case(df1,df2):

    col1 = df1.columns.values.tolist()
    col2 = df2.columns.values.tolist()

    labels=('df1', 'df2')
    # Step 1 - Check if no. of columns are the same:
    len_lr = len(col1), len(col2)
    assert len_lr[0]==len_lr[1], \
        'Cannot compare frames with different number of columns: {}.'.format(len_lr)
    logger.info( 'Cannot compare frames with different number of columns: {}.'.format(len_lr))
    # Step 2a - Check if the set of column headers are the same
    #           (order doesnt matter)
    assert set(col1)==set(col2), \
        'Left column headers are different from right column headers.' \
        +'\n   Left orphans: {}'.format(list(set(col1)-set(col2))) \
        +'\n   Right orphans: {}'.format(list(set(col2)-set(col1)))
    logger.info( 'Left column headers are different from right column headers.' )

# Step 2b - Check if the column headers are in the same order
    if col1 != col2:
        print ('[Note] Reordering right Dataframe...')
        OnlinePjaak.logging('[Note] Reordering right Dataframe...')
        df2 = df2[col1]

    # Step 3 - Check datatype are the same [Order is important]
    if set((df1.dtypes == df2.dtypes).tolist()) - {True}:
        print ('dtypes are not the same.')
        df_dtypes = pd.DataFrame({labels[0]:df1.dtypes,labels[1]:df2.dtypes,'Diff':(df1.dtypes == df2.dtypes)})
        df_dtypes = df_dtypes[df_dtypes['Diff']==False][[labels[0],labels[1],'Diff']]
        print (df_dtypes)
        logger.info(df_dtypes)
    else:
        logger.info('DataType check: Passed')


