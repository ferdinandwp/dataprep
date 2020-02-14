import numpy as np
import pandas as pd
import os
import gc
import sqlalchemy as db
from sqlalchemy import create_engine
import pymysql
from datetime import date, datetime

# Identify Path for bill dataset
data_path1 = 'E:\\EASi\\5-Operations\\50-Management\\500-DataCentral\\2-Static Dashboard\\WIP\\data\\input\\'
file_name1 = os.path.join(data_path1,'bill.csv')
# Identify path for scl dataset
data_path2 = 'E:\\EASi\\5-Operations\\50-Management\\500-DataCentral\\2-Static Dashboard\\WIP\\data\\input\\'
file_name2 = os.path.join(data_path2,'scl.csv')

# Assign dataset
df1 = pd.read_csv(file_name1)
df2 = pd.read_csv(file_name2)

# identify all required attributes
bill_df = df1.loc[:,['Customer Id',
                    'Employee ID',
                    'Customer Name',
                    'Employee Name',
                    'Proj Invoice Header Value1',
                    'WBS_Activity Id',
                    'Activity Descr',
                    'Resource Type',
                    'Accounting Status',
                    'Quantity_Unit',
                    'Bill Rate',
                    'Total Amount',
                    'Pay End Date'
                    ]]
scl_df = df2.loc[:,['Customer Id',
                    'Employee ID',
                    'Employee Name',
                    'Resource Type',
                    'Accounting Status',
                    'Quantity_Unit',
                    'WBS_Activity Id',
                    'Activity Descr',
                    'Pay End Date'
                    ]]

# Modify column name
bill_df.rename({'Customer Id':'client_id',
                'Employee ID':'easi_id',
                'Customer Name':'client_name',
               'Employee Name':'name',
               'Proj Invoice Header Value1':'sow_no',
               'WBS_Activity Id':'wbs_id',
               'Activity Descr':'description',
               'Resource Type':'res_type',
               'Accounting Status':'acc_status',
               'Quantity_Unit':'hours',
               'Bill Rate':'bill_rate',
               'Total Amount':'amount', 
               'Pay End Date':'weekending'},axis=1,inplace=True)

scl_df.rename({'Customer Id':'client_id',
               'Employee ID':'easi_id', 
               'Employee Name':'name', 
               'Resource Type':'res_type', 
               'Accounting Status':'acc_status',
               'Quantity_Unit':'hours',
               'WBS_Activity Id':'wbs_id',
               'Activity Descr':'act_desc',
               'Pay End Date':'weekending'},axis=1,inplace=True)

# Change data type as required
bill_df['weekending'] = bill_df['weekending'].astype('datetime64[ns]')
scl_df['weekending'] = scl_df['weekending'].astype('datetime64[ns]')

# filtering if required
acc_status_hours  = ['IEH','PAY'] # this is filter for hours
res_type_hours  = ['LABOR','SUBCN']

acc_status_amount  = ['BIL','BLD','XRV']
res_type_amount  = ['LABOR','SUBCN','EQUIP']

hours_raw_df = bill_df.loc[bill_df['acc_status'].isin(acc_status_hours)] 
hours_df = hours_raw_df.loc[hours_raw_df['res_type'].isin(res_type_hours)] # final hours dataframe

amount_raw_df = bill_df.loc[bill_df['acc_status'].isin(acc_status_amount)]
amount_df = amount_raw_df.loc[amount_raw_df['res_type'].isin(res_type_amount)] # final billable $$ dataframe

# for SCL ONLY
# drop internal people (Stephen, Martin, Ferdi, Mike, Jessie)
scl_df = scl_df.drop(scl_df[(scl_df['easi_id'] == 6037624)|
                                              (scl_df['easi_id'] == 6142088)|
                                              (scl_df['easi_id'] == 6271423)|
                                              (scl_df['easi_id'] == 6733494)|
                                              (scl_df['easi_id'] == 6760845)].index)

# Filter only for IEH & PAY
acc_filter = ['IEH','PAY']
res_filter = ['LABOR','SUBCN']
scl_raw_df = scl_df.loc[scl_df['acc_status'].isin(acc_filter)]
scl_df = scl_raw_df.loc[scl_raw_df['res_type'].isin(res_filter)] # final scl dataframe

# link dataset with data central
engine = db.create_engine('mysql+pymysql://admin:password@10.140.9.159:3306/datacentralserver', echo=True)
client_df = pd.read_sql_query("SELECT * FROM client",engine)
resource_df = pd.read_sql_query("SELECT EASiID as easi_id, ClientCode as client_code, Department as department FROM personnelt WHERE Active='Yes' AND ClientCode NOT IN(SELECT clientCode from personnelt WHERE clientcode IN('','Internal'))",engine)

# Join with client table in DC
combined_hours_df = hours_df.merge(client_df,on='client_id',how='left')
combined_amount_df = amount_df.merge(client_df,on='client_id',how='left')
combined_scl_df = scl_df.merge(resource_df,on='easi_id',how='left')

# Group it all based on client ID
hours_client_df = combined_hours_df.groupby('client_code')['hours'].sum()
amount_client_df = combined_amount_df.groupby('client_code')['amount'].sum()
scl_client_df = combined_scl_df.groupby('client_code')['hours'].sum()

temp_df = hours_client_df.reset_index().merge(amount_client_df.reset_index(), on='client_code', how='inner')
temp_df.rename({'hours':'bill'},axis=1, inplace=True)

td_df = temp_df.merge(scl_client_df.reset_index(), on='client_code', how='left')
td_df.rename({'hours':'nonbill', 
              'client_code':'client',
              'amount':'revenue'},axis=1, inplace=True)
td_df.fillna(0,inplace=True)
td_df.insert(4,'fring',0)                 #--------------------------------------#
td_df.insert(5,'weekending','2019-11-30') #-----Change to current weekending-----#
                                          #--------------------------------------#
print(td_df.head())
# combined_scl_df.sum()
# once validated, insert to mysql with code below
# td_df.to_sql('time_dimension',con=engine, if_exists='append',index=False)

# perpare dataset to be inputed in DC
bill_info = combined_hours_df.loc[:,['easi_id',
                                     'sow_no',
                                     'wbs_id',
                                     'description',
                                     'client_code',
                                     'hours',
                                     'weekending']]

print("Total Billable Hours = {}".format(bill_info['hours'].sum()))
# once validated, insert to mysql with code below
# bill_info.to_sql('bill',con=engine, if_exists='append',index=False)

# perpare dataset to be inputed in DC
scl_info = combined_scl_df.loc[:,['easi_id', 
                                 'hours', 
                                 'wbs_id', 
                                 'act_desc',
                                 'weekending']]
print("Total Billable Hours = {}".format(scl_info['hours'].sum()))
# once validated, insert to mysql with code below
# scl_info.to_sql('scl',con=engine, if_exists='append',index=False)