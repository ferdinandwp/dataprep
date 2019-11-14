# This script is to create pipeline for site utilization
# Import library
import numpy as np
import pandas as pd
import os
import gc
import sqlalchemy as db
from sqlalchemy import create_engine
# import openpyxl
# import xlsxwriter
import pymysql

#---------------------------------Vital Datasets-------------------------------------#
# Identify path
data_path = '/c/Users/fwidjojoputra/Desktop/dataprep/data/input'
file_name = os.path.join(data_path,'input1_bill.csv')

# bill_df = pd.read_csv(file_name)
# print(bill_df.head(5))
# df = pd.read_csv(filepath_or_buffer='./data/vital.csv', sep=',',encoding='utf-8')

# # Create internal vital dataset
# req_df = df.loc[:,['Proj Invoice Header Value1',
#                     'Employee ID',
#                     'Employee Name',
#                     'Quantity_Unit',
#                     'Pay End Date',
#                     'Accounting Status',
#                     'Resource Type'
#                     ]]
# vital_df = req_df.dropna()

# # Rename columns 
# vital_df.rename({'Proj Invoice Header Value1':'project_no',
#                 'Employee ID':'easi_id',
#                 'Employee Name':'full_name',
#                 'Quantity_Unit':'qty_unit',
#                 'Accounting Status':'acc',
#                 'Resource Type':'res',
#                 'Pay End Date':'date'},axis=1,inplace=True)

# # new_df = vital_df[(vital_df.res == 'LABOR')|(vital_df.res == 'SUBCN')]

# #------------------------------------Personnel dataset----------------------------------#
# # Data Perparation from MySQL #
engine = create_engine('mysql+pymysql://admin:password@10.140.9.159:3306/datacentralserver',echo=True)
p_df = pd.read_sql_query("SELECT easiid, fullname, team, clientcode FROM personnelt WHERE Active='Yes'" ,engine)

# p_df.rename({'easiid':'easi_id',
#             'fullname':'full_name'
#             })

print(p_df.head(10))
print(p_df.dtypes)



