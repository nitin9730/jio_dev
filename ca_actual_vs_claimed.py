import pandas as pd
import numpy as np
import math
from itertools import combinations
from datetime import datetime

import pandas as pd
import numpy as np
import os
import pandas as pd
# Path to your Excel file
# file_path = 'MOP IT ST Tables.xlsx'


from haversine import haversine, Unit


file_path1 = r'/Users/nitin14.patil/Library/CloudStorage/OneDrive-RelianceCorporateITParkLimited/Documents/rd.in/rd.in/Adhoc with new changes with_all_columns_July24f.xlsx'

file_path2 = r'/Users/nitin14.patil/Downloads/July_jpw_sa_conveyance_detail.csv'


df = pd.read_excel(file_path1,sheet_name='Sheet1')

df1 = pd.read_excel(file_path1,sheet_name='Sheet2')

dff=pd.concat([df,df1],axis=0,ignore_index=True)

dff.sort_values(by=['Emp ID', 'Date'],inplace=True)

df2=pd.read_csv(file_path2)


dff.columns



if dff is not None:
    # Perform groupby on 'Emp ID' and 'Date', summing 'Final KM' and 'Distance(KM)'
    grouped_df = dff.groupby(['Emp ID', 'Date', 'JMDO/JMDL'], as_index=False)[['Distance(KM)', 'Final KM']].sum()
    print(grouped_df)
else:
    print("d_dff is None. Check the DataFrame initialization.")




grouped_df.dtypes

df2.dtypes

df2['fromDate']=pd.to_datetime(df2['fromDate'])

df2.sort_values(by=['agentId', 'fromDate'],inplace=True)


df2_1=df2[['agentId', 'fromDate', 'totalDistance', 'totalAmount']]



merged_df=pd.merge(df2_1,grouped_df,how='left', left_on=['agentId', 'fromDate'], right_on=['Emp ID', 'Date'])


checkdf = df[(df['Emp ID'] == 50093194)&(df['Date'] == '2024-07-16')]



# merged_df.to_csv('test.csv')



# print(grouped_df.columns)
# print(df2.columns)

# print(grouped_df['Date'].dtype)
# print(df2['fromDate'].dtype)



# merged_df = pd.merge(grouped_df, df2, how='left', left_on=['Emp ID', 'Date'], right_on=['agentId', 'fromDate'])

print(merged_df.head())


merged_df1=merged_df[['Emp ID', 'Date' , 'JMDO/JMDL','Distance(KM)','Final KM','totalDistance', 'totalAmount']]





merged_df1['Difference_additinal_KM']=merged_df1['totalDistance']-merged_df1['Final KM']

merged_df1.rename(columns={'totalDistance':'Claimed_Distance'},inplace=True)


merged_df1.to_csv('ca_actual_vs_claim_16_Sep.csv')



