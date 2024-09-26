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


file_path1 = r'/Users/nitin14.patil/Library/CloudStorage/OneDrive-RelianceCorporateITParkLimited/Documents/python_work/PNL Finance/Adhoc with new changes with_all_columns_Aug24f_25sep.xlsx'

file_path2_1 = r'/Users/nitin14.patil/Downloads/August_jpw_sa_conveyance_detail.csv'

# file_path2_2 = r'/Users/nitin14.patil/Downloads/ConveyanceJune16to30.csv'

df1=pd.read_csv(file_path2_1)

# df2=pd.read_csv(file_path2_2)

# df2=pd.concat([df1,df2],axis=0,ignore_index=True)




# dff = pd.read_csv(file_path1)


# dff.to_csv('test.csv')



# dff.dtypes


# import pandas as pd

# def read_excel_with_exemptions(file_path1, sheet_name=None):
#     try:
#         # First attempt: default read_excel with openpyxl engine (for .xlsx files)
#         df = pd.read_excel(file_path1, sheet_name=sheet_name, engine='openpyxl')
#         print("File read successfully with openpyxl.")
#         return df

#     except UnicodeDecodeError as e:
#         # Handle encoding-related issues
#         print(f"UnicodeDecodeError encountered: {e}")
#         print("Retrying with a different encoding...")
#         try:
#             df = pd.read_excel(file_path1, sheet_name=sheet_name, encoding='utf-16')
#             print("File read successfully with utf-16 encoding.")
#             return df
#         except Exception as e2:
#             print(f"Failed with utf-16 encoding: {e2}")

#     except ValueError as e:
#         # Handle engine-specific issues
#         print(f"ValueError encountered: {e}")
#         print("Retrying with xlrd engine (for older .xls files)...")
#         try:
#             df = pd.read_excel(file_path1, sheet_name=sheet_name, engine='xlrd')
#             print("File read successfully with xlrd.")
#             return df
#         except Exception as e2:
#             print(f"Failed with xlrd engine: {e2}")

#     except FileNotFoundError:
#         print("File not found. Please check the file path.")
        
#     except Exception as e:
#         # Catch-all for any other exceptions
#         print(f"An error occurred: {e}")
    
#     return None


# sheet_name = 'Adhoc with new changes with_all'  # Specify sheet name or None for the first sheet
dff = pd.read_excel(file_path1)


# df = pd.read_excel(file_path1,sheet_name='Sheet1')
# df1 = pd.read_excel(file_path1,sheet_name='Sheet2')

# dff=pd.concat([df,df1],axis=0,ignore_index=True)



dff.dtypes



dff['Date1']=dff['Date'].astype(str)


# Split the 'Full Address' column by '/'
dff[['year', 'month', 'day']] = dff['Date1'].str.split('-', expand=True)


dff.sort_values(by=['Emp ID', 'month', 'day'],inplace=True)


df2=df1

df2[['year', 'month', 'day']] = df2['fromDate'].str.split('-', expand=True)

# dff.columns


# dff.dtypes


# Sum of the 'Value' column
total_sum = dff['Distance(KM)'].sum()

print(total_sum)


if dff is not None:
    # Perform groupby on 'Emp ID' and 'Date', summing 'Final KM' and 'Distance(KM)'
    grouped_df = dff.groupby(['Emp ID', 'month', 'day'], as_index=False)[['KM post Speed','KM post Mark in/Out','Car Hire KM','Distance(KM)', 'Final KM']].sum()
    print(grouped_df)
else:
    print("d_dff is None. Check the DataFrame initialization.")
    
    
    

jmd = dff[['Emp ID', 'JMDO/JMDL']].groupby('Emp ID').agg({'JMDO/JMDL': 'first'}).reset_index()

# Now perform the merge
grouped_df = pd.merge(grouped_df, jmd, how='left', on='Emp ID')


grouped_df.to_csv('test.csv')



grouped_df.dtypes

df2.dtypes


# Sum of the 'Value' column
total_sum = grouped_df['Distance(KM)'].sum()

print(total_sum)



# # grouped_df['Date']=pd.to_datetime(grouped_df['Date'])

# # Explicitly setting the format as DD/MM/YY
# grouped_df['Date'] = pd.to_datetime(grouped_df['Date'], format='%d/%m/%y')

# df2['fromDate']=pd.to_datetime(df2['Date'], format='%d/%m/%y')



df2.sort_values(by=['agentId', 'month', 'day'],inplace=True)


df2_1=df2[['agentId', 'fromDate', 'totalDistance', 'totalAmount', 'month', 'day']]



checkdf1 = dff[(dff['Emp ID'] == 50115492)&(dff['Date'] == '2024-06-01')]



# grouped_df['Date'] = pd.to_datetime(grouped_df['Date']).dt.date
# df2_1['fromDate'] = pd.to_datetime(df2_1['fromDate']).dt.date

merged_df=pd.merge(grouped_df,df2_1,how='left', left_on=['Emp ID', 'month', 'day'], right_on=['agentId', 'month', 'day'])


# Sum of the 'Value' column
total_sum = merged_df['Distance(KM)'].sum()

print(total_sum)


len(grouped_df)
len(merged_df)

merged_df.drop_duplicates(subset=['Emp ID','month', 'day'],inplace=True)

# merged_df.to_csv('test.csv')


merged_df['Date']=merged_df['day']+'/'+merged_df['month']+'/'+'24'

merged_df1=merged_df[['Emp ID', 'Date' , 'JMDO/JMDL','KM post Speed','KM post Mark in/Out','Car Hire KM','Distance(KM)','Final KM','totalDistance', 'totalAmount']]



merged_df1['Difference_additinal_KM']=merged_df1['totalDistance']-merged_df1['Final KM']

merged_df1.rename(columns={'totalDistance':'Claimed_Distance'},inplace=True)

checkdf = merged_df1[(merged_df1['Emp ID'] == 50115492)&(merged_df1['Date'] == '2024-06-01')]

merged_df1.to_csv('./ca_acutal_vs_claim/ca_actual_vs_claim_Jun24_25Sep_f.csv')

# merged_df.to_csv('ca_actual_vs_claim_17_Sep_check.csv')

# pivot_jun=pd.pivot_table(merged_df1,values='Distance(KM)')


# pivot_jun = pd.pivot_table(merged_df1, values='Distance(KM)', index='Date', columns='Emp ID', aggfunc=np.sum)






