#Libraries used in code

import os
import logging
import pandas as pd
import numpy as np
import io
import gcsfs
from datetime import datetime,timedelta
from datetime import date
from google.cloud import storage
from google.cloud import bigquery
import itertools
import math
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator


# from airflow.providers.google.cloud.hooks.gcs import GCSHook

#Date Selection

currentDate = date.today()
current_date = datetime.now()
previous_month = current_date.replace(day=1) - timedelta(days=1)
currentMonth = previous_month.month


#currentMonth selected '6'

currentMonth = '6'


#currentMonth = datetime.now().month

currentYear = datetime.now().year

#check for bigquery table if exist

# def check_bigquery_table_exist(bq_project_id, bq_dataset, bq_table):
#     check_table_exists = BigQueryCheckOperator(
#         task_id='check_table_exists_task',
#         sql='SELECT 1 FROM `{bq_project_id}.{bq_dataset}.{bq_table}` LIMIT 1',
#         use_legacy_sql=False,
#         project_id=bq_project_id
#     )

#delete bigquery table if exist

# def delete_data_from_bigquery_table(bq_project_id, bq_dataset, bq_table, month):
#     delete_data = BigQueryExecuteQueryOperator(
#         task_id='delete_data_task',
#         sql=f"DELETE FROM `{bq_project_id}.{bq_dataset}.{bq_table}` WHERE year_month= "+ "'" +month+ "'",
#         use_legacy_sql=False
#     )
#     return delete_data


#data ingestion to bigquery

# def data_ingestion_to_bq(bucket_name, file_name, bq_project_id, bq_dataset, bq_table):

# #bucket access by storage client

#     storage_client = storage.Client()
#     bucket = storage_client.bucket(bucket_name)

# #file access from bucket

#     blob = bucket.blob(file_name)
#     data = blob.download_as_bytes()

#     df = pd.read_excel(io.BytesIO(data))

# #files access by it's name and change columns for further process

#     if 'sales' in file_name:
#         df = df.rename(columns={'Month, Year of Billing Date' : 'Month & Year of Billing Date'}) #in sales
#     if 'transfer_price_input' in file_name:
#         df = df.rename(columns={'MRP  (Incl GST)' : 'MRP Incl GST','RRP  (Incl GST)' : 'RRP Incl GST','Cash Coupon  (Incl GST)' : 'Cash Coupon Incl GST', 'MOP  (Incl GST)' : 'MOP Incl GST', 'Margin %' : 'Margin per', 'Transfer Price  (Incl GST)' : 'Transfer Price Incl GST', 'Transfer Price (Excl GST)' : 'Transfer Price Excl GST'})
#     if 'family_level_input' in file_name:
#         df = df.rename(columns={'MR21 (COGS Adjst)' : 'MR21 COGS Adjst'})
#     if 'percentage_input' in file_name:
#         df = df.rename(columns={'RD.IN': 'RD IN', 'Jio Mart.Com': 'Jio Mart Com'})
#     if 'lov_family_master' in file_name:
#         df = df.rename(columns={'Family.1' : 'Family 1'})
#     if 'transfer_price_sales' in file_name:
#         df = df.rename(columns={'PBG DC / Non PBG DC' : 'PBG DC or Non PBG DC', 'Total Sales Quantity / Base Unit' : 'Total Sales Quantity or Base Unit'})

# #df column format change to year-month(yyyy-mm)

#     df['year_month']= (current_date.replace(day=1) - timedelta(days=1)).strftime('%Y-%m')

#     month = (current_date.replace(day=1) - timedelta(days=1)).strftime('%Y-%m')
    
# #delete data from bigquery table by table id dataset table and month

#     delete_data_from_bigquery_table(bq_project_id, bq_dataset, bq_table, month)

#     client = bigquery.Client()
#     table_id = f"{bq_project_id}.{bq_dataset}.{bq_table}"

#     job_config = bigquery.job.LoadJobConfig()
#     job_config.autodetect=True
#     job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

#     job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
#     job.result()  # Wait for the job to complete.

#     table = client.get_table(table_id)  # Make an API request.
#     print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")

# #data ingestion to gcs bucket

# def data_ingestion_to_gcs(bucket_name):

#     storage_client = storage.Client()
#     bucket = storage_client.bucket(bucket_name)

#     df = pd.DataFrame()
#     df['year_month']= (current_date.replace(day=1) - timedelta(days=1)).strftime('%Y-%m')

# #intermediate file created in path

#     inter_file_path = 'profit_and_loss/intermediate_file/' + str(currentYear) + "/" + str(currentMonth) + "/data_ingestion.csv"
#     bucket.blob(inter_file_path).upload_from_string(df.to_csv(), 'text/csv')

# #allocation level selection

def add_allocation_level(sales, scm):
    scm['allocation level'] = 'Family level'

#loop through each row in scm

    for i, row in scm.iterrows():
        article = row['Article Code']
        brick = row['Brick']
        
#check if article is in sales

        if article in sales['Article'].tolist():
            scm.at[i, 'allocation level'] = 'Article level'
            
#if article not in sales, check if brick is in sales
        
        elif brick in sales['MH Brick'].tolist():
            scm.at[i, 'allocation level'] = 'Brick level'

    return scm


####Calculation starts in file data from here:


# def calculate_sales_input_file(bucket_name, sales_file_name, lov_channel_master_file_name, lov_family_master_file_name):

    # storage_client = storage.Client()
    # bucket = storage_client.bucket(bucket_name)
    # blob = bucket.blob(sales_file_name)
    # data = blob.download_as_bytes()

    # df_sales = pd.read_excel(io.BytesIO(data))

#1read sales file
df_sales = pd.read_excel('/Users/nitin14.patil/Documents/PnL_python_work/pnl_data_files/June 2023 Sales Data.xlsx')
    
df_sales['Article']= df_sales['Article'].astype(str)
df_sales['Article'] = df_sales['Article'].str.lstrip('0')


#rename columns of sales dataframe

df_sales = df_sales.rename(columns={'Family': 'Family Sales','Cost':'COGS'})

    # storage_client = storage.Client()
    # bucket = storage_client.bucket(bucket_name)
    # blob = bucket.blob(lov_channel_master_file_name)
    # data = blob.download_as_bytes()

    # dist_channel_master = pd.read_excel(io.BytesIO(data))

#2read lov channel master file
dist_channel_master = pd.read_excel('/Users/nitin14.patil/Documents/PnL_python_work/pnl_data_files/lov_channel_master.xlsx')
#rename columns of sales dataframe    


dist_channel_master = dist_channel_master.loc[:, ['Name','Distribution Channel', 'Format', 'Channel Type']]
dist_channel_master = dist_channel_master.rename(columns={'Name': 'Channel'})




# storage_client = storage.Client()
# bucket = storage_client.bucket(bucket_name)
# blob = bucket.blob(lov_family_master_file_name)
# data = blob.download_as_bytes()

# fam_category_master = pd.read_excel(io.BytesIO(data))
    
    
#3read lov family master file
fam_category_master = pd.read_excel('/Users/nitin14.patil/Documents/PnL_python_work/pnl_data_files/lov_family_master.xlsx')
    
fam_category_master = fam_category_master.loc[:, ['Family','Family.1', 'Category']]

df_sales_channel = pd.merge(df_sales, dist_channel_master,  how='left', left_on=['Distribution Channel','Flag','Format'], right_on=['Distribution Channel','Channel Type','Format'])

df_sales_channel['Channel2'] = np.where(df_sales_channel.Channel == 'GT', 'GT', 'RRL')

df_sales_final = pd.merge(df_sales_channel, fam_category_master, left_on='MH Family', right_on='Family', how='left')
    


#intermediate file creation
# sales_file_path = r'profit_and_loss/intermediate_file/' + str(currentYear) + "/" + str(currentMonth) + "/sales_output.csv"

df_sales_final.to_csv('/Users/nitin14.patil/Documents/PnL_python_work/pnl_data_files/intermediate_file/june_sales_output.csv')

# bucket.blob(sales_file_path).upload_from_string(df_sales_final.to_csv(), 'text/csv')
    

# def calculate_scm_input_file(bucket_name, scm_file_name):
    
    # storage_client = storage.Client()
    # bucket = storage_client.bucket(bucket_name)
    # blob = bucket.blob(scm_file_name)
    # data = blob.download_as_bytes()

    # scm = pd.read_excel(io.BytesIO(data))


scm = pd.read_excel('/Users/nitin14.patil/Documents/PnL_python_work/pnl_data_files/scm.xlsx')
scm_file_name = pd.read_excel('/Users/nitin14.patil/Documents/PnL_python_work/pnl_data_files/scm.xlsx')

scm = scm[['Article Code','Article Name','Brick','Family','WH COST','SO Freight','Liquidation Sales','ResQ Rent','STO Cost GT','STO Cost RRL']]
scm['Article Code']= scm['Article Code'].astype(str)
scm['Article Code'] = scm['Article Code'].replace(r'\.0$', '', regex=True)

# sales_file_name = 'profit_and_loss/intermediate_file/' + str(currentYear) + "/" + str(currentMonth) + "/sales_output.csv"

# storage_client = storage.Client()
# bucket = storage_client.bucket(bucket_name)
# blob = bucket.blob(sales_file_name)
# data = blob.download_as_bytes()

sales = pd.read_csv('/Users/nitin14.patil/Documents/PnL_python_work/pnl_data_files/intermediate_file/june_sales_output.csv')
sales['Article'] = sales['Article'].astype(str)

scm = add_allocation_level(sales, scm)
    
sales_gt = sales[sales['Channel2'] == 'GT']
sales_rrl = sales[sales['Channel2'] == 'RRL']
    
grouped_df_article = sales.groupby(['Article'])['Net Sales With Tax'].sum()
grouped_df_brick = sales.groupby(['MH Brick'])['Net Sales With Tax'].sum()
grouped_df_family = sales.groupby(['MH Family'])['Net Sales With Tax'].sum()

grouped_df_article_gt = sales_gt.groupby(['Article'])['Net Sales With Tax'].sum()
grouped_df_article_rrl = sales_rrl.groupby(['Article'])['Net Sales With Tax'].sum()

scm_article = scm[scm['allocation level'] == 'Article level']
df_scm_sales_article = pd.merge(scm_article,grouped_df_article, how='left', left_on=['Article Code'], right_on = ['Article'])

scm_brick = scm[scm['allocation level'] == 'Brick level']
scm_brick_wh = scm_brick.groupby(['Brick'])['WH COST'].sum().reset_index()
df_scm_sales_brick = pd.merge(scm_brick_wh,grouped_df_brick, how='left', left_on=['Brick'], right_on = ['MH Brick'])

scm_brick_resq = scm_brick.groupby(['Brick'])['ResQ Rent'].sum().reset_index()
df_scm_sales_brick_resq = pd.merge(scm_brick_resq,grouped_df_brick, how='left', left_on=['Brick'], right_on = ['MH Brick'])

df_scm_sales_cost = pd.merge(scm,grouped_df_article, how='left', left_on=['Article Code'], right_on = ['Article'])
df_scm_sales_cost_gt = pd.merge(scm,grouped_df_article_gt, how='left', left_on=['Article Code'], right_on = ['Article'])
df_scm_sales_cost_rrl = pd.merge(scm,grouped_df_article_rrl, how='left', left_on=['Article Code'], right_on = ['Article'])

scm_family = scm[scm['allocation level'] == 'Family level']
scm_family_wh = scm_family.groupby(['Family'])['WH COST'].sum().reset_index()
df_scm_sales_family = pd.merge(scm_family_wh,grouped_df_family,  how='left', left_on=['Family'], right_on = ['MH Family'])

scm_family_resq = scm_family.groupby(['Family'])['ResQ Rent'].sum().reset_index()
df_scm_sales_family_resq = pd.merge(scm_family_resq,grouped_df_family,  how='left', left_on=['Family'], right_on = ['MH Family'])

#     df_scm_sales = df_scm_sales.dropna(subset=['allocation level'])

df_scm_sales_article['WH Allocation Article'] = np.where(df_scm_sales_article['allocation level'] == 'Article level', 
                                   (df_scm_sales_article['WH COST'] / math.pow(10, 7)) / df_scm_sales_article['Net Sales With Tax'],0)
df_scm_sales_article['ResQ Allocation Article'] = np.where(df_scm_sales_article['allocation level'] == 'Article level', 
                                   (df_scm_sales_article['ResQ Rent'] / math.pow(10, 7)) / df_scm_sales_article['Net Sales With Tax'],0)

#Resume



df_scm_sales_brick['WH Allocation Brick'] = (df_scm_sales_brick['WH COST'] / math.pow(10, 7)) / df_scm_sales_brick['Net Sales With Tax']
df_scm_sales_brick_resq['ResQ Allocation Brick'] = (df_scm_sales_brick_resq['ResQ Rent'] / math.pow(10, 7)) / df_scm_sales_brick_resq['Net Sales With Tax']

df_scm_sales_family['WH Allocation Family'] = (df_scm_sales_family['WH COST'] / math.pow(10, 7)) / df_scm_sales_family['Net Sales With Tax']


df_scm_sales_family_resq['ResQ Allocation Family'] = (df_scm_sales_family_resq['ResQ Rent'] / math.pow(10, 7)) / df_scm_sales_family_resq['Net Sales With Tax']

df_scm_sales_cost_gt['SO Freight'] = pd.to_numeric(df_scm_sales_cost_gt['SO Freight'], errors='coerce')

df_scm_sales_cost_gt['SO Allocation'] = (df_scm_sales_cost_gt['SO Freight'] / math.pow(10, 7)) / df_scm_sales_cost_gt['Net Sales With Tax']


df_scm_sales_cost_gt['Liquidation Sales'] = pd.to_numeric(df_scm_sales_cost_gt['Liquidation Sales'], errors='coerce')
df_scm_sales_cost_gt['Liquidation Allocation'] = (df_scm_sales_cost_gt['Liquidation Sales'] / math.pow(10, 7)) / df_scm_sales_cost_gt['Net Sales With Tax']



df_scm_sales_cost_gt['STO Cost GT'] = pd.to_numeric(df_scm_sales_cost_gt['STO Cost GT'], errors='coerce')
df_scm_sales_cost_gt['STO GT Allocation'] = (df_scm_sales_cost_gt['STO Cost GT'] / math.pow(10, 7)) / df_scm_sales_cost_gt['Net Sales With Tax']




df_scm_sales_cost_rrl['STO RRL Allocation'] = (df_scm_sales_cost_rrl['STO Cost RRL'] / math.pow(10, 7)) / df_scm_sales_cost_rrl['Net Sales With Tax']

# return df_scm_sales_article, df_scm_sales_brick,df_scm_sales_brick_resq,df_scm_sales_cost_gt,df_scm_sales_cost_rrl,df_scm_sales_family,df_scm_sales_family_resq


# def calculate_sales_scm_columns(bucket_name, scm_file_name):

df1, df2, df3, df4, df5, df6, df7 = scm_file_name

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(sales_file_name)
    data = blob.download_as_bytes()

    sales = pd.read_csv(io.BytesIO(data))
    sales['Article'] = sales['Article'].astype(str)
    
    df1 = df1.loc[:,['Article Code','WH Allocation Article','ResQ Allocation Article']].fillna(0)
    df2 = df2.loc[:,['Brick','WH Allocation Brick']].fillna(0)
    df3 = df3.loc[:,['Brick','ResQ Allocation Brick']].fillna(0)
    df4 = df4.loc[:,['Article Code','SO Allocation','Liquidation Allocation','STO GT Allocation']].fillna(0)
    df5 = df5.loc[:,['Article Code','STO RRL Allocation']].fillna(0)
    df6 = df6.loc[:,['Family','WH Allocation Family']].fillna(0)
    df7 = df7.loc[:,['Family','ResQ Allocation Family']].fillna(0)
    
    df_scm = pd.merge(sales,df1, how='left', left_on=['Article'], right_on = ['Article Code'])
    df_scm = pd.merge(df_scm,df2, how='left', left_on=['MH Brick'], right_on = ['Brick'])
    df_scm = pd.merge(df_scm,df3, how='left', left_on=['MH Brick'], right_on = ['Brick'])
    df_scm = pd.merge(df_scm,df4, how='left', left_on=['Article'], right_on = ['Article Code'])
    df_scm = pd.merge(df_scm,df5, how='left', left_on=['Article'], right_on = ['Article Code'])
    df_scm = pd.merge(df_scm,df6, how='left', left_on=['MH Family'], right_on = ['Family'])
    df_scm = pd.merge(df_scm,df7, how='left', left_on=['MH Family'], right_on = ['Family'])
    
    df_scm = df_scm.loc[:,~df_scm.columns.str.endswith('_y')]
    df_scm = df_scm.loc[:,~df_scm.columns.str.endswith('_x')]
    
    df_scm['SO Allocation'] = np.where(df_scm['Channel2'] == 'GT', df_scm['SO Allocation'], 0)
    df_scm['Liquidation Allocation'] = np.where(df_scm['Channel2'] == 'GT', df_scm['Liquidation Allocation'], 0)
    df_scm['STO GT Allocation'] = np.where(df_scm['Channel2'] == 'GT', df_scm['STO GT Allocation'], 0)

    df_scm['STO RRL Allocation'] = np.where(df_scm['Channel2'] == 'RRL', df_scm['STO RRL Allocation'], 0)
    
    
    df_scm['SO Freight'] = np.where(df_scm['Channel2']=='GT',df_scm['SO Allocation']*df_scm['Net Sales With Tax'],0)    
    df_scm['WH Cost @Article level'] = df_scm['WH Allocation Article']*df_scm['Net Sales With Tax'] 
    df_scm['Resq Rent @Article'] = df_scm['ResQ Allocation Article']*df_scm['Net Sales With Tax']
    df_scm['Resq Rent @Brick'] = df_scm['ResQ Allocation Brick']*df_scm['Net Sales With Tax']
    df_scm['Resq Rent @Family'] = df_scm['ResQ Allocation Family']*df_scm['Net Sales With Tax']
    df_scm['Resq Rent'] = df_scm['Resq Rent @Article'] + df_scm['Resq Rent @Brick'] + df_scm['Resq Rent @Family']
    
    df_scm['STO & Other'] = np.where(df_scm['Channel2']=='GT',df_scm['STO GT Allocation']*df_scm['Net Sales With Tax'],df_scm['STO RRL Allocation']*df_scm['Net Sales With Tax'])
    df_scm['WH Cost @Family'] = df_scm['WH Allocation Family']*df_scm['Net Sales With Tax']
    df_scm['WH Cost @Brick'] = df_scm['WH Allocation Brick']*df_scm['Net Sales With Tax']
    df_scm['WH Cost @Brick & Brand'] = df_scm['WH Cost @Family'] + df_scm['WH Cost @Brick']    
    
    df_scm['SO Freight'] = df_scm['SO Freight'].apply(lambda x: '{:.15f}'.format(x))
    df_scm['WH Cost @Article level'] = df_scm['WH Cost @Article level'].apply(lambda x: '{:.15f}'.format(x))
    df_scm['Resq Rent'] = df_scm['Resq Rent'].apply(lambda x: '{:.15f}'.format(x))
    df_scm['STO & Other'] = df_scm['STO & Other'].apply(lambda x: '{:.15f}'.format(x))
    df_scm['WH Cost @Brick & Brand'] = df_scm['WH Cost @Brick & Brand'].apply(lambda x: '{:.15f}'.format(x))
    
    df_scm = df_scm.drop(['Article Code','WH Cost @Family','WH Cost @Brick','Resq Rent @Article','Resq Rent @Brick','Resq Rent @Family'], axis=1)
    df_scm = df_scm.rename(columns={'MH Brick' : 'Brick'})
    df_scm_final = df_scm.loc[:,~df_scm.columns.duplicated()].copy()
    
    # return df_scm_final
    scm_file_path = 'profit_and_loss/intermediate_file/' + str(currentYear) + "/" + str(currentMonth) + "/scm_output.csv"
    bucket.blob(scm_file_path).upload_from_string(df_scm_final.to_csv(), 'text/csv')


def calculate_percentage_input_file(bucket_name, per_file_name):

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(per_file_name)
    data = blob.download_as_bytes()

    df_per = pd.read_excel(io.BytesIO(data))
    df_per.drop(['Month_Year'], axis=1, inplace=True)
    df_per = df_per.drop_duplicates()
    
    df_per.drop(['LinkKey'], axis=1, inplace=True)
    df_per = df_per.fillna(0.00)
    df_per = df_per.rename(columns={'Defective & E Waste': 'Defective &  E Waste'})
    
    return df_per


def calculate_percentage_columns(bucket_name, per_file_name):
    
    scm_file_path = 'profit_and_loss/intermediate_file/' + str(currentYear) + "/" + str(currentMonth) + "/scm_output.csv"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(scm_file_path)
    data = blob.download_as_bytes()

    sales = pd.read_csv(io.BytesIO(data))
    
    sales['Channel'] = sales['Channel'].replace(['RD.in'], 'RD IN')
    sales['Channel'] = sales['Channel'].astype('str')
    sales = sales.rename(columns={'Brand': 'Brand_ID'})
    
    per = calculate_percentage_input_file(bucket_name, per_file_name)
    
    df_per_sales = pd.merge(sales, per, left_on = ['Article', 'Channel'], right_on = ['Article Code', 'Channel'], how='left')
    
    df_per_sales['Bad Debt'] = (df_per_sales['Bad Debt'] * df_per_sales['Net Sales WOT']).fillna(0)
    df_per_sales['CD'] = np.where(df_per_sales['Customer Group'] == 'P6', 0, df_per_sales['CD'] * df_per_sales['Net Sales With Tax'])
    df_per_sales['Defective &  E Waste'] = (df_per_sales['Defective &  E Waste'] * df_per_sales['Net Sales WOT']).fillna(0)
    df_per_sales['Display'] = np.where(df_per_sales['Customer Group'] == 'P6', 0, df_per_sales['Display'] * df_per_sales['Net Sales WOT'])
    df_per_sales['EOL'] = (df_per_sales['EOL'] * df_per_sales['Net Sales WOT']).fillna(0)
    df_per_sales['Promo'] = np.where(df_per_sales['Customer Group'] == 'P6', 0, df_per_sales['Promo'] * df_per_sales['Net Sales WOT'])
    df_per_sales['Trade Discount'] = np.where(df_per_sales['Customer Group'] == 'P6', 0, df_per_sales['Trade Discount'] * df_per_sales['Net Sales WOT'])
    df_per_sales['Warranty'] = (df_per_sales['Warranty'] * df_per_sales['Net Sales WOT']).fillna(0)
    
    
    # return df_per_sales
    per_file_path = 'profit_and_loss/intermediate_file/' + str(currentYear) + "/" + str(currentMonth) + "/percentage_output.csv"
    bucket.blob(per_file_path).upload_from_string(df_per_sales.to_csv(), 'text/csv')


def calculate_service_cost_allocation_columns(bucket_name, sca_file_name, lov_family_master_file_name):
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(sca_file_name)
    data = blob.download_as_bytes()

    df_sca = pd.read_excel(io.BytesIO(data)) 
    
    df_sca = df_sca[['Month_Year', 'Family', 'Brand','Installation Cost','Per Call Cost','Transporation Cost','OpEx Cost','Other Exp','Part Consumption Cost']]

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(lov_family_master_file_name)
    data = blob.download_as_bytes()

    fam_category_master = pd.read_excel(io.BytesIO(data))
    fam_category_master = fam_category_master.loc[:, ['Family','Family.1', 'Category']]
    
    df_sca = pd.merge(df_sca, fam_category_master, left_on = df_sca['Family'].str.lower()
                      , right_on = fam_category_master['Family'].str.lower(), how='left')
    df_sca = df_sca.loc[:,~df_sca.columns.str.endswith('_y')]
    df_sca = df_sca.loc[:,~df_sca.columns.str.startswith('key_')]
    
    suffix='_x'
    df_sca.columns = df_sca.columns.str.rstrip(suffix)
    
    df_sca = df_sca[['Month_Year', 'Family.1','Category', 'Brand','Installation Cost','Per Call Cost','Transporation Cost','OpEx Cost','Other Exp','Part Consumption Cost']]
    df_sca = df_sca.rename(columns={'Family.1': 'Family'})
    
    df_sca

    # df_sca['Brand-Family'] = df_sca["Brand"]+df_sca["Family"]
    
    sales_file_name = 'profit_and_loss/intermediate_file/' + str(currentYear) + "/" + str(currentMonth) + "/sales_output.csv"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(sales_file_name)
    data = blob.download_as_bytes()

    sales = pd.read_csv(io.BytesIO(data))
    # sales['Net Sales With Tax'] = sales['Net Sales With Tax'].astype(float)
    # sales['Article'] = sales['Article'].astype(int)
    
    var = sales.groupby(['Brand', 'Family.1'])['Net Sales With Tax'].sum()
    df_sales = pd.DataFrame({'Brand': var.index.get_level_values(0),
                             'Family': var.index.get_level_values(1),
                             'Net Sales With Tax': var.values})
    df_sales['Net Sales With Tax'] = df_sales['Net Sales With Tax']*math.pow(10,7)
    df_sales = df_sales.rename(columns={'Net Sales With Ta': 'Net Sales With Tax'})
    
    sca_per = pd.merge(df_sca, df_sales, left_on=[df_sca['Brand'].str.lower(), df_sca['Family'].str.lower()]
                       , right_on=[df_sales['Brand'].str.lower(), df_sales['Family'].str.lower()], how = 'left')
    
    sca_per = sca_per.loc[:,~sca_per.columns.str.endswith('_y')]
    sca_per = sca_per.loc[:,~sca_per.columns.str.startswith('key_')]
    
#     suffix='_x'
#     df_sales_scm_per_sca.columns = df_sales_scm_per_sca.columns.str.rstrip(suffix)
    
    sca_per = sca_per.rename(columns={'Family_x': 'Family','Brand_x':'Brand', 'Month_Year':'Month'})
    
    
    sca_per['Installation Cost %'] = (sca_per['Installation Cost'] / sca_per['Net Sales With Tax']).fillna(0)
    sca_per['Per Call Cost %'] = (sca_per['Per Call Cost'] / sca_per['Net Sales With Tax']).fillna(0)
    sca_per['Transporation Cost %'] = (sca_per['Transporation Cost'] / sca_per['Net Sales With Tax']).fillna(0)
    sca_per['OpEx Cost %'] = (sca_per['OpEx Cost'] / sca_per['Net Sales With Tax']).fillna(0)
    sca_per['Other Exp %'] = (sca_per['Other Exp'] / sca_per['Net Sales With Tax']).fillna(0)
    sca_per['Part Consumption Cost %'] = (sca_per['Part Consumption Cost'] / sca_per['Net Sales With Tax']).fillna(0)

    sca_per = sca_per[['Month', 'Family', 'Brand','Installation Cost %','Per Call Cost %','Transporation Cost %','OpEx Cost %'
                  , 'Other Exp %', 'Part Consumption Cost %']]

    sca_inter_file_path = 'profit_and_loss/intermediate_file/' + str(currentYear) + "/" + str(currentMonth) + "/sca_intermediate_file.csv"
    bucket.blob(sca_inter_file_path).upload_from_string(sca_per.to_csv(), 'text/csv')
    
    return sca_per


def merge_sales_scm_per_sca(bucket_name, sca_file_name, lov_family_master_file_name):
    
    
    # df_sales_scm_per = calculate_percentage_columns(bucket_name, per_file_name, scm_file_name, sales_file_name, lov_file_name)
    per_file_name = 'profit_and_loss/intermediate_file/' + str(currentYear) + "/" + str(currentMonth) + "/percentage_output.csv"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(per_file_name)
    data = blob.download_as_bytes()

    df_sales_scm_per = pd.read_csv(io.BytesIO(data))

    df_sca = calculate_service_cost_allocation_columns(bucket_name, sca_file_name, lov_family_master_file_name)
    
    df_sales_scm_per_sca = pd.merge(df_sales_scm_per, df_sca
                                , left_on=[df_sales_scm_per['Brand_ID'].str.lower(), df_sales_scm_per['Family.1'].str.lower()]
                                , right_on=[df_sca['Brand'].str.lower(), df_sca['Family'].str.lower()] , how='left')

    df_sales_scm_per_sca = df_sales_scm_per_sca.loc[:,~df_sales_scm_per_sca.columns.str.endswith('_y')]
    df_sales_scm_per_sca = df_sales_scm_per_sca.loc[:,~df_sales_scm_per_sca.columns.str.startswith('key_')]
    df_sales_scm_per_sca = df_sales_scm_per_sca.rename(columns={'Family_x': 'Family'})
    df_sales_scm_per_sca = df_sales_scm_per_sca.drop(['Month', 'Brand'], axis=1)

    df_sales_scm_per_sca['Installation Cost'] = (df_sales_scm_per_sca['Installation Cost %'] *df_sales_scm_per_sca['Net Sales With Tax']).fillna(0).apply(lambda x: '{:.15f}'.format(x))
    df_sales_scm_per_sca['Per Call Cost'] = (df_sales_scm_per_sca['Per Call Cost %'] *df_sales_scm_per_sca['Net Sales With Tax']).fillna(0).apply(lambda x: '{:.15f}'.format(x))
    df_sales_scm_per_sca['Transporation Cost'] = (df_sales_scm_per_sca['Transporation Cost %'] *df_sales_scm_per_sca['Net Sales With Tax']).fillna(0).apply(lambda x: '{:.15f}'.format(x))
    df_sales_scm_per_sca['OpEx Cost'] = (df_sales_scm_per_sca['OpEx Cost %'] *df_sales_scm_per_sca['Net Sales With Tax']).fillna(0).apply(lambda x: '{:.15f}'.format(x))
    df_sales_scm_per_sca['Other Exp'] = (df_sales_scm_per_sca['Other Exp %'] *df_sales_scm_per_sca['Net Sales With Tax']).fillna(0).apply(lambda x: '{:.15f}'.format(x))
    df_sales_scm_per_sca['Part Consumption Cost'] = (df_sales_scm_per_sca['Part Consumption Cost %'] *df_sales_scm_per_sca['Net Sales With Tax']).fillna(0).apply(lambda x: '{:.15f}'.format(x))

    df_sales_scm_per_sca = df_sales_scm_per_sca.drop(['Installation Cost %', 'Per Call Cost %','Transporation Cost %','OpEx Cost %','Other Exp %','Part Consumption Cost %'], axis=1)
    
    # return df_sales_scm_per_sca
    sca_file_path = 'profit_and_loss/intermediate_file/' + str(currentYear) + "/" + str(currentMonth) + "/service_cost_output.csv"
    bucket.blob(sca_file_path).upload_from_string(df_sales_scm_per_sca.to_csv(), 'text/csv')


def opex_cost_estimation_input_files(bucket_name, opex_file_name, brand_level_input_file_name):
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(brand_level_input_file_name)
    data = blob.download_as_bytes()

    marketing_base_file = pd.read_excel(io.BytesIO(data),usecols=[0,1,4])
    marketing_base_file = marketing_base_file.rename(columns={'Month_Year' : 'Month'})

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(opex_file_name)
    data = blob.download_as_bytes()

    staff_base_file = pd.read_excel(io.BytesIO(data),usecols=[0,1])
    staff_base_file = staff_base_file.rename(columns={'Month_Year' : 'Month'})

    other_expenses_base_file = pd.read_excel(io.BytesIO(data), usecols=[0,2])
    other_expenses_base_file = other_expenses_base_file.rename(columns={'Month_Year' : 'Month'})
    
    sales_file_name = 'profit_and_loss/intermediate_file/' + str(currentYear) + "/" + str(currentMonth) + "/sales_output.csv"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(sales_file_name)
    data = blob.download_as_bytes()

    sales = pd.read_csv(io.BytesIO(data))
    # sales['Net Sales With Tax'] = sales['Net Sales With Tax'].astype(float)
    # sales['Article'] = sales['Article'].astype(int)
    
    grouped_df_1 = sales.groupby(['Brand'])['Net Sales With Tax'].sum()
    
    marketing_per = pd.merge(marketing_base_file, grouped_df_1, left_on=['Brand'], right_on = ['Brand'])
    marketing_per['Marketing_per'] = marketing_per['Marketing_Amount']/marketing_per['Net Sales With Tax']
    marketing_per = marketing_per.loc[:,['Brand','Marketing_per']]
    
    total_gross_sales = {'Net Sales With Tax': [sales['Net Sales With Tax'].sum()]}
    grouped_df_2 = pd.DataFrame(total_gross_sales)
    
    opex_per = pd.merge(staff_base_file, grouped_df_2, left_index=True, right_index=True, how='outer')

    opex_per_final = pd.merge(opex_per, other_expenses_base_file, left_index=True, right_index=True, how='outer')

    opex_per_final = opex_per_final.loc[:, ['Month_x','Net Sales With Tax','Staff_Amount','Other_expense_Amount']]
    opex_per_final = opex_per_final.rename(columns={'Month_x': 'Month_Year'})

    opex_per_final['staff_cost_per'] = opex_per_final['Staff_Amount']/opex_per_final['Net Sales With Tax']
    opex_per_final['other_expenses'] = opex_per_final['Other_expense_Amount']/opex_per_final['Net Sales With Tax']
    
    opex_per = opex_per_final.loc[:,['other_expenses','staff_cost_per']]

    opex_inter_file_path = 'profit_and_loss/intermediate_file/' + str(currentYear) + "/" + str(currentMonth) + "/opex_intermediate_file.csv"
    bucket.blob(opex_inter_file_path).upload_from_string(opex_per.to_csv(), 'text/csv') 
    
    return marketing_per,opex_per 


def merge_sales_scm_per_sca_opex(bucket_name, opex_file_name, brand_level_input_file_name):

    # df_per_sales_scm_sca = merge_sales_scm_per_sca(bucket_name, sca_file_name, per_file_name, scm_file_name, sales_file_name, lov_file_name)
    sca_file_name = 'profit_and_loss/intermediate_file/' + str(currentYear) + "/" + str(currentMonth) + "/service_cost_output.csv"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(sca_file_name)
    data = blob.download_as_bytes()

    df_per_sales_scm_sca = pd.read_csv(io.BytesIO(data))
    
    df_mark,df_opex = opex_cost_estimation_input_files(bucket_name, opex_file_name, brand_level_input_file_name)
    
    df_sales_scm_per_sca_opex = pd.merge(df_per_sales_scm_sca, df_mark, left_on=['Brand_ID'], right_on = 'Brand', how='left')
    
    df_sales_scm_per_sca_opex['marketing_exp'] = df_sales_scm_per_sca_opex['Net Sales With Tax']*df_sales_scm_per_sca_opex['Marketing_per']
    df_sales_scm_per_sca_opex['staff_cost'] = df_sales_scm_per_sca_opex['Net Sales With Tax']*df_opex['staff_cost_per'][0]
    df_sales_scm_per_sca_opex['other_expenses'] = df_sales_scm_per_sca_opex['Net Sales With Tax']*df_opex['other_expenses'][0]
    
    df_sales_scm_per_sca_opex = df_sales_scm_per_sca_opex.drop(['Brand','Marketing_per'], axis=1)
    
    # return df_sales_scm_per_sca_opex 
    opex_file_path = 'profit_and_loss/intermediate_file/' + str(currentYear) + "/" + str(currentMonth) + "/opex_output.csv"
    bucket.blob(opex_file_path).upload_from_string(df_sales_scm_per_sca_opex.to_csv(), 'text/csv')


def calculate_royalty_and_mdf_columns(bucket_name, brand_level_input_file_name):
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(brand_level_input_file_name)
    data = blob.download_as_bytes()

    df_royalty = pd.read_excel(io.BytesIO(data),usecols=[0,1,2])
    df_royalty = df_royalty.rename(columns={'Month_Year' : 'Month'})
    df_mdf = pd.read_excel(io.BytesIO(data), usecols=[0,1,3])
    df_mdf = df_mdf.rename(columns={'Month_Year' : 'Month'})

    opex_file_name = 'profit_and_loss/intermediate_file/' + str(currentYear) + "/" + str(currentMonth) + "/opex_output.csv"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(opex_file_name)
    data = blob.download_as_bytes()

    df_sales_scm_per_sca_opex = pd.read_csv(io.BytesIO(data))
    brands = df_sales_scm_per_sca_opex['Brand_ID'].unique().tolist()
    
    royalty = df_royalty.groupby('Brand')['Royalty_percentage'].apply(lambda x: x.tolist())
    royalty_dict = royalty.to_dict()
    for item in brands:
        if item not in royalty_dict:
            royalty_dict[item] = [0.0]
    royalty_df = pd.DataFrame(royalty_dict)
    
    mdf = df_mdf.groupby('Brand')['MDF_percentage'].apply(lambda x: x.tolist())
    mdf_dict = mdf.to_dict()
    for item in brands:
        if item not in mdf_dict:
            mdf_dict[item] = [0.0]
    mdf_df = pd.DataFrame(mdf_dict)
    
    duplicated_cols = df_sales_scm_per_sca_opex.columns[df_sales_scm_per_sca_opex.columns.duplicated()]
    unique_cols = df_sales_scm_per_sca_opex.columns.drop_duplicates(keep='first')
    df_royalty_mdf = df_sales_scm_per_sca_opex [unique_cols]
    
    df_royalty_mdf = df_royalty_mdf.rename(columns={'Net Sales With Ta': 'Net Sales With Tax'})
    
    df_royalty_mdf['royalty'] = df_royalty_mdf.apply(
        lambda row: row['COGS'] * royalty_df.loc[0, row['Brand_ID']] if row['Brand_ID'] == 'BPL' 
        else (row['Net Sales WOT'] - sum(row[col] for col in ['Trade Discount', 'CD', 'Promo', 'Display'])) * royalty_df.loc[0, row['Brand_ID']]
        , axis=1)
    
    df_royalty_mdf['mdf'] = df_royalty_mdf.apply(
        lambda row: row['COGS'] * mdf_df.loc[0, row['Brand_ID']] if row['Brand_ID'] == 'BPL' 
        else (row['Net Sales WOT'] - sum(row[col] for col in ['Trade Discount', 'CD', 'Promo', 'Display'])) * mdf_df.loc[0, row['Brand_ID']]
        , axis=1)
    
    df_royalty_mdf['mdf'] = df_royalty_mdf['mdf']*-1
    # return df_royalty_mdf
    royalty_mdf_file_path = 'profit_and_loss/intermediate_file/' + str(currentYear) + "/" + str(currentMonth) + "/royalty_mdf_output.csv"
    bucket.blob(royalty_mdf_file_path).upload_from_string(df_royalty_mdf.to_csv(), 'text/csv')


def calculate_FOC_input_files(bucket_name, foc_file_name):

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(foc_file_name)
    data = blob.download_as_bytes()

    df_article = pd.read_excel(io.BytesIO(data),usecols=[1,4,5])
    df_article = df_article.rename(columns={'FOC ': 'FOC','Article Code':'Article'})
    
    df_brick = pd.read_excel(io.BytesIO(data),usecols=[2,4,5])
    df_brick = df_brick.rename(columns={'FOC ': 'FOC','Article Code':'Article'})
    
    df_family = pd.read_excel(io.BytesIO(data),usecols=[3,4,5])
    df_family = df_family.rename(columns={'FOC ': 'FOC','Article Code':'Article'})
    
    df_article = df_article[df_article['Article'].notna()]
    df_brick['Brick'] = df_brick['Brick'].fillna('NA')
    df_family['Family']= df_family['Family'].fillna('NA')

    df_foc_pivot_article = df_article.pivot_table(values='FOC', index='Article', columns='Type',aggfunc='sum')
    df_foc_pivot_article = df_foc_pivot_article.fillna(0)
    df_foc_pivot_article['Channel'] = 'GT'
    df_foc_pivot_article = df_foc_pivot_article.reset_index()
    df_foc_pivot_article['Article']=df_foc_pivot_article['Article'].astype('int').astype('str')
    
    df_foc_pivot_brick = df_brick.pivot_table(values='FOC', index='Brick', columns='Type',aggfunc='sum')
    df_foc_pivot_brick = df_foc_pivot_brick.fillna(0)
    df_foc_pivot_brick['Channel'] = 'GT'
    df_foc_pivot_brick = df_foc_pivot_brick.reset_index()
    df_foc_pivot_brick['Brick']=df_foc_pivot_brick['Brick'].astype('str')
    
    df_foc_pivot_family = df_family.pivot_table(values='FOC', index='Family', columns='Type',aggfunc='sum')
    df_foc_pivot_family = df_foc_pivot_family.fillna(0)
    df_foc_pivot_family['Channel'] = 'GT'
    df_foc_pivot_family = df_foc_pivot_family.reset_index()
    df_foc_pivot_family['Family']=df_foc_pivot_family['Family'].astype('str')

    sales_file_name = 'profit_and_loss/intermediate_file/' + str(currentYear) + "/" + str(currentMonth) + "/sales_output.csv"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(sales_file_name)
    data = blob.download_as_bytes()
    
    sales = pd.read_csv(io.BytesIO(data))
    sales['Article'] = sales['Article'].astype(str)
    
    var = sales.groupby(['Article', 'Channel2'])['Net Sales With Tax'].sum()
    df_sales = pd.DataFrame({'Article': var.index.get_level_values(0),
                             'Channel': var.index.get_level_values(1),
                             'Net Sales With Tax': var.values})
    df_sales['Net Sales With Tax'] = df_sales['Net Sales With Tax']*math.pow(10,7)

    df_foc_final_article = pd.merge(df_foc_pivot_article, df_sales, how ='left', on = ['Article','Channel'])
    df_foc_final_article['foc_reversal_article_per'] = df_foc_final_article['Reversal']/df_foc_final_article['Net Sales With Tax']
    df_foc_final_article['foc_provision_article_per'] = df_foc_final_article['Provision']/df_foc_final_article['Net Sales With Tax']
 
    var = sales.groupby(['MH Brick', 'Channel2'])['Net Sales With Tax'].sum()
    df_sales = pd.DataFrame({'Brick': var.index.get_level_values(0),
                             'Channel': var.index.get_level_values(1),
                             'Net Sales With Tax': var.values})
    df_sales['Net Sales With Tax'] = df_sales['Net Sales With Tax']*math.pow(10,7)

    df_foc_final_brick = pd.merge(df_foc_pivot_brick, df_sales, how ='left', on = ['Brick','Channel'])
    df_foc_final_brick['foc_reversal_brick_per'] = df_foc_final_brick['Reversal']/df_foc_final_brick['Net Sales With Tax']
    df_foc_final_brick['foc_provision_brick_per'] = df_foc_final_brick['Provision']/df_foc_final_brick['Net Sales With Tax']
    
    var = sales.groupby(['Family.1', 'Channel2'])['Net Sales With Tax'].sum()
    df_sales = pd.DataFrame({'Family.1': var.index.get_level_values(0),
                             'Channel': var.index.get_level_values(1),
                             'Net Sales With Tax': var.values})
    df_sales['Net Sales With Tax'] = df_sales['Net Sales With Tax']*math.pow(10,7)
    
    df_foc_final_family = pd.merge(df_foc_pivot_family, df_sales, how ='left', left_on = ['Family','Channel'], right_on = ['Family.1','Channel'])
    df_foc_final_family['foc_reversal_family_per'] = df_foc_final_family['Reversal']/df_foc_final_family['Net Sales With Tax']
    df_foc_final_family['foc_provision_family_per'] = df_foc_final_family['Provision']/df_foc_final_family['Net Sales With Tax']

    foc_inter_file_path_article = 'profit_and_loss/intermediate_file/' + str(currentYear) + "/" + str(currentMonth) + "/foc_intermediate_file_article.csv"
    bucket.blob(foc_inter_file_path_article).upload_from_string(df_foc_final_article.to_csv(), 'text/csv')

    foc_inter_file_path_brick = 'profit_and_loss/intermediate_file/' + str(currentYear) + "/" + str(currentMonth) + "/foc_intermediate_file_brick.csv"
    bucket.blob(foc_inter_file_path_brick).upload_from_string(df_foc_final_brick.to_csv(), 'text/csv')

    foc_inter_file_path_family = 'profit_and_loss/intermediate_file/' + str(currentYear) + "/" + str(currentMonth) + "/foc_intermediate_file_family.csv"
    bucket.blob(foc_inter_file_path_family).upload_from_string(df_foc_final_family.to_csv(), 'text/csv')
    
    return df_foc_final_article,df_foc_final_brick,df_foc_final_family


def merge_sales_scm_per_sca_opex_roy_mdf_foc(bucket_name, foc_file_name):

    royalty_mdf_file_name = 'profit_and_loss/intermediate_file/' + str(currentYear) + "/" + str(currentMonth) + "/royalty_mdf_output.csv"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(royalty_mdf_file_name)
    data = blob.download_as_bytes()

    df_sales_scm_per_sca_opex_roy_mdf = pd.read_csv(io.BytesIO(data))
    df_sales_scm_per_sca_opex_roy_mdf['Article'] = df_sales_scm_per_sca_opex_roy_mdf['Article'].astype(str)

    df_foc_article,df_foc_brick,df_foc_family = calculate_FOC_input_files(bucket_name, foc_file_name)
    
    df_sales_scm_per_sca_opex_roy_mdf_foc = pd.merge(df_sales_scm_per_sca_opex_roy_mdf, df_foc_article, left_on = ['Article','Channel2'], right_on = ['Article','Channel'], how='left')

    df_sales_scm_per_sca_opex_roy_mdf_foc = df_sales_scm_per_sca_opex_roy_mdf_foc.loc[:,~df_sales_scm_per_sca_opex_roy_mdf_foc.columns.str.endswith('_y')]
    df_sales_scm_per_sca_opex_roy_mdf_foc = df_sales_scm_per_sca_opex_roy_mdf_foc.rename(columns={'Net Sales With Tax_x': 'Net Sales With Tax', 'Channel_x' : 'Channel'})
    
    df_sales_scm_per_sca_opex_roy_mdf_foc['foc_reversal_article'] = (df_sales_scm_per_sca_opex_roy_mdf_foc['foc_reversal_article_per'] * df_sales_scm_per_sca_opex_roy_mdf_foc['Net Sales With Tax']).fillna(0)
    df_sales_scm_per_sca_opex_roy_mdf_foc['foc_provision_article'] = (df_sales_scm_per_sca_opex_roy_mdf_foc['foc_provision_article_per'] * df_sales_scm_per_sca_opex_roy_mdf_foc['Net Sales With Tax']).fillna(0)
    df_sales_scm_per_sca_opex_roy_mdf_foc['foc_total_article'] = (-1*((df_sales_scm_per_sca_opex_roy_mdf_foc['foc_provision_article'] + df_sales_scm_per_sca_opex_roy_mdf_foc['foc_reversal_article']))).fillna(0)
    
    df_sales_scm_per_sca_opex_roy_mdf_foc = pd.merge(df_sales_scm_per_sca_opex_roy_mdf_foc, df_foc_brick, left_on = ['Brick','Channel2'], right_on = ['Brick','Channel'], how='left')

    df_sales_scm_per_sca_opex_roy_mdf_foc = df_sales_scm_per_sca_opex_roy_mdf_foc.loc[:,~df_sales_scm_per_sca_opex_roy_mdf_foc.columns.str.endswith('_y')]
    df_sales_scm_per_sca_opex_roy_mdf_foc = df_sales_scm_per_sca_opex_roy_mdf_foc.rename(columns={'Net Sales With Tax_x': 'Net Sales With Tax', 'Channel_x' : 'Channel'})
    
    df_sales_scm_per_sca_opex_roy_mdf_foc['foc_reversal_brick'] = (df_sales_scm_per_sca_opex_roy_mdf_foc['foc_reversal_brick_per'] * df_sales_scm_per_sca_opex_roy_mdf_foc['Net Sales With Tax']).fillna(0)
    df_sales_scm_per_sca_opex_roy_mdf_foc['foc_provision_brick'] = (df_sales_scm_per_sca_opex_roy_mdf_foc['foc_provision_brick_per'] * df_sales_scm_per_sca_opex_roy_mdf_foc['Net Sales With Tax']).fillna(0)
    df_sales_scm_per_sca_opex_roy_mdf_foc['foc_total_brick'] = (-1*((df_sales_scm_per_sca_opex_roy_mdf_foc['foc_provision_brick'] + df_sales_scm_per_sca_opex_roy_mdf_foc['foc_reversal_brick']))).fillna(0)
    
    df_sales_scm_per_sca_opex_roy_mdf_foc = pd.merge(df_sales_scm_per_sca_opex_roy_mdf_foc, df_foc_family, left_on = ['Family.1','Channel2'], right_on = ['Family.1','Channel'], how='left')

    df_sales_scm_per_sca_opex_roy_mdf_foc = df_sales_scm_per_sca_opex_roy_mdf_foc.loc[:,~df_sales_scm_per_sca_opex_roy_mdf_foc.columns.str.endswith('_y')]
    df_sales_scm_per_sca_opex_roy_mdf_foc = df_sales_scm_per_sca_opex_roy_mdf_foc.rename(columns={'Net Sales With Tax_x': 'Net Sales With Tax', 'Channel_x' : 'Channel'})
    
    df_sales_scm_per_sca_opex_roy_mdf_foc['foc_reversal_family'] = (df_sales_scm_per_sca_opex_roy_mdf_foc['foc_reversal_family_per'] * df_sales_scm_per_sca_opex_roy_mdf_foc['Net Sales With Tax']).fillna(0)
    df_sales_scm_per_sca_opex_roy_mdf_foc['foc_provision_family'] = (df_sales_scm_per_sca_opex_roy_mdf_foc['foc_provision_family_per'] * df_sales_scm_per_sca_opex_roy_mdf_foc['Net Sales With Tax']).fillna(0)
    df_sales_scm_per_sca_opex_roy_mdf_foc['foc_total_family'] = (-1*((df_sales_scm_per_sca_opex_roy_mdf_foc['foc_provision_family'] + df_sales_scm_per_sca_opex_roy_mdf_foc['foc_reversal_family']))).fillna(0)
    
    df_sales_scm_per_sca_opex_roy_mdf_foc = df_sales_scm_per_sca_opex_roy_mdf_foc.loc[:, ~df_sales_scm_per_sca_opex_roy_mdf_foc.columns.duplicated(keep='first')]
    df_sales_scm_per_sca_opex_roy_mdf_foc = df_sales_scm_per_sca_opex_roy_mdf_foc.rename(columns={'Family_x' : 'Family'})
    df_sales_scm_per_sca_opex_roy_mdf_foc = df_sales_scm_per_sca_opex_roy_mdf_foc.drop(['Reversal','Provision','Provision_x','Reversal_x','foc_reversal_article_per','foc_provision_article_per','foc_reversal_brick_per','foc_provision_brick_per','foc_reversal_family_per','foc_provision_family_per'], axis=1)

    foc_file_path = 'profit_and_loss/intermediate_file/' + str(currentYear) + "/" + str(currentMonth) + "/foc_output.csv"
    bucket.blob(foc_file_path).upload_from_string(df_sales_scm_per_sca_opex_roy_mdf_foc.to_csv(), 'text/csv')



def calculate_subvention_input_files(bucket_name, subvention_file_name):

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(subvention_file_name)
    data = blob.download_as_bytes()

    df_subvention = pd.read_excel(io.BytesIO(data))
    df_subvention = df_subvention.loc[:, ['Family','Brand','Amount Financed','Subvention Cost']]
    
    sales_file_name = 'profit_and_loss/intermediate_file/' + str(currentYear) + "/" + str(currentMonth) + "/sales_output.csv"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(sales_file_name)
    data = blob.download_as_bytes()

    sales = pd.read_csv(io.BytesIO(data))
    # sales['Net Sales With Tax'] = sales['Net Sales With Tax'].astype(float)
    # sales['Article'] = sales['Article'].astype(int)
    
    var = sales.groupby(['Family', 'Brand'])['Net Sales With Tax'].sum()
    df_sales_final = pd.DataFrame({'Family': var.index.get_level_values(0),
                       'Brand': var.index.get_level_values(1),
                       'Net Sales With Tax': var.values})
    
    df_subvention_final = pd.merge(df_subvention, df_sales_final,on=['Family','Brand'], how='left')
    df_subvention_final['subvention_per'] = df_subvention_final['Subvention Cost']/df_subvention_final['Net Sales With Tax']

    subvention_inter_file_path = 'profit_and_loss/intermediate_file/' + str(currentYear) + "/" + str(currentMonth) + "/subvention_intermediate_file.csv"
    bucket.blob(subvention_inter_file_path).upload_from_string(df_subvention_final.to_csv(), 'text/csv')

    return df_subvention_final


def merge_sales_scm_per_sca_opex_roy_mdf_foc_sub(bucket_name, subvention_file_name):

    foc_file_name = 'profit_and_loss/intermediate_file/' + str(currentYear) + "/" + str(currentMonth) + "/foc_output.csv"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(foc_file_name)
    data = blob.download_as_bytes()

    df_sales_scm_per_sca_opex_roy_mdf_foc = pd.read_csv(io.BytesIO(data))
    
    df_sub = calculate_subvention_input_files(bucket_name, subvention_file_name)
    
    df_sales_scm_per_sca_opex_roy_mdf_foc_sub = pd.merge(df_sales_scm_per_sca_opex_roy_mdf_foc, df_sub
                                                     , left_on = [df_sales_scm_per_sca_opex_roy_mdf_foc['MH Family'].str.lower(),df_sales_scm_per_sca_opex_roy_mdf_foc['Brand_ID'].str.lower()]
                                                     , right_on = [df_sub['Family'].str.lower(),df_sub['Brand'].str.lower()]
                                                     , how='left')
    df_sales_scm_per_sca_opex_roy_mdf_foc_sub = df_sales_scm_per_sca_opex_roy_mdf_foc_sub.loc[:,~df_sales_scm_per_sca_opex_roy_mdf_foc_sub.columns.str.endswith('_y')]
    df_sales_scm_per_sca_opex_roy_mdf_foc_sub = df_sales_scm_per_sca_opex_roy_mdf_foc_sub.loc[:,~df_sales_scm_per_sca_opex_roy_mdf_foc_sub.columns.str.startswith('key_')]
    df_sales_scm_per_sca_opex_roy_mdf_foc_sub = df_sales_scm_per_sca_opex_roy_mdf_foc_sub.rename(columns={'Net Sales With Tax_x': 'Net Sales With Tax','Family_x':'Family'})

    df_sales_scm_per_sca_opex_roy_mdf_foc_sub['Subvention'] = df_sales_scm_per_sca_opex_roy_mdf_foc_sub['subvention_per'] * df_sales_scm_per_sca_opex_roy_mdf_foc_sub['Net Sales With Tax']

    df_sales_scm_per_sca_opex_roy_mdf_foc_sub = df_sales_scm_per_sca_opex_roy_mdf_foc_sub.drop(['Brand','Amount Financed','Subvention Cost','subvention_per'], axis=1)
    
    subvention_file_path = 'profit_and_loss/intermediate_file/' + str(currentYear) + "/" + str(currentMonth) + "/subvention_output.csv"
    bucket.blob(subvention_file_path).upload_from_string(df_sales_scm_per_sca_opex_roy_mdf_foc_sub.to_csv(), 'text/csv')



def calculate_transfer_price(bucket_name, tp_sales_file_name, tp_input_file_name, lov_channel_master_file_name):

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(tp_sales_file_name)
    data = blob.download_as_bytes()

    tp_base_file = pd.read_excel(io.BytesIO(data))
    
    headers = ['SBU', 'Calendar Day', 'Brand_ID', 'Store', 'Store Name', 'Distribution Channel', 'Distribution Channel Name', 'PBG DC / Non PBG DC', 
               'Format', 'Article', 'Article Name', 'Brick', 'Family','Segment', 'City', 'Channel Type', 'Total Sales Quantity / Base Unit', 'Total Gross Sales', 
               'Total Category Discount', 'Total Net Sales with tax', 'Tax amount', 'Total Net Sales Without Tax', 'COGS As Per MAP', 'Total COGS', 'Gross Margin']
    tp_base_file = tp_base_file.loc[:, headers]

    
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(tp_input_file_name)
    data = blob.download_as_bytes()

    tp_input_file = pd.read_excel(io.BytesIO(data))
    
    headers = ['From Date', 'To Date', 'Channel Nomenclature', 'Article Code', 'Article Name', 'Transfer Price (Excl GST)']
    tp_input_file = tp_input_file.loc[:, headers]

    tp_input_file = tp_input_file.drop_duplicates()

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(lov_channel_master_file_name)
    data = blob.download_as_bytes()

    dist_channel_master = pd.read_excel(io.BytesIO(data))
    
    dist_channel_master = dist_channel_master.loc[:, ['Name','Distribution Channel', 'Format', 'Channel Type']]
    dist_channel_master = dist_channel_master.rename(columns={'Name': 'Channel'})
    
    tp_base_file_final = pd.merge(tp_base_file, dist_channel_master, on=['Distribution Channel', 'Format', 'Channel Type'], how='left')
    
    result = pd.merge(tp_base_file_final, tp_input_file, left_on=['Article', 'Channel'], right_on=['Article Code','Channel Nomenclature'], how='left')
    result = result[((result['Calendar Day'] >= result['From Date']) 
                 & (result['Calendar Day'] <= result['To Date']))
                | (result['Article Code'].isna())]
    
    result['COGS Per Unit'] = result['Total COGS'] / (result['Total Sales Quantity / Base Unit'])
    result['Difference'] = result['Transfer Price (Excl GST)'] - result['COGS Per Unit']
    result['Transfer Price GAIN/LOSS'] = result['Difference']*result['Total Sales Quantity / Base Unit']
    
    grouped = result.groupby(['Article', 'Channel'])['Transfer Price GAIN/LOSS'].sum()

    df_tp = pd.DataFrame({'Article': grouped.index.get_level_values(0),
                       'Channel': grouped.index.get_level_values(1),
                       'Transfer Price GAIN/LOSS': grouped.values})

    df_tp['Transfer Price GAIN/LOSS'] = df_tp['Transfer Price GAIN/LOSS']/math.pow(10,7)
    df_tp['Article'] = df_tp['Article'].astype('int')
    
    sales_file_name = 'profit_and_loss/intermediate_file/' + str(currentYear) + "/" + str(currentMonth) + "/sales_output.csv"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(sales_file_name)
    data = blob.download_as_bytes()

    sales = pd.read_csv(io.BytesIO(data))
    # sales['Net Sales With Tax'] = sales['Net Sales With Tax'].astype(float)
    # sales['Article'] = sales['Article'].astype(int)

    grouped_df = sales.groupby(['Article', 'Channel'])['Net Sales WOT'].sum().reset_index()
    grouped_df = grouped_df.rename(columns={'Net Sales WOT': 'Total Sales'})

    merged_df = pd.merge(df_tp, grouped_df, on=['Article', 'Channel'])

    merged_df['TP_Allocation_per'] = (merged_df['Transfer Price GAIN/LOSS']/merged_df['Total Sales'])
    merged_df['TP_Allocation_per'] = merged_df['TP_Allocation_per'].replace([np.inf, -np.inf], np.nan)
    merged_df['TP_Allocation_per'] = merged_df['TP_Allocation_per'].fillna(0)

    tp_inter_file_path = 'profit_and_loss/intermediate_file/' + str(currentYear) + "/" + str(currentMonth) + "/tp_intermediate_file.csv"
    bucket.blob(tp_inter_file_path).upload_from_string(merged_df.to_csv(), 'text/csv')
    
    return merged_df


def transfer_price_columns(bucket_name, tp_sales_file_name, tp_input_file_name, lov_channel_master_file_name):

    subvention_file_name = 'profit_and_loss/intermediate_file/' + str(currentYear) + "/" + str(currentMonth) + "/subvention_output.csv"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(subvention_file_name)
    data = blob.download_as_bytes()

    sales = pd.read_csv(io.BytesIO(data))
    
    df_tp = calculate_transfer_price(bucket_name, tp_sales_file_name, tp_input_file_name, lov_channel_master_file_name)
    
    df_sales_with_tp = pd.merge(sales, df_tp, how='left', on=['Article','Channel'])
    
    df_sales_with_tp['transfer_price'] = df_sales_with_tp['Net Sales WOT']*df_sales_with_tp['TP_Allocation_per']*-1
    df_sales_with_tp['transfer_price'] = df_sales_with_tp['transfer_price'].fillna(0)
    
    # return df_sales_with_tp
    tp_file_path = 'profit_and_loss/intermediate_file/' + str(currentYear) + "/" + str(currentMonth) + "/tp_output.csv"
    bucket.blob(tp_file_path).upload_from_string(df_sales_with_tp.to_csv(), 'text/csv')


def calculate_inward_freight_and_mr21(bucket_name, family_level_input_file_name):

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(family_level_input_file_name)
    data = blob.download_as_bytes()

    scm = pd.read_excel(io.BytesIO(data))
    scm = scm[['Family','Inward Freight','MR21 (COGS Adjst)','COGS Adj']]

    sales_file_name = 'profit_and_loss/intermediate_file/' + str(currentYear) + "/" + str(currentMonth) + "/sales_output.csv"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(sales_file_name)
    data = blob.download_as_bytes()

    sales = pd.read_csv(io.BytesIO(data))
    # sales['Net Sales With Tax'] = sales['Net Sales With Tax'].astype(float)
    # sales['Article'] = sales['Article'].astype(int)

    grouped_df_all = sales.groupby(['Family.1'])['Net Sales With Tax'].sum().reset_index()
    grouped_df_gt = sales[sales['Channel2'] == 'GT'].groupby('Family.1')['Net Sales With Tax'].sum().reset_index()
    grouped_df_gt = grouped_df_gt.rename(columns={'Net Sales With Tax': 'Net Sales With Tax_GT'})

    df_freight_mr21 = pd.merge(scm, grouped_df_all, left_on = ['Family'], right_on=['Family.1'], how = 'left')
    df_freight_mr21_cogs = pd.merge(df_freight_mr21, grouped_df_gt, left_on = ['Family'], right_on=['Family.1'], how = 'left')

    df_freight_mr21['inward_freight_per'] = ((df_freight_mr21['Inward Freight'])/df_freight_mr21['Net Sales With Tax']).fillna(0)
    df_freight_mr21['mr21_per'] = ((df_freight_mr21['MR21 (COGS Adjst)']/math.pow(10,7))/df_freight_mr21['Net Sales With Tax']).fillna(0)
    df_freight_mr21_cogs['cogs_adj_gt_per'] = ((df_freight_mr21_cogs['COGS Adj']/math.pow(10,7))/df_freight_mr21_cogs['Net Sales With Tax_GT']).fillna(0)
    
    df_freight_mr21_cogs = df_freight_mr21_cogs.drop(['Inward Freight','MR21 (COGS Adjst)','COGS Adj','Family.1_x','Net Sales With Tax','Family.1_y', 'Net Sales With Tax_GT'], axis = 1)
    df_freight_mr21 = df_freight_mr21.drop(['Inward Freight','MR21 (COGS Adjst)','COGS Adj'], axis = 1)

    freight_mr21_cogs_inter_file_path = 'profit_and_loss/intermediate_file/' + str(currentYear) + "/" + str(currentMonth) + "/freight_mr21_cogs_intermediate_file.csv"
    bucket.blob(freight_mr21_cogs_inter_file_path).upload_from_string(df_freight_mr21_cogs.to_csv(), 'text/csv')

    freight_mr21_inter_file_path = 'profit_and_loss/intermediate_file/' + str(currentYear) + "/" + str(currentMonth) + "/freight_mr21_intermediate_file.csv"
    bucket.blob(freight_mr21_inter_file_path).upload_from_string(df_freight_mr21.to_csv(), 'text/csv')

    return df_freight_mr21,df_freight_mr21_cogs


def merge_sales_scm_per_sca_opex_roy_mdf_foc_sub_tp_inward_mr21(bucket_name, family_level_input_file_name):
    
    tp_file_name = 'profit_and_loss/intermediate_file/' + str(currentYear) + "/" + str(currentMonth) + "/tp_output.csv"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(tp_file_name)
    data = blob.download_as_bytes()

    df_sales = pd.read_csv(io.BytesIO(data))

    df_inward_freight_mr21,df_inward_freight_mr21_cogs = calculate_inward_freight_and_mr21(bucket_name, family_level_input_file_name)

    df_sales_inward_freight_mr21 = pd.merge(df_sales, df_inward_freight_mr21, left_on = 'Family.1', right_on = 'Family'
                                           , how = 'left')
    
    df_sales_inward_freight_mr21 = df_sales_inward_freight_mr21.loc[:,~df_sales_inward_freight_mr21.columns.str.endswith('_y')]
    df_sales_inward_freight_mr21 = df_sales_inward_freight_mr21.rename(columns={'Net Sales With Tax_x' : 'Net Sales With Tax', 'Family_x' : 'Family', 'Family.1_x' : 'Family.1'})

    df_sales_inward_freight_mr21['Inward_Freight'] = df_sales_inward_freight_mr21['inward_freight_per']*df_sales_inward_freight_mr21['Net Sales With Tax']
    df_sales_inward_freight_mr21['MR21_COGS'] = df_sales_inward_freight_mr21['mr21_per']*df_sales_inward_freight_mr21['Net Sales With Tax']
    
   
    df_sales_inward_freight_mr21_cogs = pd.merge(df_sales_inward_freight_mr21, df_inward_freight_mr21_cogs, left_on = 'Family.1', right_on = 'Family'
                                           , how = 'left')
    
    df_sales_inward_freight_mr21_cogs = df_sales_inward_freight_mr21_cogs.loc[:,~df_sales_inward_freight_mr21_cogs.columns.str.endswith('_y')]
    df_sales_inward_freight_mr21_cogs = df_sales_inward_freight_mr21_cogs.rename(columns={'Family_x' : 'Family'})
        
    df_sales_inward_freight_mr21_cogs['COGS Adj'] = np.where(df_sales_inward_freight_mr21_cogs['Channel2'] == 'GT', 
                                       (df_sales_inward_freight_mr21_cogs['cogs_adj_gt_per']*df_sales_inward_freight_mr21_cogs['Net Sales With Tax']),0)

    df_sales_inward_freight_mr21_cogs = df_sales_inward_freight_mr21_cogs.drop(['inward_freight_per','mr21_per', 'cogs_adj_gt_per','TP_Allocation_per', 'Total Sales', 'Transfer Price GAIN/LOSS'], axis = 1)


    # return df_sales_inward_freight_mr21_cogs
    freight_mr21_cogs_file_path = 'profit_and_loss/intermediate_file/' + str(currentYear) + "/" + str(currentMonth) + "/freight_mr21_cogs_output.csv"
    bucket.blob(freight_mr21_cogs_file_path).upload_from_string(df_sales_inward_freight_mr21_cogs.to_csv(), 'text/csv')


def save_final_output_to_gcs(bucket_name, df):

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    file_path = 'profit_and_loss/output/' + str(currentYear) + "/" + str(currentMonth) + "/"
    blob = list(bucket.list_blobs(prefix=file_path))

    fileList = [file.name for file in blob if '.csv' in file.name]
    print(fileList)

    if len(fileList) == 0:
        output_file_path = 'profit_and_loss/output/' + str(currentYear) + "/" + str(currentMonth) + "/profit_and_loss_v1.csv"
        bucket.blob(output_file_path).upload_from_string(df.to_csv(), 'text/csv')
    else:
        last_file_name = fileList.pop()
        version = last_file_name.split('_')[-1].split('.')[0]
        version = ''.join(filter(str.isdigit, version))
        print("version is:")
        print(version)
        version = int(version)+1 

        output_file_path = 'profit_and_loss/output/' + str(currentYear) + "/" + str(currentMonth) + "/profit_and_loss_v" + str(version) + ".csv"
        bucket.blob(output_file_path).upload_from_string(df.to_csv(index=False), 'text/csv')


def get_latest_file_name_from_gcs(bucket_name, folder_path):

    client = storage.Client()
    bucket = client.get_bucket(bucket_name)

    blobs = bucket.list_blobs(prefix=folder_path)
    sorted_blobs = sorted(blobs, key=lambda x: x.updated, reverse=True)

    if sorted_blobs:
        latest_blob_name = sorted_blobs[0].name
        return latest_blob_name

    else:
        return None


def remove_headers(bucket_name):

    folder_path = 'profit_and_loss/output/' + str(currentYear) + "/" + str(currentMonth) + "/"
    file_path = get_latest_file_name_from_gcs(bucket_name,folder_path)

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_path)
    data = blob.download_as_bytes()

    df = pd.read_csv(io.BytesIO(data))
    df['Flag'] = 'Actuals'

    output_file_path = 'profit_and_loss/output/' + str(currentYear) + "/" + str(currentMonth) + "/profit_and_loss.csv"
    bucket.blob(output_file_path).upload_from_string(df.to_csv(header=None,index=False), 'text/csv')


def calculate_all_metrics_columns(bucket_name, bq_project_id, bq_dataset, bq_table):

    freight_mr21_cogs_file_name = 'profit_and_loss/intermediate_file/' + str(currentYear) + "/" + str(currentMonth) + "/freight_mr21_cogs_output.csv"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(freight_mr21_cogs_file_name)
    data = blob.download_as_bytes()

    df = pd.read_csv(io.BytesIO(data))
    
    df1 = df.rename(columns={'Month, Year of Billing Date' : 'Calendar Year/Month', 'MH SBU' : 'SBU', 'Distribution Channel Desc' : 'Distribution Channel Name','Flag' : 'Channel Type',
                            'MH Segment' : 'Segment','Billing Quantity' : 'Sales Qty','Net Sales With Tax' : 'Gross Sales (All Channels) - System','Net Sales WOT' : 'Net Sales (All Channels) - System','transfer_price' : 'TP','Trade Discount' : 'Backend','Installation Cost' : 'Installation Exp.','Promo' : 'Promo System','EOL' : 'EOL Expenses',
                            'Bad Debt' : 'Bad Debts','royalty' : 'Royalty System','mdf' : 'MDF Income','COGS Adj' : 'Cogs Adjustment II','Inward_Freight' : 'Inward Freight',
                            'COGS' : 'COGS (All Channels System)','staff_cost' : 'Staff Cost','marketing_exp' : 'Marketing','other_expenses' : 'Other Expenses', 'Family.1' : 'Family_lov'})
    df1 = df1[['Calendar Year/Month', 'SBU', 'Brand_ID', 'Distribution Channel','Distribution Channel Name', 'Format',
               'Channel Type','MH Family', 'Segment', 'Article', 'Article Description','Channel', 'Channel Type',
               'Brick','Channel2', 'Family', 'Family_lov', 'Category','Sales Qty','COGS (All Channels System)', 
               'Gross Sales','Net Sales (All Channels) - System','Gross Sales (All Channels) - System',
               'WH Allocation Article','WH Allocation Brick','SO Allocation',
               'Liquidation Allocation', 'STO GT Allocation', 'STO RRL Allocation','SO Freight', 
               'WH Cost @Article level', 'Resq Rent', 'STO & Other','WH Cost @Brick & Brand', 
               'Bad Debts', 'CD', 'Defective &  E Waste','Display', 'EOL Expenses', 'Promo System', 'Backend', 
               'Warranty','Installation Exp.', 'Per Call Cost', 'Transporation Cost', 'OpEx Cost','Other Exp',
               'Part Consumption Cost', 'Marketing', 'Staff Cost','Other Expenses', 'Royalty System', 'MDF Income', 
               'foc_reversal_article','foc_provision_article', 'foc_total_article',
               'foc_reversal_brick','foc_provision_brick', 'foc_total_brick',
               'foc_reversal_family','foc_provision_family', 'foc_total_family',
               'Subvention', 'TP','Inward Freight', 'MR21_COGS', 'Cogs Adjustment II','Customer Group']]
    df1.iloc[:,19:69] = df1.iloc[:,19:69].fillna(0)
    
    df1['Other Channel Gain / Loss'] = np.where(df1.Channel2 == 'GT', 0, df1['Net Sales (All Channels) - System'] - df1['COGS (All Channels System)'])
    df1['Net Sales (All Channels)'] = df1['Net Sales (All Channels) - System'] - df1['Other Channel Gain / Loss'] - df1['TP']
    df1['foc_total'] = df1['foc_total_article'] + df1['foc_total_brick'] + df1['foc_total_family']
    df1['Total Backend'] = df1['Backend'] - df1['foc_total']
    df1['Promo'] = df1['Promo System'] + df1['Installation Exp.'].astype('float')
    df1['Selling & Promo'] = df1['Total Backend'] + df1['CD'] + df1['Promo'] + df1['Display']
    df1['Total Subvention Exp'] = df1['Subvention'] #+ df1['Subvention Adjst']
    df1['S&P+Subvention'] = df1['Selling & Promo'] + df1['Total Subvention Exp']
    df1['Net_Sales (All) (Exl. S&P)'] = df1['Net Sales (All Channels)'] - df1['S&P+Subvention']
    df1 = df1.loc[:,~df1.columns.duplicated()].copy()
    df1['dummy COGS Adj'] = np.where(df1['Article Description'].str.contains('dummy| du$', case=False), df1['COGS (All Channels System)']* -1, 0)
    df1['Liquidation'] = np.where(df1['Customer Group'] == 'P6', df1['Net Sales (All Channels)'] - df1['COGS (All Channels System)'], 0)
    df1['COGS (All Channels)'] = df1['COGS (All Channels System)'] + df1['Cogs Adjustment II'] + df1['dummy COGS Adj'] + df1['Liquidation'] + df1['Inward Freight']
    df1['Marketing'] = df1['Marketing'] - df1['dummy COGS Adj']
    df1['Gross Margin'] = df1['Net_Sales (All) (Exl. S&P)'] - df1['COGS (All Channels)']
    df1['CODB'] = df1['EOL Expenses'] + df1['Defective &  E Waste'] + df1['Bad Debts'] + df1['Royalty System'] + df1['MDF Income']
    df1['Total WH Cost'] = df1['WH Cost @Article level'].astype(float) + df1['WH Cost @Brick & Brand'].astype(float)
    df1['Supply Chain Costs (Net)'] = df1['SO Freight'].astype(float) + df1['Total WH Cost'].astype(float) + df1['Resq Rent'].astype(float) + df1['STO & Other'].astype(float)
    df1['Service Cost'] = df1['Per Call Cost'].astype(float) + df1['Transporation Cost'].astype(float) + df1['OpEx Cost'].astype(float) + df1['Other Exp'].astype(float) + df1['Part Consumption Cost'].astype(float)
    df1['Total Variable Cost'] = df1['CODB'] + df1['Supply Chain Costs (Net)'] + df1['Service Cost'].astype(float) + df1['Warranty']
    df1['Net Margin'] = df1['Gross Margin'] - df1['Total Variable Cost']
    df1['Total Fixed Cost'] = df1['Staff Cost'] + df1['Marketing'] + df1['Other Expenses']
    df1['EBITDA'] = df1['Net Margin'] - df1['Total Fixed Cost']
    
    df1= df1[['Calendar Year/Month','SBU','Brand_ID','Distribution Channel','Distribution Channel Name',
              'Channel Type','Format','Brick','MH Family','Segment','Article','Article Description',
              'Channel','Channel2','Family_lov','Category','Sales Qty','Gross Sales (All Channels) - System',
              'Net Sales (All Channels)','Net Sales (All Channels) - System','Other Channel Gain / Loss',
              'TP','S&P+Subvention','Selling & Promo','Backend','foc_reversal_article','foc_provision_article', 'foc_total_article',
               'foc_reversal_brick','foc_provision_brick', 'foc_total_brick',
               'foc_reversal_family','foc_provision_family', 'foc_total_family','foc_total','Total Backend','CD','Promo',
              'Promo System','Installation Exp.','Display','Subvention','Total Subvention Exp',
              'Net_Sales (All) (Exl. S&P)','COGS (All Channels)','COGS (All Channels System)','Liquidation',
              'Cogs Adjustment II','MR21_COGS','Inward Freight','Gross Margin','Total Variable Cost',
              'CODB','EOL Expenses','Defective &  E Waste','Bad Debts','Royalty System','MDF Income',
              'Supply Chain Costs (Net)','SO Freight','WH Cost @Article level',
              'WH Cost @Brick & Brand','Total WH Cost','Resq Rent','STO & Other','Service Cost',
              'Per Call Cost','Transporation Cost','OpEx Cost','Other Exp','Part Consumption Cost',
              'Warranty','Net Margin','Total Fixed Cost','Staff Cost','Marketing','Other Expenses','EBITDA']]
    df1['WH Cost @Brick & Brand']=df1['WH Cost @Brick & Brand'].replace('nan',0)
    
    df1['COGS (All Channels)'] = df1['COGS (All Channels)'].apply(lambda x: '{:.15f}'.format(x)).astype(float)
    df1['COGS (All Channels System)'] = df1['COGS (All Channels System)'].apply(lambda x: '{:.15f}'.format(x)).astype(float)
    df1['Cogs Adjustment II'] = df1['Cogs Adjustment II'].apply(lambda x: '{:.15f}'.format(x)).astype(float)
    df1['EOL Expenses'] = df1['EOL Expenses'].apply(lambda x: '{:.15f}'.format(x)).astype(float)
    df1['Defective &  E Waste'] = df1['Defective &  E Waste'].apply(lambda x: '{:.15f}'.format(x)).astype(float)
    df1['Royalty System'] = df1['Royalty System'].apply(lambda x: '{:.15f}'.format(x)).astype(float)
    df1['Service Cost'] = df1['Service Cost'].apply(lambda x: '{:.15f}'.format(x)).astype(float)
    df1['Other Expenses'] = df1['Other Expenses'].apply(lambda x: '{:.15f}'.format(x)).astype(float)
    df1['Gross Margin'] = df1['Gross Margin'].apply(lambda x: '{:.15f}'.format(x)).astype(float)
    df1['Net Margin'] = df1['Net Margin'].apply(lambda x: '{:.15f}'.format(x)).astype(float)
    df1['TP'] = df1['TP'].apply(lambda x: '{:.15f}'.format(x)).astype(float)
    df1['S&P+Subvention'] = df1['S&P+Subvention'].apply(lambda x: '{:.15f}'.format(x)).astype(float)
    df1['Selling & Promo'] = df1['Selling & Promo'].apply(lambda x: '{:.15f}'.format(x)).astype(float)
    df1['Promo'] = df1['Promo'].apply(lambda x: '{:.15f}'.format(x)).astype(float)
    
    df1 = df1.loc[:,~df1.columns.duplicated()].copy()
    df1.iloc[:,17:] = df1.iloc[:,17:].fillna(0)
    df1['Staff Cost'] = df1['Staff Cost'].apply(lambda x: '{:.15f}'.format(x)).astype(float)
    df1['Marketing'] = df1['Marketing'].apply(lambda x: '{:.15f}'.format(x)).astype(float)
    df1=df1.replace('nan',0)
    # df1['month_year']= (current_date.replace(day=1) - timedelta(days=1)).strftime('%Y-%m')
    df1['month_year'] = df1['Calendar Year/Month'].apply(lambda x: datetime.strptime(x, '%B %Y').strftime('%Y-%m'))

    
    save_final_output_to_gcs(bucket_name, df1)

    remove_headers(bucket_name)

    # month = (current_date.replace(day=1) - timedelta(days=1)).strftime('%Y-%m')
    # delete_data_from_bigquery_table(bq_project_id, bq_dataset, bq_table, month)


def calculate_aop_and_plan(bucket_name, aop_file_name, plan_file_name):
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(aop_file_name)
    data = blob.download_as_bytes()

    last_month = current_date.month - 1
    current_year = current_date.year

    df_aop = pd.read_excel(io.BytesIO(data))

    # df_aop = df_aop[(df_aop['Month'].dt.month == last_month) & (df_aop['Month'].dt.year == current_year)]
    df_aop = df_aop.rename(columns={'Month' : 'Calendar Year/Month', 'Brand' : 'Brand_ID', 'Family' : 'MH Family',
                         'Other Channel Gain' : 'Other Channel Gain / Loss', 'TP Loss / Gain' : 'TP',
                         'S&P\n+Subvention' : 'S&P+Subvention', 'Net_Sales (All)\n(Exl. S&P)' : 'Net_Sales (All) (Exl. S&P)',
                         'COGS (All Channels\nSystem' : 'COGS (All Channels System)', 'Cogs Adjustment' : 'Cogs Adjustment II',
                         'MR21\n(COGS Adjst)' : 'MR21_COGS', 'Defective & E waste' : 'Defective &  E Waste',
                         'Royalty_System' : 'Royalty System', 'WH Cost' : 'Total WH Cost', 'Brick' : 'Family_lov'})


    new_columns = ['SBU', 'Distribution Channel', 'Distribution Channel Name', 'Channel Type', 'Format', 'Segment',
               'Article', 'Article Description', 'Channel', 'Channel2', 'Brick','month_year']

    df_aop = df_aop.assign(**{col: np.nan for col in new_columns})

    new_columns = ['foc_reversal_article', 'foc_provision_article', 'foc_total_article', 'foc_reversal_brick',
               'foc_provision_brick', 'foc_total_brick', 'foc_reversal_family', 'foc_provision_family',
               'foc_total_family', 'foc_total', 'Total Backend', 'Total Subvention Exp', 'Liquidation',
               'WH Cost @Article level', 'WH Cost @Brick & Brand']

    df_aop = df_aop.assign(**{col: 0.0 for col in new_columns})
    # df_aop['month_year']= (current_date.replace(day=1) - timedelta(days=1)).strftime('%Y-%m')
    df_aop['Calendar Year/Month'] = df_aop['Calendar Year/Month'].dt.strftime('%B %Y')

    df_aop= df_aop[['Calendar Year/Month', 'SBU', 'Brand_ID', 'Distribution Channel','Distribution Channel Name', 'Channel Type', 'Format', 'Brick','MH Family', 'Segment', 'Article', 'Article Description', 'Channel',
              'Channel2', 'Family_lov', 'Category', 'Sales Qty','Gross Sales (All Channels) - System', 'Net Sales (All Channels)','Net Sales (All Channels) - System', 'Other Channel Gain / Loss', 'TP',
              'S&P+Subvention', 'Selling & Promo', 'Backend', 'foc_reversal_article','foc_provision_article', 'foc_total_article', 'foc_reversal_brick','foc_provision_brick', 'foc_total_brick', 'foc_reversal_family',
              'foc_provision_family', 'foc_total_family', 'foc_total','Total Backend', 'CD', 'Promo', 'Promo System', 'Installation Exp.','Display', 'Subvention', 'Total Subvention Exp',
              'Net_Sales (All) (Exl. S&P)', 'COGS (All Channels)','COGS (All Channels System)', 'Liquidation', 'Cogs Adjustment II','MR21_COGS', 'Inward Freight', 'Gross Margin', 'Total Variable Cost',
              'CODB', 'EOL Expenses', 'Defective &  E Waste', 'Bad Debts','Royalty System', 'MDF Income', 'Supply Chain Costs (Net)','SO Freight', 'WH Cost @Article level', 'WH Cost @Brick & Brand',
              'Total WH Cost', 'Resq Rent', 'STO & Other', 'Service Cost','Per Call Cost', 'Transporation Cost', 'OpEx Cost', 'Other Exp',
              'Part Consumption Cost', 'Warranty', 'Net Margin', 'Total Fixed Cost','Staff Cost', 'Marketing', 'Other Expenses', 'EBITDA', 'month_year', 'Flag']]

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(plan_file_name)
    data = blob.download_as_bytes()

    df_plan = pd.read_excel(io.BytesIO(data))

    # df_plan = df_plan[(df_plan['Month'].dt.month == last_month) & (df_plan['Month'].dt.year == current_year)]
    df_plan = df_plan.rename(columns={'Month' : 'Calendar Year/Month', 'Brand' : 'Brand_ID', 'Family' : 'MH Family',
                         'Other Channel Gain' : 'Other Channel Gain / Loss', 'TP Loss / Gain' : 'TP',
                         'S&P\n+Subvention' : 'S&P+Subvention', 'Net_Sales (All)\n(Exl. S&P)' : 'Net_Sales (All) (Exl. S&P)',
                         'COGS (All Channels\nSystem' : 'COGS (All Channels System)', 'Cogs Adjustment' : 'Cogs Adjustment II',
                         'MR21\n(COGS Adjst)' : 'MR21_COGS', 'Defective & E waste' : 'Defective &  E Waste',
                         'Royalty_System' : 'Royalty System', 'WH Cost' : 'Total WH Cost', 'Brick' : 'Family_lov'})

    new_columns = ['SBU', 'Distribution Channel', 'Distribution Channel Name', 'Channel Type', 'Format', 'Segment',
               'Article', 'Article Description', 'Channel', 'Channel2', 'Brick' ]

    df_plan = df_plan.assign(**{col: np.nan for col in new_columns})

    new_columns = ['foc_reversal_article', 'foc_provision_article', 'foc_total_article', 'foc_reversal_brick',
               'foc_provision_brick', 'foc_total_brick', 'foc_reversal_family', 'foc_provision_family',
               'foc_total_family', 'foc_total', 'Total Backend', 'Total Subvention Exp', 'Liquidation',
               'WH Cost @Article level', 'WH Cost @Brick & Brand']

    df_plan = df_plan.assign(**{col: 0.0 for col in new_columns})
    # df_plan['month_year']= (current_date.replace(day=1) - timedelta(days=1)).strftime('%Y-%m')
    df_plan['Calendar Year/Month'] = df_plan['Calendar Year/Month'].dt.strftime('%B %Y')
    df_plan['month_year'] = df_plan['Calendar Year/Month'].apply(lambda x: datetime.strptime(x, '%B %Y').strftime('%Y-%m'))

    df_plan= df_plan[['Calendar Year/Month', 'SBU', 'Brand_ID', 'Distribution Channel','Distribution Channel Name', 'Channel Type', 'Format', 'Brick','MH Family', 'Segment', 'Article', 'Article Description', 'Channel',
              'Channel2', 'Family_lov', 'Category', 'Sales Qty','Gross Sales (All Channels) - System', 'Net Sales (All Channels)','Net Sales (All Channels) - System', 'Other Channel Gain / Loss', 'TP',
              'S&P+Subvention', 'Selling & Promo', 'Backend', 'foc_reversal_article','foc_provision_article', 'foc_total_article', 'foc_reversal_brick','foc_provision_brick', 'foc_total_brick', 'foc_reversal_family',
              'foc_provision_family', 'foc_total_family', 'foc_total','Total Backend', 'CD', 'Promo', 'Promo System', 'Installation Exp.','Display', 'Subvention', 'Total Subvention Exp',
              'Net_Sales (All) (Exl. S&P)', 'COGS (All Channels)','COGS (All Channels System)', 'Liquidation', 'Cogs Adjustment II','MR21_COGS', 'Inward Freight', 'Gross Margin', 'Total Variable Cost',
              'CODB', 'EOL Expenses', 'Defective &  E Waste', 'Bad Debts','Royalty System', 'MDF Income', 'Supply Chain Costs (Net)','SO Freight', 'WH Cost @Article level', 'WH Cost @Brick & Brand',
              'Total WH Cost', 'Resq Rent', 'STO & Other', 'Service Cost','Per Call Cost', 'Transporation Cost', 'OpEx Cost', 'Other Exp',
              'Part Consumption Cost', 'Warranty', 'Net Margin', 'Total Fixed Cost','Staff Cost', 'Marketing', 'Other Expenses', 'EBITDA', 'month_year', 'Flag']]

    union_df = pd.concat([df_aop, df_plan])

    output_file_path = 'profit_and_loss/aop_plan/' + str(currentYear) + "/" + str(currentMonth) + "/aop_plan_v1.csv"
    bucket.blob(output_file_path).upload_from_string(union_df.to_csv(index=False), 'text/csv')

    output_file_path = 'profit_and_loss/aop_plan/' + str(currentYear) + "/" + str(currentMonth) + "/aop_plan.csv"
    bucket.blob(output_file_path).upload_from_string(union_df.to_csv(header=None,index=False), 'text/csv')