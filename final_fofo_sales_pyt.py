#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 24 11:09:17 2024
@author: suraj2.desai
"""
from sshtunnel import SSHTunnelForwarder
import mysql.connector
import psycopg2
from datetime import timedelta
from datetime import datetime
import pandas as pd
import time
import json
import numpy as np
import pymysql
# Define your SSH and MySQL details
ssh_host = '10.134.72.5'
ssh_port = 22
ssh_user = 'fynd'
ssh_private_key =  r'/Users/Nitin14.Patil/Downloads/ssh_key.fynd'
 

mysql_host = '172.17.0.38'  # This should be 127.0.0.1 because we are using SSH tunneling
mysql_port = 3306
mysql_user = 'fynd_avis_read'
mysql_password = 'fynd_avis_read!2022'
mysql_database = 'avis'
fromDate = datetime(2024, 4, 1, 0, 0, 0)
toDate = datetime(2024, 9, 21, 15, 56, 0) 
from_timestamp = int(fromDate.timestamp())
to_timestamp = int(toDate.timestamp())
query = """
SELECT s.fynd_order_id,
s.id as "shipment_id",
FROM_UNIXTIME(o.created_ts) as "order_date",
pm.mode,
ss.status,
b.seller_identifier as "article_code",
b.id as "bag_id",
i.name as "item_name",
i.brand,
i.l1_category ,
i.l2_category ,
i.l3_category ,
b.item_id,
b.gstin_code,
b.quantity ,
b.journey_type,
s2.code as "store_code",
b.store_id ,
s.store_invoice_id ,
s.credit_note_id,
payment_mode_id ,
source,ordering_channel,
s.delivery_awb_number,
s.delivery_address_json,
b.prices,
b.article_json,
b.meta,
b.applied_promos,
s.meta as "shipment_meta"
FROM avis.`order` o
LEFT JOIN shipment s ON o.fynd_order_id = s.fynd_order_id
LEFT JOIN (SELECT shipment_id, status, created_at
FROM (SELECT shipment_id,status,created_at, ROW_NUMBER() OVER (PARTITION BY shipment_id ORDER BY created_ts DESC ,updated_ts DESC, id DESC) AS row_num FROM shipment_status) AS subquery
WHERE row_num = 1) ss ON s.id = ss.shipment_id
LEFT JOIN bag b on s.id = b.shipment_id
LEFT JOIN item i on b.item_id = i.id
LEFT JOIN payment_mode pm on o.payment_mode_id = pm.id
LEFT JOIN store s2 on b.store_id = s2.id
WHERE o.fynd_order_id like 'SS%' 
 
and
o.created_ts >= {from_timestamp} 
AND o.created_ts <= {to_timestamp}
""".format(from_timestamp=from_timestamp, to_timestamp=to_timestamp)
with SSHTunnelForwarder(
    (ssh_host, ssh_port),
    ssh_username=ssh_user,
    ssh_pkey=ssh_private_key,
    remote_bind_address=(mysql_host, mysql_port)
) as tunnel:
    # Connect to the MySQL database using pymysql
    connection = pymysql.connect(
        user=mysql_user,
        password=mysql_password,
        host='127.0.0.1',
        port=tunnel.local_bind_port,
        database=mysql_database
    )
    # Create a cursor object
    cursor = connection.cursor()
    # Disable ONLY_FULL_GROUP_BY for the current session
    cursor.execute("SET sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));")
    cursor.execute(query)
    result = cursor.fetchall()
    connection.close()
    # Convert the result to a Pandas DataFrame
    columns = [column[0] for column in cursor.description]
    df = pd.DataFrame(result, columns=columns)
 
    df1 = df[['fynd_order_id','shipment_id','article_code','bag_id','delivery_address_json','prices','article_json','meta','shipment_meta']]
    df2 = df1.values.tolist()
    df3 = df2[1][6]
    result = pd.DataFrame()
    result1 = pd.DataFrame()
    result2 = pd.DataFrame()
    result3 = pd.DataFrame()
    result4 = pd.DataFrame()
    df5 = pd.DataFrame()
    df8 = pd.DataFrame()
    for n in range(len(df2)):
        df3 = df2[n][6]
        if df3 is not None:
            df4 = json.loads(df3)
            df5 = pd.json_normalize(df4)[['size', 'price_marked']]
            df5['fynd_order_id'] = df2[n][0]
            df5['shipment_id'] = df2[n][1]
            df5['article_code'] = df2[n][2]
            df5['bag_id'] = df2[n][3]
            df5['identifier'] = [df4['identifier']]
            df5 = df5[['fynd_order_id','shipment_id','article_code','bag_id','size','price_marked','identifier']]
        if df3 is None:
            df5['fynd_order_id'] = df2[n][0]
            df5['shipment_id'] = df2[n][1]
            df5['article_code'] = df2[n][2]
            df5['bag_id'] = df2[n][3]
            df5['size'] = ''
            df5['price_marked'] = ''
            df5['identifier'] = ''
            df5 = df5[['fynd_order_id','shipment_id','article_code','bag_id','size','price_marked','identifier']]
        result = pd.concat([result, df5], axis=0, ignore_index=True)
        #df6 = pd.json_normalize(df4, meta=['size', 'price_marked', 'identifier.ean','identifier.sku_code'])
 
 
    for n in range(len(df2)):
        df3 = df2[n][4]
        df4 = json.loads(df3)
        df5 = pd.json_normalize(df4)
        df5['fynd_order_id'] = df2[n][0]
        df5['shipment_id'] = df2[n][1]
        df5['article_code'] = df2[n][2]
        df5['bag_id'] = df2[n][3]
        result1 = pd.concat([result1, df5], axis=0, ignore_index=True)
 
    for n in range(len(df2)):
        df3 = df2[n][5]
        if df3 is not None:
            df4 = json.loads(df3)
            df5 = pd.json_normalize(df4['amount_paid']['amount'])
            df6 = pd.json_normalize(df4['price_marked']['amount'])
            df7 = pd.json_normalize(df4['discount']['amount'])
            df14 =pd.json_normalize(df4['coupon_effective_discount']['amount'])
            df15 =pd.json_normalize(df4['promotion_effective_discount']['amount'])                      
            df8 = pd.concat([df5, df6, df7,df14,df15], axis=1, ignore_index=True)
            df8 = df8[[0,2,4,6,8]]
            df8 = df8.rename(columns={0: 'amount_paid', 2: 'price_marked', 4: 'discount',6:'coupon_applied',8:'promo_applied'})
            df8['fynd_order_id'] = df2[n][0]
            df8['shipment_id'] = df2[n][1]
            df8['article_code'] = df2[n][2]
            df8['bag_id'] = df2[n][3]
            df8 = df8[['fynd_order_id','shipment_id','article_code','bag_id','amount_paid','price_marked','discount','coupon_applied','promo_applied']]
        if df3 is None:
            df8['fynd_order_id'] = df2[n][0]
            df8['shipment_id'] = df2[n][1]
            df8['article_code'] = df2[n][2]
            df8['bag_id'] = df2[n][3]
            df8['amount_paid'] = ''
            df8['price_marked'] = ''
            df8['discount'] = ''
            df8['coupon_applied'] = ''
            df8['promo_applied'] = ''
            df8 = df8[['fynd_order_id','shipment_id','article_code','bag_id','amount_paid','price_marked','discount','coupon_applied','promo_applied']]
        result2 = pd.concat([result2, df8], axis=0, ignore_index=True)
    for n in range(len(df2)):
        df3 = df2[n][7]
        if df3 is not None:
            df4 = json.loads(df3)
            if 'serial_numbers' in df4:
                df5['serial_numbers'] = [df4['serial_numbers']]
                df5['fynd_order_id'] = df2[n][0]
                df5['shipment_id'] = df2[n][1]
                df5['article_code'] = df2[n][2]
                df5['bag_id'] = df2[n][3]
            if 'serial_numbers' not in df4:
                df5['serial_numbers'] = ''
                df5['fynd_order_id'] = df2[n][0]
                df5['shipment_id'] = df2[n][1]
                df5['article_code'] = df2[n][2]
                df5['bag_id'] = df2[n][3]
        if df3 is None:
            df5['serial_numbers'] = ''
            df5['fynd_order_id'] = df2[n][0]
            df5['shipment_id'] = df2[n][1]
            df5['article_code'] = df2[n][2]
            df5['bag_id'] = df2[n][3]
        result3 = pd.concat([result3, df5], axis=0, ignore_index=True)
    for n in range(len(df2)):
        df3 = df2[n][8]
        df4 = json.loads(df3)
        if any(key.startswith('article_level_discount') for key in df4):
                for key in df4:
                    if key.startswith('article_level_discount'):
                        df5 = pd.json_normalize(df4[key])
                        df5['fynd_order_id'] = df2[n][0]
                        df5['bag_id'] = df2[n][3]
        else:
           df5['bagId'] = ''
           df5['lineNo'] = ''
           df5['articleId'] = ''
           df5['articlePrice'] = ''
           df5['discountAmount'] = ''
           df5['fynd_order_id'] = df2[n][0]
           df5['bag_id'] = df2[n][3]
        result4 = pd.concat([result4, df5], axis=0, ignore_index=True)
 
 
    final_dataframe = pd.merge(df, result, on = ['fynd_order_id','shipment_id','article_code','bag_id'], how = 'inner')
    final_dataframe1 = pd.merge(final_dataframe, result1, on = ['fynd_order_id','shipment_id','article_code','bag_id'], how = 'inner')
    final_dataframe2 = pd.merge(final_dataframe1, result2, on = ['fynd_order_id','shipment_id','article_code','bag_id'], how = 'inner')
    final_dataframe3 = pd.merge(final_dataframe2, result3, on = ['fynd_order_id','shipment_id','article_code','bag_id'], how = 'inner')
    final_dataframe4 = pd.merge(final_dataframe3, result4, on = ['fynd_order_id','bag_id'], how = 'left')
    
 
final_dataframe4['Final_status']  = np.where(final_dataframe4['credit_note_id'].isna(), final_dataframe4['status'], 'handed_over_to_customer') 


# Sample dataframe


# Create column 'c' where column 'b' contains 'xx'
final_dataframe4['SMKITTI'] = ''
final_dataframe4.loc[final_dataframe4['applied_promos'].str.contains('SMKITTI',na=False), 'SMKITTI'] = 'Yes'

print(df)

# and
# ss.status not in ('pending','payment_failed')




date = time.strftime("%Y%m%d%H%M%S")
final_dataframe4 = final_dataframe4.replace('\n', '-', regex=True)
final_dataframe4.to_csv(rf'\Users\Nitin14.Patil\Downloads\order_discount{date}.txt', sep = '|')