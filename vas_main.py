import psycopg2
import time
import datetime
from dateutil import parser
from datetime import timedelta
import pandas as pd
import numpy as np
import json
from sshtunnel import SSHTunnelForwarder
import psycopg2
import paramiko
# SSH server information
ssh_host = '10.134.72.5'
ssh_port = 22
ssh_username = 'fynd'
private_key_path = '/Users/Nitin14.Patil/Downloads/ssh_key.fynd'
# Database server information
db_host = '172.17.0.16'
db_port = 5432
db_username = 'fynd_hogwarts_read'
db_password = 'fynd_hogwarts_read!2022'
 
"""
sql = '''select
t.primary_identifier,
t.state,
 
 
t.status,
 
 
t.data,
 
 
te.payload
 
 
from task t
 
 
join
 
 
task_events te
 
 
on t.uid = te.task_id
 
 
where
 
t.state='placed' and
 
t.status not in ('stopped', 'failed') and
 
identifier_type = 'fynd_order' and
 
te.payload <> 'None' and
 
t.created_on >= '{compare_date}' '''.format(compare_date = datetime.datetime.now() - datetime.timedelta(minutes= 10))
 
"""
"""
sql = '''select
 
t.primary_identifier,
 
 
t.state,
 
 
t.status,
 
 
t.data,
 
 
te.payload
 
 
from task t
 
 
join
 
 
task_events te
 
 
on t.uid = te.task_id
 
 
where
 
t.state='placed' and
 
t.status not in ('stopped', 'failed') and
 
t.identifier_type = 'fynd_order' and
 
t.primary_identifier in (

'FY6568B8A80E1D112AD7',
'FY656B303C0E2FC06551',
'FY656F36720EB3099EA5',
'FY6572BE090EA692F0BE',
'FY6578640B0E70227AE9',
'FY657AB1CB0ECAC47D9F',
'FY6568A93A0EBE573EC8',
'FY656C88140EC03B0AE6',
'FY656DB6A10EC71E0A3D',
'FY65770E5A0ECA1990DD',
'FY657348D60E9B5168B4',
'FY6583E5110E4E0C2C8C',
'FY65689DDA0E6CC3505B',
'FY657441830E3CB1F028',
'FY6579A5A40ED2C94D33',
'FY6579BD6F0E7CB90A8B',
'FY656B4A790EF348557B',
'FY656F0A950EB9FF651C',
'FY65E079140EF9162D52'
                         
                         
) and
 
te.payload <> 'None'
 
'''


"""
sql = '''select
 
 
t.primary_identifier,
 
 
t.state,
 
 
t.status,
 
 
t.data,
 
 
te.payload
 
 
from task t
 
 
join
 
 
task_events te
 
 
on t.uid = te.task_id
 
 
where
 
t.state='placed' and
 
t.status not in ('stopped', 'failed') and
 
identifier_type = 'fynd_order' and
 
te.payload <> 'None' and
 
t.created_on >= '{fromDate}' and
 
t.created_on <= '{toDate}' '''.format(fromDate = datetime.datetime(2024, 4, 26, 0, 0, 0, 0), toDate = datetime.datetime(2024, 5, 1, 0, 0, 0, 0))




 

ssh_key = paramiko.RSAKey.from_private_key_file(private_key_path)
 
 
 
with SSHTunnelForwarder(
    (ssh_host, ssh_port),
    ssh_username=ssh_username,
    ssh_pkey=ssh_key,
    remote_bind_address=(db_host, db_port)
) as tunnel:

    conn = psycopg2.connect(
        host='localhost',
        port=tunnel.local_bind_port,
        user=db_username,
        password=db_password,
        database='hogwarts'
    )
 
 
    cursor = conn.cursor()
    cursor.execute("select version()")    
    data = cursor.fetchone()
    print("Connection established to: ",data)
 
 
    cursor.execute(sql)
    results = cursor.fetchall()
    print(sql)
 
    cursor.close()
    conn.close()
data=results


list_json=[]
vas_json=[]
for t in results:
    list_json.append(list(t))
for n in range(len(list_json)):   
    a=list_json[n][4]
    a=eval(a)
    if 'products' in a and any(product.get('ITEM_VAL') == 'VAS' for product in a['products']):
    
        vas_json.append(a)

meta_d=pd.DataFrame()
for n1 in range(len(vas_json)):
    keys_to_extract = ['temp_order_id', 'customer_name','contact_number','billing_address','email_id','order_created_date']
    extracted_values = [[d[key] for key in keys_to_extract] for d in vas_json]
    
    df = pd.DataFrame(extracted_values, columns=keys_to_extract)

    ajc=pd.json_normalize(vas_json[n1])
    Article_aj=pd.json_normalize(ajc['products'])
    meta=Article_aj[0].to_dict()
    meta_df=pd.json_normalize(meta)
    
    meta_df1=meta_df[['0.REF_NO','0.article_id']]
    
    meta_d=pd.concat([meta_d,meta_df1])




meta_d=meta_d.reset_index()
meta_d=meta_d.drop(columns=['index'])


try:

    for n2 in range (len(df['billing_address'])):
    
    
        my_dict=df['billing_address'][n2]
    
        keys_to_get = ['flat_no', 'building_name','street','sector','city','state','pin']
    
        selected_values = {key: my_dict[key] for key in keys_to_get if key in my_dict}
    
    
    
    
        values = [str(value) for value in selected_values.values()]
    
        comma_separated_values = ' '.join(values)
    
    
    
        df['billing_address'][n2]=comma_separated_values
        final_df=pd.concat([df,meta_d],axis=1,ignore_index=True)
        column_names = ['temp_order_id', 'customer_name','contact_number','billing_address','email_id','order_created_date','imei_no','article_code']
        final_df.columns=(column_names)

        print(final_df)
        final_df.to_csv('VAS_data.csv',index=False)
        
except:
    print('Messege:- There is no VAS entry Yesterday')