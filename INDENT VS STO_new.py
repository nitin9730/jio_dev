from pymongo import MongoClient
import pandas as pd
import time
import datetime 
from datetime import datetime, timedelta



# Assuming you have the list of not_present_identifiers
#not_present_identifiers = ['RD64A802990133A838FF', 'FYjewjwkjewfwe']



# MongoDB connection
client = MongoClient('mongodb://fynd_cofo_readwrite:readWrite_cofo!2023@10.134.32.72:27017,10.134.32.70:27017,10.134.32.71:27017/cofo?replicaSet=re_extensions_set&readPreference=secondaryPreferred')



# Access the 'extensions' database
db = client.cofo



# Access the 'raven_order_proxy' collection
collection = db.cofo_store_mapping



date= time.strftime("%Y%m%d%H%M%S")
yesterday = datetime.now() - timedelta(days=1)
yesterday_start = datetime(yesterday.year, yesterday.month, yesterday.day, 0, 0, 0, 0)
yesterday_end = datetime(yesterday.year, yesterday.month, yesterday.day, 23, 59, 59, 999999)





#https://fyndsecure.perimeter81.com/sign-in?redirectUrl=perimeter81://perimeter81.com/macos/callback
cursor= collection.find()
list_curs = list(cursor)
print(len(list_curs))
df1 = pd.DataFrame(list_curs)
# df1.to_csv(f'/Users/Nitin14.Patil/Documents/MONGODB/werks{date}.csv', index=False)




import psycopg2

import time


import json




from datetime import datetime, timedelta, timezone  
import pandas as pd


 
from sshtunnel import SSHTunnelForwarder

import paramiko 
# SSH server information

ssh_host = '10.134.72.5'

ssh_port = 22

ssh_username = 'fynd'

private_key_path = '/Users/nitin14.patil/Downloads/ssh_key.fynd'
 
# Database server information

db_host = '172.17.0.80'

db_port = 5432

db_username = 'fynd_dms_orders_readwrite'

db_password = 'readWrite_dms_orders!2024'
last_two_hours_end_time = datetime.now(timezone.utc)
last_two_hours_start_time = last_two_hours_end_time.replace(minute=0, second=0, microsecond=0) - timedelta(days=200)
 
formatted_start_time = last_two_hours_start_time.strftime('%Y-%m-%d %H:%M:%S')
formatted_end_time = last_two_hours_end_time.strftime('%Y-%m-%d %H:%M:%S')
 
date1 = time.strftime("%Y%m%d%H%M%S")
"""
 
sql = '''select
 
 
t.primary_identifier,
 
 
t.state,
 
 
t.status,
 
 
t.data,
 
 
te.payload
 
 
from task t
 
 
joinarticle_x
 
 
task_events te
 
 
on t.uid = te.task_id
 
 
where
 
t.state='placed' and
 
t.status not in ('stopped', 'failed') and
 
identifier_type = 'fynd_order' and
 
te.payload <> 'None' and
 
t.created_on >= '{fromDate}' and
 
t.created_on <= '{toDate}' '''.format(fromDate = datetime.datetime(2024, 2, 13, 0, 0, 0, 0), toDate = datetime.datetime(2024, 2, 15, 0, 0, 0, 0))
 
"""
 
sql = f'''select o.channel_order_id,o.created_at  ,p.order_id ,p.quantity ,p.processed_quantity,p.sku,p.seq_id,o.sto_number ,o.prm_id,p.processed_unit_price ,srr.response_entity
from orders o  
left join products p on p.order_id = o.order_id
left join sto_request_response srr on srr.channel_order_id =o.channel_order_id 
where
o.created_at BETWEEN '{formatted_start_time}' AND '{formatted_end_time}' '''
ssh_key = paramiko.RSAKey.from_private_key_file(private_key_path)

 
 
with SSHTunnelForwarder(

    (ssh_host, ssh_port),

    ssh_username=ssh_username,

    ssh_pkey=ssh_key,

    remote_bind_address=(db_host, db_port)

) as tunnel:

    # Connect to the PostgreSQL database via the SSH tunnel

    conn = psycopg2.connect(

        host='localhost',

        port=tunnel.local_bind_port,

        user=db_username,

        password=db_password,

        database='dms_orders'

    )
 
 
    cur = conn.cursor()
 
 
    cur.execute(sql)

    results = cur.fetchall()
 
 
    cur.close()

    conn.close()
 
 
print(sql)
 
 
len(results)
 
print(len(results))



print(results)


df = pd.DataFrame(results)

df.columns = ['channel_order_id', 'created_at','order_id','quantity','processed_quantity','sku','seq_id','sto_number','prm_id','processed_unit_price','Error']





# Sample JSON strings

# Parse JSON strings and extract messages
json_strings = df['Error'].values
messages = []
    
for json_string in json_strings: 
    try: 
        json_data = json.loads(json_string)
        return_data = json_data.get('t_RETURN')
        if return_data:
            
            for item in return_data:
                
                message = item.get('message', '')
                
            messages.append(message)
            print(len(messages))
    except:
        message='Null'
        messages.append(message)
        print(len(messages))


# Create DataFrame
dof = pd.DataFrame(messages, columns=['Message'])
a=pd.DataFrame()
df=pd.concat([df,dof],axis=1)
# Display DataFrame
df.drop(['Error'], axis=1,inplace=True)






result = pd.merge(df, df1, on='prm_id', how='left')

print(result)

print(df.columns)


result12 = result[['channel_order_id', 'created_at', 'order_id', 'quantity',
       'processed_quantity', 'sku', 'seq_id', 'sto_number', 'prm_id',
       'processed_unit_price', 'Message', 'store_code']]


result12.to_csv(f'/Users/nitin14.patil/Documents/MONGODB/Indent{date1}.csv', index=False)
