


import time
from datetime import datetime, timedelta, timezone
import paramiko
from sshtunnel import SSHTunnelForwarder
import pymysql
import pandas as pd
import json
 
# SSH and DB server information
ssh_host = '10.134.72.5'
ssh_port = 22
ssh_username = 'fynd'
private_key_path = '/Users/nitin14.patil/Downloads/ssh_key.fynd'
 
db_host = '172.17.0.8'
db_port = 3306
db_username = 'fynd_marketing_write'
db_password = 'fynd_marketing_write!2022'
db_name = 'marketing'
 
# Get current time and format time range
last_two_hours_end_time = datetime.now(timezone.utc)
last_two_hours_start_time = last_two_hours_end_time.replace(minute=0, second=0, microsecond=0) - timedelta(days=300)
 
formatted_start_time = last_two_hours_start_time.strftime('%Y-%m-%d %H:%M:%S')
formatted_end_time = last_two_hours_end_time.strftime('%Y-%m-%d %H:%M:%S')
 
# SQL query
#sql = ""
 
# Load SSH private key
ssh_key = paramiko.RSAKey.from_private_key_file(private_key_path)
 
# Establish SSH tunnel and connect to the MySQL database
with SSHTunnelForwarder(
    (ssh_host, ssh_port),
    ssh_username=ssh_username,
    ssh_pkey=ssh_key,
    remote_bind_address=(db_host, db_port)
) as tunnel:
    conn = pymysql.connect(
        host='localhost',
        port=tunnel.local_bind_port,
        user=db_username,
        password=db_password,
        db=db_name
    )
 
    # Execute the query and fetch results
    with conn.cursor() as cur:
       # cur.execute("SET sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));")
        cur.execute('''select
         
         
        r.prm_id ,
        r.partner_id,
        r.seller_name as 'Partner_Name',
        r.mobile_number,
        # r.created_date,
        rpd.approved_at,
        # a.building_name,
        # a.city as 'CITY',
        # a.district,
        # a.pincode,
        # a.state,
        # a.street_name,
        # a.floor,
        # a.house,
        # a.landmark,
        ed.email_id
        # r.store_name,
        # bd.account_number,
        # bd.bank_name,
        # bd.ifsc,
        # gst.gstin,
        # bd.bank_name
         
         
         
        from retailer r
         
        left join bank_detail bd on bd.bank_detail_id = r.bank_detail_id
        left join email_detail ed on ed.id=r.email_detail_id
        left join gst_detail gst on gst.gst_detail_id = r.gst_detail_id
         
        left join address a on a.retailer_id = r.id
         
        left join pan_detail pd on pd.pan_detail_id = r.pan_detail_id
         
        left join retailer_category rc on rc.retailer_category_id = r.retailer_category_id
         
        left join rejections rj on rj.retailer_id = r.id
         
        left join rejection_reasons rjn on rjn.rejection_reason_id = rj. rejection_reason_id
         
        left join admin adm on adm.id = r.approved_by_id
         
        left join retailer_prm_details rpd on rpd.retailer_id = r.id
         
        left join jmd_officer jo on jo.employee_id = r.assisted_by
         
        left join user_type ut on ut.id = r.user_type_id
         
        WHERE
         
        a.is_primary = 1 and
        r.partner_id is not null
        
        ''')
        results = cur.fetchall()
        columns = [column[0] for column in cur.description]
        result_dataFrame = pd.DataFrame(results, columns=columns)
        #result_dataFrame = pd.read_sql_query(query, conn)
 
    conn.close()
result_dataFrame1=result_dataFrame.drop_duplicates()
result_dataFrame1.to_csv('onboarding_data_07Jun.csv')


#current_time = datetime.datetime.now(timezone.utc)
#last_hour_start_time = current_time.replace(minute=0, second=0, microsecond=0) - timedelta(hours=15)
#last_hour_end_time = last_hour_start_time + timedelta(hours=15)














mydb = connection.connect(host="jioretailer-marketing-prod.cgszbyyjeamy.ap-south-1.rds.amazonaws.com", database = 'marketing',user="fynd_marketing_write", passwd="fynd_marketing_write!2022",use_pure=True)
 
print(mydb)
 
query = '''select
 
 
r.prm_id ,
r.partner_id,
r.seller_name as 'Partner_Name',
r.mobile_number,
a.building_name,
a.city as 'CITY',
a.district,
a.pincode,
a.state,
a.street_name,
a.floor,
a.house,
a.landmark,
ed.email_id,
r.store_name,
bd.account_number,
bd.bank_name,
bd.ifsc,
gst.gstin,
bd.bank_name
 
 
 
from retailer r
 
left join bank_detail bd on bd.bank_detail_id = r.bank_detail_id
left join email_detail ed on ed.id=r.email_detail_id
left join gst_detail gst on gst.gst_detail_id = r.gst_detail_id
 
left join address a on a.retailer_id = r.id
 
left join pan_detail pd on pd.pan_detail_id = r.pan_detail_id
 
left join retailer_category rc on rc.retailer_category_id = r.retailer_category_id
 
left join rejections rj on rj.retailer_id = r.id
 
left join rejection_reasons rjn on rjn.rejection_reason_id = rj. rejection_reason_id
 
left join admin adm on adm.id = r.approved_by_id
 
left join retailer_prm_details rpd on rpd.retailer_id = r.id
 
left join jmd_officer jo on jo.employee_id = r.assisted_by
 
left join user_type ut on ut.id = r.user_type_id
 
WHERE
 
a.is_primary = 1 and
LEFT(r.partner_id,2)='JM'
 
 
 
'''
 
#.format(fromDate = last_hour_start_time, toDate = last_hour_end_time)'
result_dataFrame = pd.read_sql(query,mydb)
 
 

date = time.strftime("%Y%m%d%H%M%S")
result_dataFrame['address']=result_dataFrame['building_name']+'-'+result_dataFrame['floor']+'-'+result_dataFrame['street_name']+'-'+result_dataFrame['house']+"-"+result_dataFrame['landmark']+'-'+result_dataFrame['CITY']+'-'+result_dataFrame['district']
result_dataFrame.drop(columns=['building_name','floor','house','landmark','CITY', 'district','street_name'] ,inplace=True)
result_dataFrame['logo']=''
result_dataFrame['mid']=''
result_dataFrame['serial_num_series']=''
result_dataFrame['account_type']=''
result_dataFrame['consent']=''
result_dataFrame['vpa_id']=''
columnsTitles = ['prm_id','partner_id','store_name','Partner_Name', 'email_id', 'mobile_number','address', 'pincode', 'state','gstin', 'mid', 'logo','serial_num_series', 'account_type',
'consent','bank_name','account_number',  'ifsc','vpa_id']
result_dataFrame = result_dataFrame[columnsTitles]
df=result_dataFrame
new_column_names = {
    'prm_id':'prm_id',
    'partner_id': 'partner_id',
    'store_name': 'name',
    'Partner_Name': 'owner',
    'email_id': 'email',
    'mobile_number': 'phone',
    'address': 'address',
    'pincode': 'pincode',
    'state': 'State',
    'gstin': 'gst_number',
    'mid': 'mid',
    'logo': 'logo',
    'serial_num_series': 'serial_num_series',
    'account_type': 'account_type',
    'consent': 'consent',
    'bank_name': 'bank_account_name',
    'account_number': 'bank_account_number',
    'ifsc': 'bank_account_ifsc',
    'vpa_id': 'vpa_id'
}
 
# Rename the columns
df = df.rename(columns=new_column_names)
 
# Now, the columns are renamed according to your requirements
 
 
# Reassign the columns attribute with the new column names
df.columns = ['prm_id', 'name', 'owner', 'email', 'phone', 'address', 'pincode', 'State', 'gst_number', 'mid', 'logo', 'serial_num_series', 'account_type', 'consent', 'bank_account_name', 'bank_account_number', 'bank_account_ifsc', 'vpa_id']
 
# Now, the columns are renamed according to your requirements
 
 
df1 = df[df['prm_id'].notna()]
 
 
 
 
#result_dataFrame.to_csv(f'/home/fynd/file_uploads_onboarding/Onboarding{date}.txt', sep = '|')
 
df1.to_csv(f'/Users/akash13.tiwari/Documents/on/Onboarding{date}.txt', sep = '|')