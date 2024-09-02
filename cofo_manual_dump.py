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


collection = db.cofo_manual_credit_update


date= time.strftime("%Y%m%d%H%M%S")

cursor= collection.find()
list_curs = list(cursor)
print(len(list_curs))
df1 = pd.DataFrame(list_curs)


print(df1.columns)

# # Filter using isin() method for multiple cities
# filtered_df = df1[df1['store_code'].isin([
    
    
# 'T5WM',
# 'T5ZU',
# 'T6CA',
# 'T6DU',
# 'T5SS',
# 'T6NG',
# 'T6FL',
# 'T6CW',
# 'T6CC',
# 'T5TG',
# 'T6CV',
# 'T6ET',
# 'T6JE',
# 'T6MT',
# 'T6FN',
# 'T6HK',
# 'T6HX',
# 'T6BZ',
# 'T6BK',
# 'T6NA',
# 'T6EX',
# 'T6HW',
# 'T5SV',
# 'T6BX',
# 'T6DR',
# 'T6HJ',
# 'T6DQ',
# 'T6LR',
# 'T6AS',
# 'T6DA',
# 'T6EA',
# 'T6HN',
# 'T6DG',
# 'T6KF',
# 'T6FS',
# 'T6JI',
# 'T6NX',
# 'T5YT',
# 'T5ZY',
# 'T6CX',
# 'T6HP',
# 'T6HM',
# 'T6NB',
# 'T6PI',
# 'T6FO',
# 'T6VT',
# 'T6OH',
# 'T5TL',
# 'T5YQ',
# 'T6CH',
# 'T6HY',
# 'T6RE',
# 'T6MZ',
# 'T6JT',
# 'T5PV',
# 'T6JL',
# 'T6QV'


# ])]
# print(filtered_df)



# filtered_df.to_csv(f'/Users/Nitin14.Patil/Documents/MONGODB/cofo_manual_credit_update_{date}.csv', index=False)


# for all stores

df1.to_csv(f'/Users/Nitin14.Patil/Documents/MONGODB/cofo_manual_credit_update_{date}.csv', index=False)




