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

# result=hs.haversine(loc1,loc2,unit=Unit.KILOMETERS)



# Define the folder path
folder_path1 = '/Users/nitin14.patil/Library/CloudStorage/OneDrive-RelianceCorporateITParkLimited/Documents/python_work/conveyance_analysis/JPW July'

# Initialize an empty list to store DataFrames
dfs1 = []

for file in os.listdir(folder_path1):
    file_path1 = os.path.join(folder_path1, file)
    if os.path.isfile(file_path1):
        print(f"Reading file: {file_path1}")
        df = pd.read_excel(file_path1)
        dfs1.append(df)

# Concatenate DataFrames if any were found
if dfs1:
    result1 = pd.concat(dfs1, axis=0, ignore_index=True)
    print("Concatenation successful!")
else:
    print("No files were found to concatenate in folder_path1.")




df_origin=result1

df_origin.rename(columns={
    'Agentid': 'Emp ID',
    'Day of Date': 'Date',
    'PRM ID': 'PRM Id',
    'Checkin Lat': 'Lat',
    'Checkin Long': 'Long',
    'Checkin Time': 'Timestamp'
}, inplace=True)

# Forward fill missing values in all columns
df_origin.ffill(inplace=True)



df_origin = df_origin.sort_values(by=['Emp ID', 'Timestamp'])

# Begineing

df=df_origin

# df = df.sort_values(by=['Emp ID', 'Timestamp'])



df['Emp ID'] = df['Emp ID'].astype(int)
# Convert the Date column to datetime
df['Date'] = pd.to_datetime(df['Date'], errors='coerce')




# df=df[(df['Emp ID'] == 50095250)&(df['Date'] == '2024-07-01')]



print('data imported successfull.')


# # Filter the DataFrame for the specific date
# checkdf = df[(df['Emp ID'] == 50095250)&(df['Date'] == '2024-07-01')]


print('line 55 run successfully')


emp_t = '/Users/nitin14.patil/Downloads/RIL/ril/conveyance_analysis/Emp Master Final.xlsx'


excel_file_emp = pd.ExcelFile(emp_t)

sheet_names_emp = excel_file_emp.sheet_names

df_emp = pd.read_excel(excel_file_emp)

# df_ca['JMDO/JMDL']=df_ca['\tJMDO/JMDL']


# df_ca = df.merge(df, df_emp, how='left', left_on='Emp ID', right_on='EM')






# df=df[df['Emp ID']==50123511]

# df=df_ca


df.columns


# def haversine(lat1, lon1, lat2, lon2):
#     R = 6371.0  # Radius of Earth in kilometers
#     lat1, lon1 = math.radians(lat1), math.radians(lon1)
#     lat2, lon2 = math.radians(lat2), math.radians(lon2)
#     dlon, dlat = lon2 - lon1, lat2 - lat1
#     a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
#     c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
#     return R * c


# Convert Timestamp to datetime, handling mixed formats
df['Timestamp'] = pd.to_datetime(df['Timestamp'], format="%Y-%m-%d %H:%M:%S.%f", errors='coerce')

# Step 1: Convert the 'Date' column to datetime format
df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
print('line 100 run successfully')


# # Step 2: Extract only the date part
# df['Date'] = df['Date'].dt.date


# Filter the DataFrame for the specific date
checkdf = df[(df['Emp ID'] == 50095250)&(df['Date'] == '2024-07-01')]

df.dtypes

# Initialize a list to store results
results = []

# from haversine import haversine, Unit

# Group by Employee ID and Date to calculate distances within each group
for (emp_id, date), group in df.groupby(['Emp ID', 'Date']):
    # Reset the index for the group
    group = group.reset_index(drop=True)
    
    for i in range(len(group) - 1):
        lat1, lon1 = group.loc[i, 'Lat'], group.loc[i, 'Long']
        lat2, lon2 = group.loc[i+1,'Lat'], group.loc[i+1,'Long']

        
        loc1 = (lat1, lon1)
        loc2 = (lat2, lon2)
        
        # print(loc1,loc2)
        
        # Calculate the Haversine distance in kilometers
        distance = haversine(loc1, loc2, unit=Unit.KILOMETERS)
        
        # Calculate time difference in hours
        time_diff = (group.loc[i+1, 'Timestamp'] - group.loc[i, 'Timestamp']).total_seconds() / 3600.0
        
        # Calculate distance per hour
        distance_per_hour = distance / time_diff if time_diff > 0 else 0.0
        
        results.append({
            'Emp ID': emp_id,
            'Date': date,
            'PRM Id': group.loc[i, 'PRM Id'],
            'Lat': lat1,
            'Long': lon1,
            'Timestamp': group.loc[i, 'Timestamp'],
            'Distance(KM)': distance,
            'Speed of Travel (KM/Hr)': distance_per_hour,

        })
    
    # Append the last row with a distance of 0 and Distance Per Hour of 0
    results.append({
        'Emp ID': emp_id,
        'Date': date,
        'PRM Id': group.loc[len(group) - 1, 'PRM Id'],
        'Lat': group.loc[len(group) - 1, 'Lat'],
        'Long': group.loc[len(group) - 1, 'Long'],
        'Timestamp': group.loc[len(group) - 1, 'Timestamp'],
        'Distance(KM)': 0.00,
        'Speed of Travel (KM/Hr)': 0.00,

    })
    
    
print('line 155 run successfully')
    

# Convert the results list to a DataFrame
df_with_distances = pd.DataFrame(results)

print('Distance and Distance/h column added')


df_with_distances['Distance(KM)'] = df_with_distances['Distance(KM)'].round(2)
df_with_distances['Speed of Travel (KM/Hr)'] = df_with_distances['Speed of Travel (KM/Hr)'].round(2)


print('Digits converted to 2 desimal points')


# Filter the DataFrame for the specific date
checkdf = df_with_distances[(df_with_distances['Emp ID'] == 50095250)&(df_with_distances['Date'] == '2024-07-01')]

df_ca = df_with_distances.merge(df_emp, how='left', left_on='Emp ID', right_on='EMP Id')


print('JMDO/JMDL column added')



df_ca=df_ca[df_ca['JMDO/JMDL'].isin(['JMDO','JMDL'])]




df_ca['Emp ID']=df_ca['Emp ID'].astype(int)

df_ca['Date123']=pd.to_datetime(df_ca['Date'])

checkdf= df_ca[df_ca['Emp ID']&(df_ca['Date123']=='2024-07-01')]



df_ca.dtypes


#start from here

d_df=df_ca


# # Reset the index in both DataFrames to align them by their row order
# df_with_distances.reset_index(drop=True, inplace=True)
# df.reset_index(drop=True, inplace=True)

# # Then perform the concatenation
# d_df = pd.concat([df_with_distances, df[['JMDO/JMDL']]], axis=1)


# print(df_with_distances)
d_df['70_Distance(KM)_zero_n'] = d_df.groupby(['Emp ID', 'Date'])['Distance(KM)'].transform(
    lambda x: x.apply(lambda y: 0 if y > 70 else y))


d_df['new_distance_n'] = d_df['Distance(KM)'].where(d_df['Distance(KM)'] <= 70, 0)




# Filter the DataFrame for the specific date
checkdf = d_df[(d_df['Emp ID'] == 50095250)&(d_df['Date'] == '2024-07-01')]




d_df.dtypes




d_df['Distance is more than 10 Km']=d_df['Distance(KM)'].apply(lambda x: 'Yes' if x > 10 else 'No')

d_df['Distance is more than 25 Km']=d_df['Distance(KM)'].apply(lambda x: 'Yes' if x > 25 else 'No')

d_df['Speed>70 KM/HR']=d_df['Speed of Travel (KM/Hr)'].apply(lambda x: 'Yes' if x > 70 else 'No')

print('code completed till "Speed>70 KM/HR" column')

d_df['Attendance chk'] = d_df.apply(
    lambda row: "Yes" if (
        (row['JMDO/JMDL'] == "JMDO" and row['Emp ID'] == "Attendance" and row['Distance(KM)'] > 20) or
        (row['JMDO/JMDL'] == "JMDL" and row['Emp ID'] == "Attendance" and row['Distance(KM)'] > 40)
    ) else "No", axis=1
)


# Define the bin edges and labels
bins = [0, 60, 70, 80, 100, float('inf')]
labels = ['0-60KM', '60-70KM', '70-80KM', '80-100KM', '>100KM']

# Create the 'Distance Bucket' column
d_df['Speed Bucket'] = pd.cut(d_df['Speed of Travel (KM/Hr)'], bins=bins, labels=labels, right=False)

# Define the bin edges and labels
bins = [0, 10, 15, 20, 30, 60, float('inf')]
labels = ['0-10KM', '10-15KM', '15-20KM', '20-30KM', '30-60KM', '>60KM']

# Create the 'Distance Bucket' column
d_df['KM Bucket'] = pd.cut(d_df['Speed of Travel (KM/Hr)'], bins=bins, labels=labels, right=False)


d_df['beat > 10 KM']=d_df['Distance(KM)'].apply(lambda x: 'Yes' if x > 10 else 'No')

from datetime import datetime, date  # Import date directly

# Define the Excel start date as a datetime object
excel_start_date = datetime(1900, 1, 1)

# Define a function to convert a date to Excel serial number
def date_to_excel_serial(date_obj):
    # Check if the date is a Timestamp, convert to datetime if necessary
    if isinstance(date_obj, pd.Timestamp):
        date_obj = date_obj.to_pydatetime()  # This will be a datetime object
    elif isinstance(date_obj, str):
        date_obj = datetime.strptime(date_obj, "%d/%m/%Y")
    elif isinstance(date_obj, date):  # Check for datetime.date
        # Convert to datetime if it is a date object
        date_obj = datetime.combine(date_obj, datetime.min.time())

    serial_number = (date_obj - excel_start_date).days + 2
    return serial_number

# Apply the function to create the 'date_value' column
d_df['date_value'] = d_df['Date'].apply(date_to_excel_serial)

# Corrected code for conditionally creating 'CC' column
d_df['CC'] = (d_df['Emp ID'].astype(str) + d_df['date_value'].astype(str)).astype(int)


#Pivot
d_df.columns

# Assuming d_df is your original DataFrame
df_fp = d_df[[
    'Emp ID',
    'Date',
    'PRM Id',
    'Lat',
    'Long',
    'Timestamp',
    'Distance(KM)',
    'Speed of Travel (KM/Hr)',
]]

# Group by 'Emp ID' and 'Date' and sum the 'Distance(KM)'
result12 = df_fp.groupby(['Emp ID', 'Date'])['Distance(KM)'].sum().reset_index()

# Rename the summed column
result12.rename(columns={'Distance(KM)': 'Sum of Distance(KM)'}, inplace=True)

# Apply the function to create the 'date_value' column
result12['date_value'] = result12['Date'].apply(date_to_excel_serial)

# Add a new column with concatenated 'Emp ID' and 'Date'
result12['CC'] = result12['Emp ID'].astype(str) + result12['date_value'].astype(str)

# Convert 'CC' in result12 to int64
result12['CC'] = result12['CC'].astype(int)

# Now merge the DataFrames
d_df = pd.merge(d_df, result12, on='CC', how='left')

d_df.drop(columns=['Emp ID_y',	'Date_y',	'date_value_y'], inplace=True)

d_df.rename(columns={
    'Emp ID_x':'Emp ID',
'Date_x':'Date',
'date_value_x':'date_value',
'Sum of Distance(KM)':'Daily KMs'
}, inplace=True)


d_df['Daily KMs chk- Flag A']= np.where(
    ((d_df['JMDO/JMDL'] == "JMDO") & (d_df['Daily KMs'] > 75)) | ((d_df['JMDO/JMDL'] == "JMDL") & (d_df['Daily KMs'] > 120)),
    "Yes", "No"
)

# Filter the DataFrame for the specific date
checkdf = d_df[(d_df['Emp ID'] == 50095250)&(d_df['Date'] == '2024-07-01')]


d_df['Line >10 KM']=np.where(
    (d_df['PRM Id'] == "Attendance") & (d_df['Distance(KM)']>10), 'Yes', 'No'
    
    )


# Filter the DataFrame for the specific date
checkdf = d_df[(d_df['Emp ID'] == 50095250)&(d_df['Date'] == '2024-07-01')]


#Pivot for flag B

# Group by emp_id and create Check In/Out remarks
d_df['Is last checkin to Mark-out'] = ''

# Filter only 'Attendance' rows and determine the min and max timestamps per employee
attendance_df = d_df[d_df['PRM Id'] == 'Attendance']

# # Check In for min timestamp
# min_timestamps = attendance_df.groupby('Emp ID')['Timestamp'].min()

# Extract date from the Timestamp for grouping
attendance_df['Date'] = attendance_df['Timestamp'].dt.date

# Find the minimum timestamp for each employee on each day
min_timestamps = attendance_df.groupby(['Emp ID', 'Date'])['Timestamp'].min().reset_index()

# Merge with the original DataFrame to get the full attendance record for the minimum timestamp
attendance_of_min_time = pd.merge(min_timestamps, attendance_df, on=['Emp ID', 'Date', 'Timestamp'], how='left')

print(attendance_of_min_time)


d_df.loc[d_df['Timestamp'].isin(attendance_of_min_time['Timestamp']) & (d_df['PRM Id'] == 'Attendance'), 'Is last checkin to Mark-out'] = 'Mark In'

checkdf= d_df[(d_df['Emp ID']==50123511) & (d_df['Date']=='2024-07-05')]



# Filter the DataFrame for the specific date
checkdf = d_df[(d_df['Emp ID'] == 50095250)&(d_df['Date'] == '2024-07-01')]



# Find the minimum timestamp for each employee on each day
max_timestamps = attendance_df.groupby(['Emp ID', 'Date'])['Timestamp'].max().reset_index()


# Merge to find matching rows
merged = max_timestamps.merge(min_timestamps, on=['Emp ID', 'Date'], how='inner')

df1_filtered = merged[merged['Timestamp_x'] != merged['Timestamp_y']]

# Reset index if needed
df1_filtered.reset_index(drop=True, inplace=True)

print(df1_filtered)


d_df.loc[d_df['Timestamp'].isin(df1_filtered['Timestamp_x']) & (d_df['PRM Id'] == 'Attendance'), 'Is last checkin to Mark-out'] = 'Mark Out'


checkdf= d_df[(d_df['Emp ID']==50123511) & (d_df['Date']=='2024-07-05')]

b_df = d_df[(d_df['Line >10 KM']=='Yes')&(d_df['Is last checkin to Mark-out']=='Mark In')]

# Group by 'Emp ID' and 'Date' and sum the 'Distance(KM)'
result13 = b_df.groupby(['Emp ID'])['Line >10 KM'].count().reset_index()

# Now merge the DataFrames
d_df = pd.merge(d_df, result13, on='Emp ID', how='left')

d_df.rename(columns={
     'Line >10 KM_x' : 'Line >10 KM',
    'Line >10 KM_y':'count >10KM check in'
    
    }, inplace=True)


d_df.columns


d_df['count >10KM check in']=d_df['count >10KM check in'].round(0)


d_df['Count of 8 Occurence >10KM- Flag B'] =np.where(
    (d_df['Is last checkin to Mark-out']=='Mark In') &  
    (d_df['count >10KM check in']>8)&
    (d_df['Distance(KM)'].round(0)>10),
    'Yes',
    'No'
    )

checkdf= d_df[(d_df['Emp ID']==50123511)]









# Count occurrences of each CC value per Emp ID and Date
count_cc = d_df.groupby(['CC']).size().reset_index(name='Count')




d_df = pd.merge(d_df,count_cc,how='left',left_on='CC',right_on='CC')

d_df.rename(columns={'Count':'Occurrance'},inplace=True)

# Set values in column 'B' to NaN where 'A' > 3
d_df.loc[d_df['CC'] == '', 'Occurrance'] = ''

d_df['Occurrance Chk'] = d_df['beat > 10 KM'].apply(lambda x: 'Yes' if x == 'Yes' else 'No')


df = d_df


def add_sequential_occurrance_column_with_where(df):
    # Convert 'Date' to datetime format if it's not already
    df['Date'] = pd.to_datetime(df['Date'], format='%d/%m/%y')

    # Create a mask for the rows that meet the conditions
    mask = (df['Occurrance'] != '') & (df['Occurrance Chk'] == 'Yes')


    df['Occurance of more than 10 Km Distance on the same day'] = df[mask].groupby(['CC', 'Date']).cumcount() + 1

    # Use `where` to keep NaN where conditions are not met
    df['Occurance of more than 10 Km Distance on the same day'] = df['Occurance of more than 10 Km Distance on the same day'].where(mask, '')

    return df

df = add_sequential_occurrence_column_with_where(df)


# Group by emp_id and create Check In/Out remarks
df['Is last checkin to Mark-out'] = ''

# Filter only 'Attendance' rows and determine the min and max timestamps per employee
attendance_df = df[df['PRM Id'] == 'Attendance']

# # Check In for min timestamp
# min_timestamps = attendance_df.groupby('Emp ID')['Timestamp'].min()

# Extract date from the Timestamp for grouping
attendance_df['Date'] = attendance_df['Timestamp'].dt.date

# Find the minimum timestamp for each employee on each day
min_timestamps = attendance_df.groupby(['Emp ID', 'Date'])['Timestamp'].min().reset_index()

# Merge with the original DataFrame to get the full attendance record for the minimum timestamp
attendance_of_min_time = pd.merge(min_timestamps, attendance_df, on=['Emp ID', 'Date', 'Timestamp'], how='left')

print(attendance_of_min_time)


df.loc[df['Timestamp'].isin(attendance_of_min_time['Timestamp']) & (df['PRM Id'] == 'Attendance'), 'Is last checkin to Mark-out'] = 'Mark In'

checkdf= df[(df['Emp ID']==50123511) & (df['Date']=='2024-07-05')]



# Filter the DataFrame for the specific date
checkdf = d_df[(d_df['Emp ID'] == 50095250)&(d_df['Date'] == '2024-07-01')]



# Find the minimum timestamp for each employee on each day
max_timestamps = attendance_df.groupby(['Emp ID', 'Date'])['Timestamp'].max().reset_index()


# Merge to find matching rows
merged = max_timestamps.merge(min_timestamps, on=['Emp ID', 'Date'], how='inner')

df1_filtered = merged[merged['Timestamp_x'] != merged['Timestamp_y']]

# Reset index if needed
df1_filtered.reset_index(drop=True, inplace=True)

print(df1_filtered)


df.loc[df['Timestamp'].isin(df1_filtered['Timestamp_x']) & (df['PRM Id'] == 'Attendance'), 'Is last checkin to Mark-out'] = 'Mark Out'


checkdf= df[(df['Emp ID']==50123511) & (df['Date']=='2024-07-05')]



# checkdf.to_csv('test_1.csv')
# df.to_csv('test_0.csv')

df['Is Mark-In & Markout'] = df['Is last checkin to Mark-out'].apply(lambda x: 'Yes' if x != '' else 'No')

# Assuming your DataFrame is named df
# Group by the PRM column and calculate the median Lat for each group
median_lat_by_prm = df.groupby('PRM Id')['Lat'].median()

# Map these median values back to the original DataFrame
df['Median Lat of PRM'] = df['PRM Id'].map(median_lat_by_prm)

# Assuming your DataFrame is named df
# Group by the PRM column and calculate the median Lat for each group
median_long_by_prm = df.groupby('PRM Id')['Long'].median()

# Map these median values back to the original DataFrame
df['Median Long of PRM'] = df['PRM Id'].map(median_long_by_prm)
def custom_check(row):
    if row['CC'] == "JMDL" and row['Is Mark-In & Markout'] == "Mark In" and row['Distance(KM)'] > 40:
        return "yes"
    elif row['CC'] == "JMDL" and row['Is Mark-In & Markout'] == "Mark Out" and row['Distance(KM)'] > 40:
        return "yes"
    elif row['CC'] == "JMDO" and row['Is Mark-In & Markout'] == "Mark In" and row['Distance(KM)'] > 20:
        return "yes"
    elif row['CC'] == "JMDO" and row['Is Mark-In & Markout'] == "Mark Out" and row['Distance(KM)'] > 20:
        return "Yes"
    else:
        return "No"

# Apply the function to the DataFrame
df['Mark in & Out>10KM'] = df.apply(custom_check, axis=1)

df['Occurrence KM'] = df.apply(
    lambda row: row['Distance(KM)'] / row['Occurrence'] * 2 
    if row['Occurrence'] != '' 
    else row['Distance(KM)'], 
    axis=1
)


# Create a shifted version of the 'Distance(KM)' column
d_df['Distance(KM)_shift'] = d_df['Distance(KM)'].shift(1)



# Filter the DataFrame for the specific date
checkdf = d_df[(d_df['Emp ID'] == 50095250)&(d_df['Date'] == '2024-07-01')]







def custom_check_v2(row):
    if row['JMDO/JMDL'] == "JMDL" and row['Is last checkin to Mark-out'] == "Mark In" and row['Distance(KM)'] > 40:
        return (row['Distance(KM)'])
    elif row['JMDO/JMDL'] == "JMDL" and row['Is last checkin to Mark-out']=="Mark Out" and row['Distance(KM)_shift'] > 40:
        return (row['Distance(KM)'])
    if row['JMDO/JMDL'] == "JMDO" and row['Is last checkin to Mark-out'] == "Mark In" and row['Distance(KM)'] > 20:
        return (row['Distance(KM)'])
    elif row['JMDO/JMDL'] == "JMDO" and row['Is last checkin to Mark-out']=="Mark Out" and row['Distance(KM)_shift'] > 20:
        return (row['Distance(KM)'])
    # else:
    #     return ""
# Apply the function to the DataFrame
df['Max KM for Mark in & Mark out_n'] = df.apply(custom_check_v2, axis=1)

checkdf= df[(df['Emp ID']==50123511)]

# & (df['Date']=='2024-07-18')



df['new_distance_n'] = d_df.apply(lambda row: (
    40 if row['JMDO/JMDL'] == "JMDL" and row['Is last checkin to Mark-out'] == "Mark In" and row['Distance(KM)'] > 40 else
    40 if row['JMDO/JMDL'] == "JMDL" and row['Is last checkin to Mark-out'] == "Mark Out" and row['Distance(KM)_shift'] > 40 else
    20 if row['JMDO/JMDL'] == "JMDO" and row['Is last checkin to Mark-out'] == "Mark In" and row['Distance(KM)'] > 20 else
    20 if row['JMDO/JMDL'] == "JMDO" and row['Is last checkin to Mark-out'] == "Mark Out" and row['Distance(KM)_shift'] > 20 else
    row['new_distance_n']  # default case if no conditions are met
), axis=1)

checkdf= df[(df['Emp ID']==50123511) & (df['Date'] =='2024-07-05')]
    
    

# Filter the DataFrame for the specific date
checkdf = df[(df['Emp ID'] == 50095250)&(df['Date'] == '2024-07-01')]




# Haversine formula to calculate the distance between two lat/long pairs
def haversine(lat1, lon1, lat2, lon2):
    R = 6371  # Radius of the Earth in kilometers
    dlat = np.radians(lat2 - lat1)
    dlon = np.radians(lon2 - lon1)
    a = np.sin(dlat/2) * np.sin(dlat/2) + np.cos(np.radians(lat1)) * np.cos(np.radians(lat2)) * np.sin(dlon/2) * np.sin(dlon/2)
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1-a))
    distance = R * c
    return distance

# Function to calculate distance if column value is "Yes"
def calculate_distance_if_yes(row, prev_row):
    if row['Is Mark-In & Markout'] == "Yes":
        return haversine(prev_row['Lat'], prev_row['Long'], row['Lat'], row['Long'])
    else:
        return np.nan  # Return NaN or another placeholder if not "Yes"

# Assuming your DataFrame is named df
df['Distance between mean to actual'] = np.nan  # Initialize the column with NaN values



for i in range(1, len(df)):
    if df.loc[i, 'Is Mark-In & Markout'] != "Yes":
        df.loc[i, 'Distance between mean to actual'] = calculate_distance_if_yes(df.loc[i], df.loc[i-1])

# Optionally, you might want to set NaN values to 0 or another placeholder based on your requirements
df['Distance between mean to actual'].fillna(0, inplace=True)



df['Distance is more than 1 Km'] = df['Median Lat of PRM'].apply(
    lambda x: 'Yes' if x == 1 else 'No'
)



# Function to set 'KM post Speed' based on the condition
def calculate_km_post_speed(row):
    if row['Speed>70 KM/HR'] == 'Yes':
        return 0
    else:
        return row['Distance(KM)']

# Apply the function to the DataFrame
df['KM post Speed1'] = df.apply(calculate_km_post_speed, axis=1)



df['KM post Speed'] = df.groupby(['Emp ID', 'Date'])['KM post Speed1'].transform(
    lambda x: 0 if (x > 70).any() else x
)


# Filter the DataFrame for the specific date
checkdf = df[(df['Emp ID'] == 50095250)&(df['Date'] == '2024-07-01')]



def apply_formula(row):
    if row['JMDO/JMDL'] == "JMDL" and row['Is last checkin to Mark-out'] == "Mark In":
        return min(row['Distance(KM)'], 20)
    elif row['JMDO/JMDL'] == "JMDL" and row['Is last checkin to Mark-out'] == "Mark Out":
        return min(row['Distance(KM)'], 30)
    elif row['JMDO/JMDL'] == "JMDO" and row['Is last checkin to Mark-out'] == "Mark In":
        return min(row['Distance(KM)'], 10)
    elif row['JMDO/JMDL'] == "JMDO" and row['Is last checkin to Mark-out'] == "Mark Out":
        return min(row['Distance(KM)'], 15)
    else:
        return ""

# Assuming your DataFrame is named df
df['KM post Mark in & Out'] = df.apply(apply_formula, axis=1)

# Apply the conditions
df['Max KM'] = np.where(
    df['JMDO/JMDL'] == 'JMDO',
    np.where(df['Distance(KM)'] > 30, '', df['Distance(KM)']),
    np.where(df['JMDO/JMDL'] == 'JMDL',
             np.where(df['Distance(KM)'] > 60, '', df['Distance(KM)']),
             '')
)





# Assuming your DataFrame is named df
df['No issue'] = df.apply(lambda x: x['Distance(KM)'] if x['Speed>70 KM/HR'] == "No" and x['Occurrence Chk'] == "No" and x['Mark in & Out>10KM'] == "No" else '', axis=1)
    


checkdf= df[(df['Emp ID']==50093903) & (df['Date']=='2024-07-18')]



# Specify the columns to convert
columns_to_convert = ['KM post Speed', 'KM post Mark in & Out', 'Occurrence KM', 'Max KM', 'No issue']

# Replace empty strings and non-numeric values with NaN
df[columns_to_convert] = df[columns_to_convert].replace('', np.nan)
df[columns_to_convert] = df[columns_to_convert].apply(pd.to_numeric, errors='coerce')

# Convert columns to float
df[columns_to_convert] = df[columns_to_convert].astype(float)

# Calculate the minimum value across the specified columns
df['Final KM'] = df[['KM post Speed', 'KM post Mark in & Out', 'Occurrence KM', 'Max KM', 'No issue']].min(axis=1)

df['Diff KM'] = (df['Distance(KM)'] - df['Final KM']).round(2)


# Filter the DataFrame for the specific date
checkdf = df[(df['Emp ID'] == 50095250)&(df['Date'] == '2024-07-01')]





def custom_check_v23(row):
    if row['Is last checkin to Mark-out'] == "Mark In":
        return "Mark In"
    elif row['Is last checkin to Mark-out'] == "Mark Out":
        return "Mark Out"
    else:
        return "Beat"

# Apply the function to the DataFrame
df['Travel type'] = df.apply(custom_check_v23, axis=1)

checkdf= df[(df['Emp ID']==50093903) & (df['Date']=='2024-07-18')]



df['LAT & LONG']= df['Lat'].astype(str) + ',' + df['Long'].astype(str)

df['Observation']=''

# df.to_csv('Conveyance Analysis Adhoc.csv')

df.columns


df['Distance(KM)'] = np.where(
    (df['PRM Id'] == 'Attendance') & 
    (df['Is last checkin to Mark-out'] == 'Mark Out'),
    df['Distance(KM)'].shift(1),  # Value if condition is True
    df['Distance(KM)']  # Value if condition is False
)

# Filter the DataFrame for the specific date
checkdf = df[(df['Emp ID'] == 50095250)&(df['Date'] == '2024-07-01')]


df['new_Distance(KM)'] = df.apply(
    lambda row: 20 if (
        row['Is last checkin to Mark-out'] in ['Mark In', 'Mark Out'] and 
        row['JMDO/JMDL'] == 'JMDO' and 
        row['PRM Id'] == 'Attendance' and 
        row['Distance(KM)'] > 20
    ) else (
        40 if (
            row['Is last checkin to Mark-out'] in ['Mark In', 'Mark Out'] and 
            row['JMDO/JMDL'] == 'JMDL' and 
            row['PRM Id'] == 'Attendance' and 
            row['Distance(KM)'] > 40
        ) 
        else row['Distance(KM)']
    ), 
    axis=1
)

        
df['new_Distance(KM)'] = df.apply(
    lambda x: 0 if (
        x['Speed of Travel (KM/Hr)'] > 70 and 
        x['Is last checkin to Mark-out'] not in ['Mark In', 'Mark Out']
    ) else x['new_Distance(KM)'], 
    axis=1
)



# Group by 'Emp ID' and 'Date' and sum the distances
grouped_jmdo = df.groupby(['Emp ID', 'Date'])['Distance(KM)'].sum().reset_index()
grouped_jmdo = grouped_jmdo[grouped_jmdo['Distance(KM)'] > 70]


df_l = df

df_l.columns
# Replace empty strings with a default value (e.g., 0)
df_l['Occurrence'] = df_l['Occurrence'].replace('', '0')

# Convert to integer
df_l['Occurrence'] = df_l['Occurrence'].astype(int)

import numpy as np

# Set default value for 'Flag A'
df_l['Flag A'] = 'No'

# Create a mask for the conditions
mask = (
    (df_l['JMDO/JMDL'] == 'JMDO') & 
    (df_l['Speed of Travel (KM/Hr)'] > 75) & 
    (df_l['Emp ID'].isin(grouped_jmdo['Emp ID'])) & 
    (df_l['Date'].isin(grouped_jmdo['Date']))
)

# Assign 'Analyse' where the mask is True
df_l.loc[mask, 'Flag A'] = 'Analyse'


df_l.columns


import pandas as pd

# Step 1: Create a boolean mask for filtering
mask = (
    grouped_jmdo['Emp ID'].isin(df_l['Emp ID']) & 
    grouped_jmdo['Date'].isin(df_l['Date'])
)

# Filter df_l based on the conditions
filtered_df = df_l[df_l['Is last checkin to Mark-out'] == 'Mark In'].copy()

# Step 2: Extract month from 'Date' column
filtered_df['Month'] = pd.to_datetime(filtered_df['Date']).dt.to_period('M')

# Step 3: Group and calculate occurrences
occurrences_df = (
    filtered_df.groupby(['Emp ID', 'Month'])
    .agg(Occurrence=('Distance(KM)', lambda x: (x > 10).sum()))
    .reset_index()
)

# Step 4: Merge occurrences with df_l
df_l['Month'] = pd.to_datetime(df_l['Date']).dt.to_period('M')
df_l = df_l.merge(occurrences_df, on=['Emp ID', 'Month'], how='left')


df_l.columns

# Step 5: Create Flag B based on the conditions
df_l['Flag B'] = np.where(
    (df_l['JMDO/JMDL'] == 'JMDO') & 
    (df_l['Speed of Travel (KM/Hr)'] > 75) & 
    (df_l['Occurrence_x'] < 8) & 
    (df_l['Is last checkin to Mark-out'] == 'Mark In') & 
    (df_l['Distance(KM)'] > 10), 
    'Analyse', 
    ''
)


# Filter the DataFrame for the specific date
checkdf = df_l[(df_l['Emp ID'] == 50095250)&(df_l['Date'] == '2024-07-01')]



checkdf= df_l[(df_l['Flag B']=='Analyse')]

import pandas as pd
import numpy as np

# Ensure the 'Date' column is in datetime format (do this only once)
df_l['Date'] = pd.to_datetime(df_l['Date'])

# Create a boolean mask to filter rows that exist in grouped_jmdo
mask = df_l[['Emp ID', 'Date']].apply(tuple, axis=1).isin(grouped_jmdo[['Emp ID', 'Date']].apply(tuple, axis=1))

# Filter df_l based on the mask
filtered_df = df_l[mask].copy()

# Step 3: Group by 'Emp ID' and 'Date' and count occurrences where 'Distance(KM)' > 10 km
occurrences_df_c = (
    filtered_df.groupby(['Emp ID', 'Date'])
    .agg(Occurrences_c=('Distance(KM)', lambda x: (x > 10).sum()))
    .reset_index()
)

# Rename the column if needed
# occurrences_df_c.rename(columns={'Occurrences_c': 'Occurrence_c'}, inplace=True)

# Step 6: Merge the occurrences back to the original DataFrame
df_l = pd.merge(df_l, occurrences_df_c, on=['Emp ID', 'Date'], how='left')

# Step 7: Create Flag C based on the conditions
df_l['Flag C'] = np.where(
    (df_l['JMDO/JMDL'] == 'JMDO') & 
    (df_l['Speed of Travel (KM/Hr)'] > 75) & 
    (df_l['Occurrences_c'] > 3) & 
    (df_l['Distance(KM)'] > 10), 
    'Analyse', 
    ''
)


checkdf= df_l[(df_l['Flag C']=='Analyse')]


# Filter the DataFrame for the specific date
checkdf = df[(df['Emp ID'] == 50095250)&(df['Date'] == '2024-07-01')]


#FOR JMDL
import numpy as np

# Set default value for 'Flag A'
df_l['Flag A'] = 'No'

# Create a mask for the conditions
mask = (
    (df_l['JMDO/JMDL'] == 'JMDL') & 
    (df_l['Speed of Travel (KM/Hr)'] > 120) & 
    (df_l['Emp ID'].isin(grouped_jmdo['Emp ID'])) & 
    (df_l['Date'].isin(grouped_jmdo['Date']))
)

# Assign 'Analyse' where the mask is True
df_l.loc[mask, 'Flag A'] = 'Analyse'

checkdf= df_l[(df_l['Flag A']=='Analyse') & (df_l['JMDO/JMDL']=='JMDL')]


df_l.columns



# Filter the DataFrame for the specific date
checkdf = df_l[(df_l['Emp ID'] == 50095250)&(df_l['Date'] == '2024-07-01')]


df_ll=df_l

import pandas as pd

# Step 1: Create a boolean mask for filtering
mask = (
    grouped_jmdo['Emp ID'].isin(df_ll['Emp ID']) & 
    grouped_jmdo['Date'].isin(df_ll['Date'])
)

# Filter df_l based on the conditions
filtered_df = df_ll[df_ll['Is last checkin to Mark-out'] == 'Mark In'].copy()

# Step 2: Extract month from 'Date' column
filtered_df['Month'] = pd.to_datetime(filtered_df['Date']).dt.to_period('M')

# Step 3: Group and calculate occurrences
occurrences_df = (
    filtered_df.groupby(['Emp ID', 'Month'])
    .agg(Occurrence=('Distance(KM)', lambda x: (x > 10).sum()))
    .reset_index()
)


# Step 4: Merge occurrences with df_l
df_ll['Month'] = pd.to_datetime(df_ll['Date']).dt.to_period('M')
# Merge with suffixes
df_ll = df_ll.merge(occurrences_df, on=['Emp ID', 'Month'], how='left', suffixes=('', '_occurrences'))



# Filter the DataFrame for the specific date
checkdf = df_ll[(df_ll['Emp ID'] == 50095250)&(df_ll['Date'] == '2024-07-01')]


# Step 5: Create Flag B based on the conditions
df_ll['Flag B'] = np.where(
    (df_ll['JMDO/JMDL'] == 'JMDL') & 
    (df_ll['Speed of Travel (KM/Hr)'] > 120) & 
    (df_ll['Occurrence'] < 8) & 
    (df_ll['Is last checkin to Mark-out'] == 'Mark In') & 
    (df_ll['Distance(KM)'] > 10), 
    'Analyse', 
    ''
)

# Filter the DataFrame for the specific date
checkdf = df_ll[(df_ll['Emp ID'] == 50095250)&(df_ll['Date'] == '2024-07-01')]


checkdf= df_ll[(df_ll['Flag B']=='Analyse')]



df_=df_ll


import pandas as pd
import numpy as np

# Ensure the 'Date' column is in datetime format (do this only once)
df_['Date'] = pd.to_datetime(df_['Date'])

# Create a boolean mask to filter rows that exist in grouped_jmdo
mask = df_[['Emp ID', 'Date']].apply(tuple, axis=1).isin(grouped_jmdo[['Emp ID', 'Date']].apply(tuple, axis=1))

# Filter df_l based on the mask
filtered_df = df_[mask].copy()

# Step 3: Group by 'Emp ID' and 'Date' and count occurrences where 'Distance(KM)' > 10 km
occurrences_df_c = (
    filtered_df.groupby(['Emp ID', 'Date'])
    .agg(Occurrences_c=('Distance(KM)', lambda x: (x > 10).sum()))
    .reset_index()
)

# Rename the column if needed
# occurrences_df_c.rename(columns={'Occurrences_c': 'Occurrence_c'}, inplace=True)

# Step 6: Merge the occurrences back to the original DataFrame
df_ = pd.merge(df_, occurrences_df_c, on=['Emp ID', 'Date'], how='left')




# Step 7: Create Flag C based on the conditions
df_['Flag C'] = np.where(
    (df_['JMDO/JMDL'] == 'JMDO') & 
    (df_['Speed of Travel (KM/Hr)'] > 120) & 
    (df_['Occurrence_x'] > 3) & 
    (df_['Distance(KM)'] > 10), 
    'Analyse', 
    ''
)


checkdf= df_[(df_['Flag C']=='Analyse')]


# # Filter the DataFrame for the specific date
# checkdf = df[(df['Emp ID'] == 50095250)&(df['Date'] == '2024-07-01')]


df_.to_csv('test.csv')

checkdf= df_[(df_['Emp ID']==50095250) & (df_['Date']=='2024-07-01')&(df_['Speed>70 KM/HR']=='Yes')]




