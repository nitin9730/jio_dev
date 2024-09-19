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

folder_path1 = '/Users/nitin14.patil/Library/CloudStorage/OneDrive-RelianceCorporateITParkLimited/Documents/python_work/conveyance_analysis/JPW Aug'


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
checkdf = df[(df['Emp ID'] == 55037974)&(df['Date'] == '2024-08-05')]


print('line 55 run successfully')


emp_t = '/Users/nitin14.patil/Downloads/RIL/ril/conveyance_analysis/JMD Geography RAG - 06.09.2024.xlsx'


excel_file_emp = pd.ExcelFile(emp_t)

sheet_names_emp = excel_file_emp.sheet_names

df_emp = pd.read_excel(excel_file_emp)

# Ensure there are no extra spaces
df_emp['Position Text'] = df_emp['Position Text'].str.strip()

# Filter using OR condition
df_emp_filtered = df_emp[(df_emp['Position Text'] == 'JMD Officer') | (df_emp['Position Text'] == 'JMD Lead')]

# print(df_emp_filtered)

# df=df[df['Emp ID']==50123511]

# df=df_ca

df.columns

# Convert Timestamp to datetime, handling mixed formats
df['Timestamp'] = pd.to_datetime(df['Timestamp'], format="%Y-%m-%d %H:%M:%S.%f", errors='coerce')

# Step 1: Convert the 'Date' column to datetime format
df['Date'] = pd.to_datetime(df['Date'], errors='coerce')

print('line 100 run successfully')


# # Step 2: Extract only the date part
# df['Date'] = df['Date'].dt.date


# Filter the DataFrame for the specific date

checkdf = df[(df['Emp ID'] == 55037974)&(df['Date'] == '2024-08-05')]

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
checkdf = df_with_distances[(df_with_distances['Emp ID'] == 55037974)&(df_with_distances['Date'] == '2024-08-05')]

df_ca = df_with_distances.merge(df_emp_filtered, how='left', left_on='Emp ID', right_on='Emp Code')

df_ca.drop(columns=['Emp Code'],inplace=True)

df_ca.rename(columns={'Position Text':'JMDO/JMDL'},inplace=True)

df_ca['JMDO/JMDL'] = np.where(
    df_ca['JMDO/JMDL'] == 'JMD Officer', 'JMDO', 
    np.where(df_ca['JMDO/JMDL'] == 'JMD Lead', 'JMDL', '')
)



print('JMDO/JMDL column added')



# df_ca=df_ca[df_ca['JMDO/JMDL'].isin(['JMDO','JMDL'])]

df_ca['Emp ID']=df_ca['Emp ID'].astype(int)




#start from here

d_df=df_ca



# # print(df_with_distances)
# d_df['70_Distance(KM)_zero_n'] = d_df.groupby(['Emp ID', 'Date'])['Distance(KM)'].transform(
#     lambda x: x.apply(lambda y: 0 if y > 70 else y))


# d_df['new_distance_n'] = d_df['Distance(KM)'].where(d_df['Distance(KM)'] <= 70, 0)




# Filter the DataFrame for the specific date
checkdf = d_df[(d_df['Emp ID'] == 55037974)&(d_df['Date'] == '2024-08-05')]




d_df.dtypes




d_df['Speed>70 KM/HR']=d_df['Speed of Travel (KM/Hr)'].apply(lambda x: 'Yes' if x > 70 else 'No')

print('code completed till "Speed>70 KM/HR" column')

d_df['Attendance chk'] = d_df.apply(
    lambda row: "Yes" if (
        (row['JMDO/JMDL'] == "JMDO" and row['PRM Id'] == "Attendance" and row['Distance(KM)'] > 20) or
        (row['JMDO/JMDL'] == "JMDL" and row['PRM Id'] == "Attendance" and row['Distance(KM)'] > 40)
    ) else "No", axis=1
)



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
    ((d_df['JMDO/JMDL'] == "JMDO") & (d_df['Daily KMs'] > 65)) | ((d_df['JMDO/JMDL'] == "JMDL") & (d_df['Daily KMs'] > 100)),
    "Yes", "No"
)

# Filter the DataFrame for the specific date
checkdf = d_df[(d_df['Emp ID'] == 50095250)&(d_df['Date'] == '2024-07-01')]


d_df['Line >10 KM']=np.where(
    (d_df['PRM Id'] != "Attendance") & (d_df['Distance(KM)']>10), 'Yes', 'No'
    
    )


d_df['Line >10 KM_for_10_Ocurence']=np.where(
    (d_df['PRM Id'] == "Attendance") & (d_df['Distance(KM)']>10), 'Yes', 'No'
    
    )

# Filter the DataFrame for the specific date
checkdf = d_df[(d_df['Emp ID'] == 50095250)&(d_df['Date'] == '2024-07-01')]


#Pivot for flag C

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

checkdf= d_df[(d_df['Emp ID']==55037974) & (d_df['Date']=='2024-08-05')]


# Find the minimum timestamp for each employee on each day
max_timestamps = attendance_df.groupby(['Emp ID', 'Date'])['Timestamp'].max().reset_index()


# Merge to find matching rows
merged = max_timestamps.merge(min_timestamps, on=['Emp ID', 'Date'], how='inner')

df1_filtered = merged[merged['Timestamp_x'] != merged['Timestamp_y']]

# Reset index if needed
df1_filtered.reset_index(drop=True, inplace=True)

print(df1_filtered)


d_df.loc[d_df['Timestamp'].isin(df1_filtered['Timestamp_x']) & (d_df['PRM Id'] == 'Attendance'), 'Is last checkin to Mark-out'] = 'Mark Out'


checkdf= d_df[(d_df['Emp ID']==55037974) & (d_df['Date']=='2024-08-05')]

b_df = d_df[(d_df['Line >10 KM_for_10_Ocurence']=='Yes')&(d_df['Is last checkin to Mark-out']=='Mark In')]

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


d_df['Count of 8 Occurence/M >10KM- Flag B'] =np.where(
    (d_df['Is last checkin to Mark-out']=='Mark In') &  
    (d_df['count >10KM check in']>8)&
    (d_df['Distance(KM)']>10),
    'Yes',
    'No'
    )



d_df.dtypes

# FLAG C
# High Travel Frequency: For instances of more than 10 km 
# travel between retailers more than 3 times in a day, 
# review for potential fake visits. Flag C- Analyse
# Pivot_c
# Step 1: Filter rows where KM > 10
df_filtered = d_df[(d_df['Distance(KM)'] > 10)&
                   (d_df['Is last checkin to Mark-out']!="Mark In")&
                   (d_df['Is last checkin to Mark-out']!="Mark Out")]

# Step 2: Group by 'Emp ID' and 'Date', and count the number of occurrences
km_count = df_filtered.groupby(['Emp ID', 'Date']).size().reset_index(name='Flag C check')

# View the result
print(km_count)

# Merging DataFrames
merged_df = d_df.merge(km_count[['Emp ID', 'Date', 'Flag C check']], on=['Emp ID', 'Date'], how='left')

merged_df['Distance(KM)_shift'] = merged_df['Distance(KM)'].shift(1)

merged_df['Distance(KM)_shift1']=merged_df['Distance(KM)_shift'].shift(-1)

merged_df['Distance(KM)_shift2'] = merged_df['Distance(KM)'].shift(-1)

merged_df['Is last checkin to Mark-out1']=merged_df['Is last checkin to Mark-out'].shift(-1)



merged_df['Count >3 per day of >10KM - Flag-C between Retailers']=np.where(
   (merged_df['Flag C check']>3)&
   (merged_df['Distance(KM)']>10),'Yes','No')



d_df=merged_df

checkdf= d_df[(d_df['Emp ID']==55037974)]






c_file_path = '/Users/nitin14.patil/Downloads/RIL/ril/conveyance_analysis/Car Request Details.xlsx'
c_excel_file = pd.ExcelFile(c_file_path)
c_sheet_names = c_excel_file.sheet_names
c_df_ca = pd.read_excel(c_excel_file)

c_df_ca['TRDMFIELD1'] = c_df_ca['TRDMFIELD1'].str.replace('P', '', regex=False)

c_df_ca['TRDMFIELD1']=c_df_ca['TRDMFIELD1'].astype(int)

f_c_df = c_df_ca[['TRDMFIELD1', 'TRDMBOOKSTARTDATE', 'TRDMBOOKENDDATE']]



# Ensure date columns are in datetime format
f_c_df['TRDMBOOKSTARTDATE'] = pd.to_datetime(f_c_df['TRDMBOOKSTARTDATE'])
f_c_df['TRDMBOOKENDDATE'] = pd.to_datetime(f_c_df['TRDMBOOKENDDATE'])

# Create a new DataFrame with expanded date ranges
expanded_rows = []

for _, row in f_c_df.iterrows():
    # Generate date range for each row
    date_range = pd.date_range(start=row['TRDMBOOKSTARTDATE'], end=row['TRDMBOOKENDDATE'])
    for date in date_range:
        expanded_rows.append({'TRDMFIELD1': row['TRDMFIELD1'], 'Date': date})

# Create a new DataFrame from the expanded rows
expanded_df = pd.DataFrame(expanded_rows)


# Sort the DataFrame by the TRDMBOOKSTARTDATE
expanded_df.sort_values(by='Date', inplace=True)
# Rename columns to match the desired output
expanded_df.rename(columns={'Date': 'TRDMBOOKSTARTDATE'}, inplace=True)


# Apply the function to create the 'date_value' column
expanded_df['date_value'] = expanded_df['TRDMBOOKSTARTDATE'].apply(date_to_excel_serial)

# Corrected code for conditionally creating 'CC' column
expanded_df['CC'] = (expanded_df['TRDMFIELD1'].astype(str) + expanded_df['date_value'].astype(str)).astype(int)


# Step 1: Create a shifted column for Distance(KM) with the correct shift applied

# shifted_km_post = d_df['KM post Mark in/Out'].shift(-1)

# 'KM post Mark in/Out',

d_df.dtypes



checkdf = d_df[(d_df['Emp ID'] == 55037974)]






def calculate_new_distance(row):
    # Apply the JMDL conditions
    if row['JMDO/JMDL'] == "JMDL" and row['Is last checkin to Mark-out'] == "Mark In" and row['Distance(KM)'] > 40:
        return 40
    
    if row['JMDO/JMDL'] == "JMDL" and row['Is last checkin to Mark-out1'] == "Mark Out" and row['Distance(KM)'] > 40:
        return 40
    
    # Apply the JMDO conditions
    if row['JMDO/JMDL'] == "JMDO" and row['Is last checkin to Mark-out'] == "Mark In" and row['Distance(KM)'] > 20:
        return 20
    if row['JMDO/JMDL'] == "JMDO" and row['Is last checkin to Mark-out1'] == "Mark Out" and row['Distance(KM)'] > 20:
        return 20

    # Default case if no conditions are met
    return row['Distance(KM)']

# Apply the function to the dataframe
d_df['new_distance_n'] = d_df.apply(calculate_new_distance, axis=1)


checkdf = d_df[(d_df['Emp ID'] == 50163693) &(d_df['Date'] == '2024-08-20')]
              

d_df['new_distance_n1'] = d_df['new_distance_n'].shift(1)


def apply_km_post_mark_in_out(d_df):
    # Apply the first condition (for 'Mark In')
    d_df['KM post Mark in/Out'] = np.where(
        (d_df['JMDO/JMDL'].isin(["JMDO", "JMDL"])) & 
        (d_df['Is last checkin to Mark-out'] == "Mark In"),
        d_df['new_distance_n'],
        d_df['Distance(KM)']  # If condition is not met, keep the original value
    )
    
    # Apply the second condition (for 'Mark Out') without overwriting the first
    d_df['KM post Mark in/Out'] = np.where(
        (d_df['JMDO/JMDL'].isin(["JMDO", "JMDL"])) & 
        (d_df['Is last checkin to Mark-out1'] == "Mark Out"),
        d_df['new_distance_n'],
        d_df['KM post Mark in/Out']  # Keep previous 'KM post Mark in/Out' values if condition not met
    )

    return d_df

d_df = apply_km_post_mark_in_out(d_df)

# Example of filtering for a specific employee ID
checkdf = d_df[(d_df['Emp ID'] == 50163693) &(d_df['Date'] == '2024-08-20')]








# Step 4: Check if the values have been updated
df_ch=d_df[['JMDO/JMDL','Emp ID', 'Date', 'Is last checkin to Mark-out', 'Distance(KM)',  'Distance(KM)_shift','Distance(KM)_shift1','Is last checkin to Mark-out1','KM post Mark in/Out']]


# Example of filtering for a specific employee ID
checkdf = df_ch[(df_ch['Emp ID'] == 50163693) &(df_ch['Date'] == '2024-08-20')]





# Create a shifted version of the 'Distance(KM)' column
# d_df['Distance(KM)_shift'] = d_df['Distance(KM)'].shift(1)

# .loc[i+1,'Lat']


# Assign 'Yes' or 'No' based on whether the value in 'CC' is in expanded_df['CC']
d_df['Car Hire chk'] = np.where(d_df['CC'].isin(expanded_df['CC']), 'Yes', 'No')


# Example of filtering for a specific employee ID
checkdf = d_df[d_df['Emp ID'] == 50123511]

checkdf = d_df[(d_df['Emp ID'] == 67646412) & (d_df['Distance(KM)'] >70)]




d_df['Car Hire KM']=np.where(
    d_df['Car Hire chk'] == 'Yes',0,d_df['Distance(KM)']
    )


d_df['No Issues'] = np.where(
    (d_df['Speed>70 KM/HR'] == 'No') & (d_df['Attendance chk'] == 'No') & (d_df['Is last checkin to Mark-out1']!='Mark Out'),
    d_df['Distance(KM)'],
    0     # Default value if the condition is not met
)


d_df['KM post Speed'] = np.where(
    d_df['Speed>70 KM/HR']=='Yes',0,d_df['Distance(KM)']
    )

# Convert columns to numeric, forcing errors to NaN
d_df[['KM post Speed', 'KM post Mark in/Out', 'Car Hire KM', 'No Issues']] = d_df[['KM post Speed', 
    'KM post Mark in/Out', 'Car Hire KM', 'No Issues']].apply(pd.to_numeric, errors='coerce')



def calculate_final_km(row):
    # Step 1: Check if 'Car Hire chk' is 'Yes' or 'Speed>70 KM/HR' is 'Yes'
    if row['Car Hire chk'] == 'Yes' or row['Speed>70 KM/HR'] == 'Yes':
        return 0  # Set 'Final KM' to 0 if either condition is True
    # Step 2: Check if 'No Issues' is 0
    elif row['No Issues'] == 0:
        return row['KM post Mark in/Out']  # Set 'Final KM' to 'KM post Mark in/Out' if 'No Issues' is 0
    else:
        return row['No Issues']  # Retain 'No Issues' value if none of the above conditions are met

# Apply the function to each row in the DataFrame
d_df['Final KM'] = d_df.apply(calculate_final_km, axis=1)

print(d_df[['Car Hire KM', 'Speed>70 KM/HR', 'Attendance chk', 'KM post Mark in/Out', 'No Issues']].head())


    
d_df.dtypes
# Example of filtering for a specific employee ID

checkdf = d_df_f[(d_df_f['Emp ID'] == 50163693) & (d_df_f['Date'] == '2024-08-20')]



    
d_df['Diff KM'] = d_df['Final KM']-d_df['Distance(KM)']

d_df['LAT & LONG'] = (d_df['Lat'].astype(str) +","+d_df['Long'].astype(str))

d_df['Observations']=''




# 50093194	23/07/24


d_df_f=d_df[[
              'Emp ID',
'Date',
'PRM Id',
'Lat',
'Long',
'Timestamp',
'Distance(KM)',
'Speed of Travel (KM/Hr)',
'JMDO/JMDL',
'Speed>70 KM/HR',
'Attendance chk',
'CC',
'Daily KMs',
'Daily KMs chk- Flag A',
'Line >10 KM',
'Count of 8 Occurence/M >10KM- Flag B',
'Count >3 per day of >10KM - Flag-C between Retailers',
'Car Hire chk',
'KM post Speed',
'KM post Mark in/Out',
'Car Hire KM',  
'No Issues',
'Final KM',
'Diff KM',
'LAT & LONG',
'Observations'
              ]]



max_rows_per_sheet = 1050000

# Calculate the number of sheets needed
num_sheets = (len(d_df) // max_rows_per_sheet) + 1

# Create an Excel writer objecta
with pd.ExcelWriter('Adhoc with new changes with_all_columns_Aug24f.xlsx', engine='xlsxwriter') as writer:
    for i in range(num_sheets):
        start_row = i * max_rows_per_sheet
        end_row = (i + 1) * max_rows_per_sheet
        
        # Slice the DataFrame for the current sheet
        chunk = d_df.iloc[start_row:end_row]
        
        # Write the chunk to a new sheet
        chunk.to_excel(writer, sheet_name=f'Sheet{i + 1}', index=False)

print("Export completed successfully_a!")



# Calculate the number of sheets needed
num_sheets = (len(d_df_f) // max_rows_per_sheet) + 1

# Create an Excel writer object
with pd.ExcelWriter('Adhoc with new changes with__limited_columns_Aug24f.xlsx', engine='xlsxwriter') as writer:
    for i in range(num_sheets):
        start_row = i * max_rows_per_sheet
        end_row = (i + 1) * max_rows_per_sheet
        
        # Slice the DataFrame for the current sheet
        chunk = d_df_f.iloc[start_row:end_row]
        
        # Write the chunk to a new sheet
        chunk.to_excel(writer, sheet_name=f'Sheet{i + 1}', index=False)

print("Export completed successfully_l!")

checkdf = d_df_f[(d_df_f['Emp ID'] == 50163693) & (d_df_f['Date'] == '2024-08-20')]


# d_df_f.to_csv('Adhoc with new changes with_limited_columns_May24.csv')

# d_df.to_csv('Adhoc with new changes with_all_columns_May24.csv')

