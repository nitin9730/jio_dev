import pandas as pd
import numpy as np
import math
from itertools import combinations
from datetime import datetime
import os



file_path = '/Users/nitin14.patil/Downloads/RIL/ril/conveyance_analysis/Data_input_f.xlsx'


excel_file = pd.ExcelFile(file_path)

sheet_names = excel_file.sheet_names

df_ca = pd.read_excel(excel_file)

# df_ca['JMDO/JMDL']=df_ca['\tJMDO/JMDL']


# df=df_ca[df_ca['Emp ID']==50123511]

df=df_ca


df.columns


def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0  # Radius of Earth in kilometers
    lat1, lon1 = math.radians(lat1), math.radians(lon1)
    lat2, lon2 = math.radians(lat2), math.radians(lon2)
    dlon, dlat = lon2 - lon1, lat2 - lat1
    a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


# Convert Timestamp to datetime, handling mixed formats
df['Timestamp'] = pd.to_datetime(df['Timestamp'], dayfirst=True, infer_datetime_format=True)

# Initialize a list to store results
results = []

# Group by Employee ID and Date to calculate distances within each group
for (emp_id, date), group in df.groupby(['Emp ID', 'Date']):
    # Reset the index for the group
    group = group.reset_index(drop=True)
    
    
    for i in range(len(group) - 1):
        lat1, lon1 = group.loc[i, 'Lat'], group.loc[i, 'Long']
        lat2, lon2 = group.loc[i+1, 'Lat'], group.loc[i+1, 'Long']
        distance = haversine(lat1, lon1, lat2, lon2)
        
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

# Convert the results list to a DataFrame
df_with_distances = pd.DataFrame(results)


# Reset the index in both DataFrames to align them by their row order
df_with_distances.reset_index(drop=True, inplace=True)
df.reset_index(drop=True, inplace=True)

# Then perform the concatenation
d_df = pd.concat([df_with_distances, df[['JMDO/JMDL']]], axis=1)


# print(df_with_distances)

d_df['70_Distance(KM)_zero'] = d_df.groupby(['Emp ID', 'Date'])['Distance(KM)'].transform(
    lambda x: 0 if (x > 70).any() else x)


d_df['Distance is more than 10 Km']=d_df['Distance(KM)'].apply(lambda x: 'Yes' if x > 10 else 'No')

d_df['Distance is more than 25 Km']=d_df['Distance(KM)'].apply(lambda x: 'Yes' if x > 25 else 'No')

d_df['Speed>70 KM/HR']=d_df['Speed of Travel (KM/Hr)'].apply(lambda x: 'Yes' if x > 70 else 'No')

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

# Define the Excel start date
excel_start_date = datetime(1900, 1, 1)

# Define a function to convert a date to Excel serial number
def date_to_excel_serial(date_obj):
    # Check if the date is a Timestamp, convert to datetime if necessary
    if isinstance(date_obj, pd.Timestamp):
        date_obj = date_obj.to_pydatetime()
    elif isinstance(date_obj, str):
        date_obj = datetime.strptime(date_obj, "%d/%m/%Y")
        
    serial_number = (date_obj - excel_start_date).days + 2
    return serial_number

# Apply the function to create the 'date_value' column
d_df['date_value'] = d_df['Date'].apply(date_to_excel_serial)

# Corrected code for conditionally creating 'CC' column
d_df['CC'] = d_df.apply(
    lambda row: str(row['Emp ID']) + str(row['date_value']) if row['beat > 10 KM'] == 'Yes' else '',
    axis=1
)

# Count occurrences of each CC value per Emp ID and Date
count_cc = d_df.groupby(['CC']).size().reset_index(name='Count')

d_df = pd.merge(d_df,count_cc,how='left',left_on='CC',right_on='CC')

d_df.rename(columns={'Count':'Occurrence'},inplace=True)

# Set values in column 'B' to NaN where 'A' > 3
d_df.loc[d_df['CC'] == '', 'Occurrence'] = ''

d_df['Occurrence Chk'] = d_df['beat > 10 KM'].apply(lambda x: 'Yes' if x == 'Yes' else 'No')


df = d_df


def add_sequential_occurrence_column_with_where(df):
    # Convert 'Date' to datetime format if it's not already
    df['Date'] = pd.to_datetime(df['Date'], format='%d/%m/%y')

    # Create a mask for the rows that meet the conditions
    mask = (df['Occurrence'] != '') & (df['Occurrence Chk'] == 'Yes')


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

checkdf= df[(df['Emp ID']==50093655) & (df['Date']=='2024-07-10')]



# Find the minimum timestamp for each employee on each day
max_timestamps = attendance_df.groupby(['Emp ID', 'Date'])['Timestamp'].max().reset_index()

# Merge with the original DataFrame to get the full attendance record for the minimum timestamp
attendance_of_max_time = pd.merge(max_timestamps, attendance_df, on=['Emp ID', 'Date', 'Timestamp'], how='left')

print(attendance_of_max_time)


df.loc[df['Timestamp'].isin(attendance_of_max_time['Timestamp']) & (df['PRM Id'] == 'Attendance'), 'Is last checkin to Mark-out'] = 'Mark Out'


checkdf= df[(df['Emp ID']==50093655) & (df['Date']=='2024-07-10')]



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
    if row['CC'] == "JMDL" and row['Is Mark-In & Markout'] == "Mark In" and row['Distance(KM)'] > 20:
        return "yes"
    elif row['CC'] == "JMDL" and row['Is Mark-In & Markout'] == "Mark Out" and row['Distance(KM)'] > 30:
        return "yes"
    elif row['CC'] == "JMDO" and row['Is Mark-In & Markout'] == "Mark In" and row['Distance(KM)'] > 10:
        return "yes"
    elif row['CC'] == "JMDO" and row['Is Mark-In & Markout'] == "Mark Out" and row['Distance(KM)'] > 15:
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

def custom_check_v2(row):
    if row['CC'] == "JMDO":
        return "yes" if row['Distance(KM)'] > 30 else "no"
    elif row['CC'] == "JMDL":
        return "yes" if row['Distance(KM)'] > 60 else "no"
    else:
        return ""

# Apply the function to the DataFrame
df['Max KM'] = df.apply(custom_check_v2, axis=1)

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

# Iterate through the DataFrame to calculate distances
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

checkdf= df[(df['Emp ID']==50093655) & (df['Date']=='2024-07-10')]


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

df['Flag A'] = df.apply(
    lambda x: 'Yes' if (
        x['Speed of Travel (KM/Hr)'] > 70 and 
        x['Is last checkin to Mark-out'] not in ['Mark In', 'Mark Out']
    ) else 'No', 
    axis=1
)

df.columns

# Step 1: Filter DataFrame where 'Is last checkin to Mark-out' is "Mark In"
filtered_df = df[df['Is last checkin to Mark-out'] == 'Mark In'].copy()

# Step 2: Extract month from 'Date' column
filtered_df['Month'] = pd.to_datetime(filtered_df['Date']).dt.to_period('M')

occurrences_df = filtered_df.groupby(['Emp ID', 'Month', 'Is last checkin to Mark-out']).apply(
    lambda x: (x['Distance(KM)'] > 10).sum()
).reset_index(name='Occurrences_1')

# Step 5: Add Month to original DataFrame for merging
df['Month'] = pd.to_datetime(df['Date']).dt.to_period('M')

# Step 6: Merge the occurrences back to the original DataFrame
df = pd.merge(df, occurrences_df[['Emp ID', 'Month', 'Occurrences_1']], on=['Emp ID', 'Month'], how='left')


# Step 7: Create Flag B - Analyse based on the merged data
df['Flag B'] = df.apply(
    lambda x: 'Analyse' if x['Occurrences_1'] > 8 and x['Is last checkin to Mark-out'] == 'Mark In' and x['Distance(KM)']>10 else '', 
    axis=1
)
checkdf= df[(df['Emp ID']==50093194) & (df['Month']=='2024-07')]

# Ensure the 'Date' column is in datetime format
df['Date'] = pd.to_datetime(df['Date'])

# Step 3: Group by 'Emp ID' and 'Date'
# Step 4: Count occurrences where 'Distance(KM)' is greater than 10 km
occurrences_df_c = df.groupby(['Emp ID', 'Date']).apply(
    lambda x: (x['Distance(KM)'] > 10).sum()
).reset_index(name='Occurrences_c')

# Display the result
print(occurrences_df_c)

# Step 6: Merge the occurrences back to the original DataFrame
df = pd.merge(df, occurrences_df_c[['Emp ID', 'Date', 'Occurrences_c']], on=['Emp ID', 'Date'], how='left')

# Step 7: Create Flag B - Analyse based on the merged data
df['Flag C'] = df.apply(
    lambda x: 'Analyse' if x['Occurrences_c'] > 3 and x['Distance(KM)']>10 else '', 
    axis=1
)

df[['Distance(KM)', 'new_Distance(KM)']] = df.apply(
    lambda x: pd.Series([0, 0]) if x['Is last checkin to Mark-out'] == 'Mark Out' else pd.Series([x['Distance(KM)'], x['new_Distance(KM)']]),
    axis=1
)

df[['Median Lat of PRM', 'Median Long of PRM']] = df.apply(
    lambda x: pd.Series([0, 0]) if x['PRM Id'] == 'Attendance' else pd.Series([x['Median Lat of PRM'], x['Median Long of PRM']]),
    axis=1
)



c_file_path = '/Users/nitin14.patil/Downloads/RIL/ril/conveyance_analysis/Car Request Details.xlsx'


c_excel_file = pd.ExcelFile(c_file_path)


c_sheet_names = c_excel_file.sheet_names

c_df_ca = pd.read_excel(c_excel_file)


c_df_ca['TRDMFIELD1'] = c_df_ca['TRDMFIELD1'].apply(lambda x: x[1:] if isinstance(x, str) else x)


print(c_df_ca.dtypes)




# Corrected function definition
def check_record_match(row, df2):
    matches = df2[
        (df2['TRDMFIELD1'].astype('int64') == row['Emp ID'])
                    &
                    (df2['TRDMBOOKSTARTDATE'] <= row['Date']) &
                    (df2['TRDMBOOKENDDATE'] >= row['Date'])    
                  ]
                   
    return len(matches) > 0

# Apply the function to each row in df1 with the additional argument passed via 'args'
df['Travel_Record_Match'] = df.apply(check_record_match, axis=1, args=(c_df_ca,))

# Print the resulting DataFrame
print(df)

checkdf= df[(df['Emp ID']==50123511) & (df['Travel_Record_Match']==True)]



checkdf.dtypes
a=c_df_ca.dtypes


df.to_csv('/Users/nitin14.patil/Downloads/RIL/ril/conveyance_analysis/adhoc_04Sep24_all_columns.csv')






df_final=df[['Emp ID',
'Date',
'PRM Id',
'Lat',
'Long',
'Timestamp',
'Distance(KM)',
'Speed of Travel (KM/Hr)',
# 'Distance is more than 10 Km',
# 'Distance is more than 25 Km',
# 'Speed>70 KM/HR',
# 'Speed Bucket',
# 'KM Bucket',
# 'Beat > 10 KM',
# 'CC',
# 'Occurrence',
# 'Occurrence Chk',
# 'Occurance of more than 10 Km Distance on the same day',
# 'Is Mark-In & Markout',
'Is last checkin to Mark-out',
# 'Mark in & Out>10KM',
# 'Max KM',
# 'Median Lat of PRM',
# 'Median Long of PRM',
# 'Distance between mean to actual',
# 'Distance is more than 1 Km',
# 'KM post Speed',
# 'KM post Mark in & Out',
# 'Occurrence KM',
# 'No issue',
'Final KM',
# 'Diff KM',
# 'JMDO/JMDL',
# 'Travel type',
# 'LAT & LONG',
# 'Observation', 
'Flag A', 'Flag B', 'Flag C']]

# df.columns


# # Corrected function definition
# def check_record_match(row, df2):
#     matches = df2[(df2['TRDMID'] == row['Emp ID']) &
#                   (df2['TRDMBOOKSTARTDATE'] <= row['Date']) &
#                   (df2['TRDMBOOKENDDATE'] >= row['Date'])]
#     return len(matches) > 0

# # Apply the function to each row in df1 with the additional argument passed via 'args'
# df_final['Travel_Record_Match'] = df_final.apply(check_record_match, axis=1, args=(c_df_ca,))

# # Print the resulting DataFrame
# print(df)


checkdf1= df_final[(df_final['Emp ID']==50123511) & (df_final['Date']=='2024-07-05')]



# df.columns


50123511





