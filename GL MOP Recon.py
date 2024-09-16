import pandas as pd
import numpy as np
# Path to your Excel file
file_path = '/Users/nitin14.patil/Library/CloudStorage/OneDrive-RelianceCorporateITParkLimited/Documents/python_work/GL MOP Reco./Requirements for Recon Automation.xlsx'

# Load the Excel file
excel_file = pd.ExcelFile(file_path)

# Get the sheet names
sheet_names = excel_file.sheet_names


# Load specific sheets into DataFrames
df_st = pd.read_excel(excel_file, sheet_name='ST')
df_it = pd.read_excel(excel_file, sheet_name='IT')

# Select 'orderID' columns from both DataFrames
st_order_ids = df_st['OrderI D (20 digit)']
it_order_ids = df_it['OrderI D (20 digit)']


# Concatenate the columns vertically
merged_order_ids = pd.concat([st_order_ids, it_order_ids])

# Remove duplicate values
unique_order_ids = merged_order_ids.drop_duplicates().reset_index(drop=True)

unique_order_ids=pd.DataFrame(unique_order_ids)

unique_order_ids.rename(columns={'OrderI D (20 digit)': 'OrderI D'}, inplace=True)

# Create a pivot table
pivot_table_it = df_it.pivot_table(index='OrderI D', columns='Type.1', values='Tender Value', aggfunc='sum').reset_index()

# Replace NaN values with 0
pivot_table_it.fillna(0, inplace=True)

# Merge the unique_order_ids with the pivot_table on 'Order ID'
merged_data_it = pd.merge(unique_order_ids, pivot_table_it, how='left', on='OrderI D')

# Replace NaN values with 0
merged_data_it.fillna(0, inplace=True)


# Calculate the 'Net Invoice' column based on the specified logic
merged_data_it['Net Invoice'] = np.where(
    merged_data_it['Fwd'].notna(),
    np.where(merged_data_it['Rev'].notna(), merged_data_it['Fwd'] + merged_data_it['Rev'], merged_data_it['Fwd']),
    np.where(merged_data_it['Rev'].notna(), merged_data_it['Rev'], np.nan)
)
# Create a pivot table
pivot_table_st = df_st.pivot_table(index='OrderI D', columns='Type.1', values='Gross amount', aggfunc='sum')

# Reset the index to make 'Order ID' a column again (optional, depending on your needs)
pivot_table_st.reset_index(inplace=True)

# Replace NaN values with 0
pivot_table_st.fillna(0, inplace=True)


# Merge the unique_order_ids with the pivot_table on 'Order ID'
merged_data_st = pd.merge(unique_order_ids, pivot_table_st, how='left', on='OrderI D')


# Replace NaN values with 0
merged_data_st.fillna(0, inplace=True)


# Calculate the 'Net Invoice' column based on the specified logic
merged_data_st['Net Invoice'] = np.where(
    merged_data_st['Fwd'].notna(),
    np.where(merged_data_st['Rev'].notna(), merged_data_st['Fwd'] + merged_data_st['Rev'], merged_data_st['Fwd']),
    np.where(merged_data_st['Rev'].notna(), merged_data_st['Rev'], np.nan)
)

# Merge the DataFrames on the common column
all_orders = pd.merge(merged_data_it, merged_data_st, on="OrderI D", how='inner')


# Calculate the 'Net Invoice' column based on the specified logic
all_orders['Difference'] = np.where(
    all_orders['Net Invoice_x'].notna(),
    np.where(all_orders['Net Invoice_y'], all_orders['Net Invoice_x'] + all_orders['Net Invoice_y'], merged_data_it['Fwd']),
    np.where(all_orders['Net Invoice_y'], all_orders['Net Invoice_y'], np.nan)
)


all_orders['Difference']= all_orders['Net Invoice_x']-all_orders['Net Invoice_y']

all_orders.rename(columns={'Fwd_x':'POS +ve',	'Rev_x':'POS -ve',	'Net Invoice_x':'Net POS',	'Fwd_y':'MPR +ve',	'Rev_y':'MPR -ve',	'Net Invoice_y':'Net MPR',	'Difference':'Difference'}, inplace = True)
                           # 'Remark':'Remarks',	'IT Totals':'IT Totals',	'ST Totals':'ST Totals',	'Net Totals':'Net Totals',	'IT Count Totals':'IT Count Totals',	'ST Count Totals':'ST Count Totals',	'Max Inv Date':'Max Inv Date',	'Max Sett Date':'Max Sett Date',}, inplace=True)
# # Get the counts of unique values
# unique_counts = all_orders['Remark'].value_counts()

# # Convert the Series to a DataFrame to include counts and unique values
# unique_counts_df = unique_counts.reset_index()
# unique_counts_df.columns = ['Unique Value', 'Count']

# # Print the DataFrame
# print(unique_counts_df)

# Add the 'Remark' column based on the condition
all_orders['Remarks'] = np.where(all_orders['Difference'] == 0, 'Matched', '')# Get the counts of unique values
unique_counts = all_orders['Remarks'].value_counts()

# Convert the Series to a DataFrame to include counts and unique values
unique_counts_df = unique_counts.reset_index()
unique_counts_df.columns = ['Unique Value', 'Count']

# Print the DataFrame
print(unique_counts_df)
# Load specific sheets into DataFrames
df_it_star = pd.read_excel(excel_file, sheet_name='IT star')

# Create a pivot table
pivot_table_it_star = df_it_star.pivot_table(index='OrderI D', values='Tender Value', aggfunc='sum')


# Merge the DataFrames on the common column
all_orders_it = pd.merge(all_orders, pivot_table_it_star, on='OrderI D', how='left')

all_orders_it.rename(columns={'Tender Value': 'IT Totals'}, inplace=True)
# Replace NaN values with 0
all_orders_it.fillna(0, inplace=True)


# Load specific sheets into DataFrames
df_st_star = pd.read_excel(excel_file, sheet_name='ST star')

# Create a pivot table
pivot_table_st_star = df_st_star.pivot_table(index='OrderI D', values='Gross amount', aggfunc='sum')


# Merge the DataFrames on the common column
all_orders_st = pd.merge(all_orders_it, pivot_table_st_star, on='OrderI D', how='left')

all_orders_st.rename(columns={'Gross amount': 'ST Totals'}, inplace=True)

# Replace NaN values with 0
all_orders_st.fillna(0, inplace=True)


all_orders_st['Net Totals'] = all_orders_st['IT Totals']-all_orders_st['ST Totals']

# Count occurrences in df2
counts_df2 = df_it_star['OrderI D'].value_counts()

# Map counts to values in df1
all_orders_st['IT Count Totals'] = all_orders_st['OrderI D'].map(counts_df2).fillna(0).astype(int)

# Count occurrences in df2
counts_df2 = df_st_star['OrderI D'].value_counts()

# Map counts to values in df1
all_orders_st['ST Count Totals'] = all_orders_st['OrderI D'].map(counts_df2).fillna(0).astype(int)

a=all_orders_st


# a.columns
# List of columns to apply the logic to
columns_to_modify = ['Net POS', 'Net MPR', 'Difference', 'IT Totals', 'ST Totals', 'Net Totals']


# a1=a

# # Example condition: update 'Remark' where 'POS' > 1 and 'MPR' < 30
# a1['Remark'] == 'Inv in Current month, Sett subsequently'

# count = a1['Remark'].value_counts().get('Inv in Current month, Sett subsequently', 0)
# print(f"Count of 'Inv in Current month, Sett subsequently': {count}")

#all okay till
# Apply the lambda function to replace values
a[columns_to_modify] = a[columns_to_modify].applymap(lambda x: 0 if -10 <= x <= 10 else x)


a.loc[(a['Difference'] == 0) & (a['Remarks'] == ''), 'Remarks'] = 'Matched'


# Create a boolean mask for the condition
mask2 = (a['Remarks'] == '') & \
       (a['POS +ve'] > 0) & \
       (a['POS -ve'] == 0) & \
       (a['MPR +ve'] == 0) & \
       (a['MPR -ve'] == 0) & \
       (a['Net Totals'] < 11)&\
           (a['Net Totals'] > -11)

# Use np.where() to assign values based on the mask
a['Remarks'] = np.where(mask2, 'Inv in Current month, Sett subsequently', a['Remarks'])
# Ensure 'Posting Date' is in datetime format
df_it_star['Posting Date'] = pd.to_datetime(df_it_star['Posting Date'])

# Calculate the maximum posting date for each 'Order ID'
max_posting_dates = df_it_star.groupby('OrderI D')['Posting Date'].max().reset_index()

# Merge the maximum posting dates back to DataFrame 'a'
a = a.merge(max_posting_dates, on='OrderI D', how='left')

# Initialize 'Max Inv Date' as object type to handle mixed types
a['Max Inv Date'] = a['Posting Date'].astype(object)

# Apply the conditional logic to create the 'Max Inv Date' column
a.loc[a['Remarks'] == 'Matched', 'Max Inv Date'] = 'NA'

# Drop the temporary 'Posting Date' column
a.drop(columns=['Posting Date'], inplace=True)

# Ensure 'Posting Date' is in datetime format
df_st_star['Date on which record was created'] = pd.to_datetime(df_st_star['Date on which record was created'])

# Calculate the maximum posting date for each 'Order ID'
max_post_dates = df_st_star.groupby('OrderI D')['Date on which record was created'].max().reset_index()# Merge the maximum posting dates back to DataFrame 'a'
a = a.merge(max_post_dates, on='OrderI D', how='left')

# Initialize 'Max Inv Date' as object type to handle mixed types
a['Max Sett Date'] = a['Date on which record was created'].astype(object)

# Apply the conditional logic to create the 'Max Inv Date' column
a.loc[a['Remarks'] == 'Matched', 'Max Sett Date'] = 'NA'

# Drop the temporary 'Posting Date' column
a.drop(columns=['Date on which record was created'], inplace=True)


# # Get the counts of unique values
# unique_counts = a['Remark'].value_counts()

# # Convert the Series to a DataFrame to include counts and unique values
# unique_counts_df = unique_counts.reset_index()
# unique_counts_df.columns = ['Unique Value', 'Count']

# # Print the DataFrame
# print(unique_counts_df)
# a.rename(columns={'POS +ve':'POS +ve',	'POS -ve':'POS -ve',	'Net POS':'Net POS',	'MPR +ve':'MPR +ve',	'MPR -ve':'MPR -ve',	'Net MPR':'Net MPR',	'Difference':'Difference',	'Remark':'Remarks',	'IT Totals':'IT Totals',	'ST Totals':'ST Totals',	'Net Totals':'Net Totals',	'IT Count Totals':'IT Count Totals',	'ST Count Totals':'ST Count Totals',	'Max Inv Date':'Max Inv Date',	'Max Sett Date':'Max Sett Date',}, inplace=True)


# Convert the 'Date' column to datetime
a['Max Inv Date'] = pd.to_datetime(a['Max Inv Date'], errors='coerce')# Convert the 'Date' column to datetime
a['Max Sett Date'] = pd.to_datetime(a['Max Sett Date'], errors='coerce')
End_date = pd.to_datetime('2024-05-31 00:00:00')

# Create a boolean mask for the condition
mask3 = (a['Remarks'] == '') & \
    (a['POS +ve'] == 0) & \
    (a['POS -ve'] < 0) & \
    (a['MPR +ve'] == 0) & \
    (a['MPR -ve'] == 0) & \
    (a['Net Totals'] == 0) & \
    (pd.to_datetime(a['Max Sett Date']) > End_date)

# Use np.where() to assign values based on the mask
a['Remarks'] = np.where(mask3, 'Reversal in Current month, Refunded subsequently', a['Remarks'])
# Create a boolean mask for the condition
mask4 = (a['Remarks'] == '') & \
    (a['POS +ve'] == 0) & \
    (a['POS -ve'] < 0) & \
    (a['MPR +ve'] == 0) & \
    (a['MPR -ve'] == 0) & \
    (a['Net Totals'] == 0) & \
    (pd.to_datetime(a['Max Sett Date']) <= End_date)

# Use np.where() to assign values based on the mask
a['Remarks'] = np.where(mask4, 'Reversal in Current month, Refunded Previously', a['Remarks'])#mask5

a1=a
            
# Now you can apply your filter
filtered_df = a1[(a1['Remarks'] == '') & (a['Net POS'] == 0) & (a['MPR -ve'] == 0) & (a['Net Totals'] == 0) & (a['Max Sett Date'] <= End_date)]      


filtered_df['Remarks'] = 'Sett in Current month, Inv issued previously' 
                
# Step 1: Get the indices from df2
indices_to_remove = filtered_df.index

# Step 2: Remove these indices from df1
df1_filtered = a1.drop(indices_to_remove, errors='ignore')


# Concatenate df1 and df2 along the rows (axis=0)
df_combined = pd.concat([df1_filtered, filtered_df])

# Sort the resulting DataFrame by index
df_sorted = df_combined.sort_index()# Get the counts of unique values


a=df_sorted

# # Example condition: update 'Remark' where 'POS' > 1 and 'MPR' < 30
# a1['Remarks'] == 'Sett in Current month, Inv issued previously'

# count = a1['Remarks'].value_counts().get('Sett in Current month, Inv issued previously', 0)
# print(f"Sett in Current month, Inv issued previously': {count}")


a1=a
# Create a boolean mask for the condition
mask6 = (a1['Remarks'] == '') & \
    (a1['POS +ve'] == 0) & \
    (a1['POS -ve'] == 0) & \
    (a1['MPR -ve'] == 0) & \
    (a1['Net Totals'] == 0) & \
    (pd.to_datetime(a1['Max Sett Date']) > End_date)

# Use np.where() to assign values based on the mask
a1['Remarks'] = np.where(mask6, 'Sett in Current month, Refunded Subsequently', a1['Remarks'])


a1=a


# Create a boolean mask for the condition
mask7 = (a1['Remarks'] == '') & \
    (a1['POS +ve'] > 0) & \
    (a1['POS -ve'] < 0) & \
    (a1['Net POS'] == 0) & \
    (a1['MPR -ve'] == 0) & \
    (a1['Net Totals'] == 0)

# Use np.where() to assign values based on the mask
a1['Remarks'] = np.where(mask7, 'Sett in Current month, Refunded Subsequently', a1['Remarks'])



a=a1

a1=a

# Create a boolean mask for the condition
mask8 = (a1['Remarks'] == '') & \
    (a1['POS -ve'] == 0) & \
    (a1['MPR -ve'] == 0) & \
    (a1['Difference'] < 0) & \
    (a1['Net Totals'] == 0)
# Use np.where() to assign values based on the mask
a1['Remarks'] = np.where(mask8, 'Sett in Current month, Refunded Subsequently', a1['Remarks'])



# Create a boolean mask for the condition
mask9 = (a['Remarks'] == '') & \
    (a['POS -ve'] == 0) & \
    (a['MPR -ve'] == 0) & \
    (a['Difference'] > 0) & \
    (a['Net Totals'] == 0)
# Use np.where() to assign values based on the mask
a['Remarks'] = np.where(mask9, 'Inv in Current month, Sett subsequently', a['Remarks'])# Get the counts of unique values


  
a=a1
a1=a


# Create a boolean mask for the condition
mask10 = (a['Remarks'] == '') & \
    (a['POS +ve'] > 0) & \
    (a['POS -ve'] < 0) & \
    (a['MPR -ve'] == 0) & \
    (a['Net Totals'] == 0) & \
    (pd.to_datetime(a['Max Sett Date']) > End_date)

# Use np.where() to assign values based on the mask
a['Remarks'] = np.where(mask10, 'Reversal in Current month, Refunded subsequently', a['Remarks'])# Get the counts of unique values


# Create a boolean mask for the condition
mask11 = (a['Remarks'] == '') & \
    (a['POS +ve'] > 0) & \
    (a['POS -ve'] < 0) & \
    (a['MPR -ve'] == 0) & \
    (a['Net Totals'] == 0) & \
    (pd.to_datetime(a['Max Sett Date']) <= End_date)

# Use np.where() to assign values based on the mask
a['Remarks'] = np.where(mask11, 'Reversal in Current month, Refunded Previously', a['Remarks'])

# Create a boolean mask for the condition
mask12 = (a['Remarks'] == '') & \
    (a['POS +ve'] == 0) & \
    (a['POS -ve'] == 0) & \
    (a['MPR +ve'] == 0) & \
    (a['Net Totals'] == 0) & \
        (a['IT Count Totals'] == 0)

# Use np.where() to assign values based on the mask
a['Remarks'] = np.where(mask12, 'Refunded in Current month, Sett received Previously', a['Remarks'])

# Create a boolean mask for the condition
mask13 = (a['Remarks'] == '') & \
    (a['POS +ve'] == 0) & \
    (a['POS -ve'] == 0) & \
    (a['MPR +ve'] == 0) & \
    (a['Net Totals'] == 0) & \
        (a['IT Count Totals'] > 0)

# Use np.where() to assign values based on the mask
a['Remarks'] = np.where(mask13, 'Refunded in Current month, Sett received Previously', a['Remarks'])
# Get the counts of unique values

mask14 = (a['Remarks'] == '') & \
    (a['POS +ve'] > 0) & \
    (a['POS -ve'] < 0) & \
    (a['Net POS'] == 0) & \
    (a['MPR +ve'] == 0) & \
        (a['MPR -ve'] < 0) & \
    (a['Net Totals'] == 0)

# Use np.where() to assign values based on the mask
a['Remarks'] = np.where(mask14, 'Refunded in Current month, Sett received Previously', a['Remarks'])# Create a boolean mask for the condition
mask15 = (a['Remarks'] == '') & \
    (a['MPR +ve'] > 0) & \
    (a['Net MPR'] == 0) & \
    (a['Net Totals'] == 0) & \
    (pd.to_datetime(a['Max Sett Date']) <= End_date)

# Use np.where() to assign values based on the mask
a['Remarks'] = np.where(mask15, 'Reversal & Refund in Current month, Inv issued previously', a['Remarks'])# Create a boolean mask for the condition
mask16 = (a['Remarks'] == '') & \
    (a['POS +ve'] == 0) & \
    (a['POS -ve'] == 0) & \
    (a['Net MPR'] < 0) & \
    (a['Net Totals'] == 0)

# Use np.where() to assign values based on the mask
a['Remarks'] = np.where(mask16, 'Refunded in Current month, Sett received Previously', a['Remarks'])# Create a boolean mask for the condition
mask17 = (a['Remarks'] == '') & \
    (a['POS +ve'] == 0) & \
    (a['POS -ve'] == 0) & \
    (a['Net MPR'] > 0) & \
    (a['Net Totals'] == 0) & \
    (pd.to_datetime(a['Max Sett Date']) <= End_date) & \
    (pd.to_datetime(a['Max Inv Date']) <= End_date)

# Use np.where() to assign values based on the mask
a['Remarks'] = np.where(mask17, 'Sett in Current month, Inv issued previously', a['Remarks'])
# Create a boolean mask for the condition
mask18 = (a['Remarks'] == '') & \
    (a['POS +ve'] == 0) & \
    (a['POS -ve'] == 0) & \
    (a['Net MPR'] > 0) & \
    (a['Net Totals'] == 0) & \
        (a['IT Count Totals'] == 0) & \
    (pd.to_datetime(a['Max Sett Date']) > End_date)

# Use np.where() to assign values based on the mask
a['Remarks'] = np.where(mask18, 'Sett in Current month, Refunded Subsequently', a['Remarks'])

# Create a boolean mask for the condition
mask19 = (a['Remarks'] == '') & \
    (a['POS +ve'] > 0) & \
    (a['POS -ve'] < 0) & \
        (a['Net POS'] == 0) & \
    (a['Net MPR'] > 0) & \
    (a['Net Totals'] == 0) & \
    (pd.to_datetime(a['Max Sett Date']) > End_date)

# Use np.where() to assign values based on the mask
a['Remarks'] = np.where(mask19, 'Sett in Current month, Refunded Subsequently', a['Remarks'])

# Create a boolean mask for the condition
mask20 = (a['Remarks'] == '') & \
    (a['POS +ve'] > 0) & \
    (a['POS -ve'] < 0) & \
        (a['Difference'] > 0) & \
    (a['Net MPR'] > 0) & \
    (a['Net Totals'] == 0) & \
    (pd.to_datetime(a['Max Sett Date']) > End_date)

# Use np.where() to assign values based on the mask
a['Remarks'] = np.where(mask20, 'Inv in Current month, Sett subsequently', a['Remarks'])

# Create a boolean mask for the condition
mask21 = (a['Remarks'] == '') & \
    (a['Net MPR'] > 0) & \
        (a['IT Totals'] == 0) & \
    (a['Net Totals'] < 0) & \
        (a['IT Count Totals'] == 0)

# Use np.where() to assign values based on the mask
a['Remarks'] = np.where(mask21, 'Sett in Current month, Refund Pending', a['Remarks'])

# Create a boolean mask for the condition
mask22 = (a['Remarks'] == '') & \
    (a['POS -ve'] < 0) & \
    (a['Net POS'] < 0) & \
        (a['Difference'] < 0) & \
    (a['Net Totals'] < 0)
# Use np.where() to assign values based on the mask
a['Remarks'] = np.where(mask22, 'Sett in Current month/previously, Refund Pending', a['Remarks'])


# Create a boolean mask for the condition
mask23 = (a['Remarks'] == '') & \
    (a['Net MPR'] > 0) & \
    (a['Net POS'] > 0) & \
        (a['Difference'] < 0) & \
    (a['Net Totals'] < 0)
# Use np.where() to assign values based on the mask
a['Remarks'] = np.where(mask23, 'Sett in Current month/previously, Refund Pending', a['Remarks'])

# Create a boolean mask for the condition
mask24 = (a['Remarks'] == '')
a['Remarks'] = np.where(mask24, 'To be checked', a['Remarks'])
a.to_excel('output.xlsx', index=False)





