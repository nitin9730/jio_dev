import pandas as pd
import numpy as np
import os
import pandas as pd
# Path to your Excel file
# file_path = 'MOP IT ST Tables.xlsx'


# Define the folder path
folder_path1 = './IT Input'
folder_path2 = './ST Input'


# # Define the folder path
# folder_path1 = 'C:/Users/krishna.bathija/Downloads/GL Recon Python/GL MOP Reco/IT Input'
# folder_path2 = 'C:/Users/krishna.bathija/Downloads/GL Recon Python/GL MOP Reco/ST Input'

# Initialize an empty list to store DataFrames
dfs1 = []
dfs2 = []
# Iterate over all files in the folder
# Process the first folder
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

# # Show the result
# print(result)
# Iterate over all files in the folder
# Process the first folder
for file in os.listdir(folder_path2):
    file_path2 = os.path.join(folder_path2, file)
    if os.path.isfile(file_path2):
        print(f"Reading file: {file_path2}")
        df = pd.read_excel(file_path2)
        dfs2.append(df)

# Concatenate DataFrames if any were found
if dfs2:
    result2 = pd.concat(dfs2, axis=0, ignore_index=True)
    print("Concatenation successful!")
else:
    print("No files were found to concatenate in folder_path2.")





result1['OrderI D (20 digit)'] = result1['OrderI D'].str.slice(0, 20)
result2['OrderI D (20 digit)'] = result2['OrderI D'].str.slice(0, 20)

# Assuming 'Gross Amount' is the column name in your DataFrame
result1['Type.1'] = result1['Tender Value'].apply(lambda x: 'Fwd' if x > 0 else 'Rev')

# Assuming 'Gross Amount' is the column name in your DataFrame
result2['Type.1'] = result2['Gross amount'].apply(lambda x: 'Fwd' if x > 0 else 'Rev')


df_it=result1
df_st=result2

st_order_ids = df_st['OrderI D (20 digit)']
it_order_ids = df_it['OrderI D (20 digit)']

# Concatenate the columns vertically
merged_order_ids = pd.concat([st_order_ids, it_order_ids])

# Remove duplicate values
unique_order_ids = merged_order_ids.drop_duplicates().reset_index(drop=True)

unique_order_ids=pd.DataFrame(unique_order_ids)

# unique_order_ids.rename(columns={'OrderI D (20 digit)': 'OrderI D'}, inplace=True)

print(unique_order_ids.count())



f_a=df_it[df_it['OrderI D']=='BB65CC58B60EF07A80AB']
f_b=df_st[df_st['OrderI D']=='BB65CC58B60EF07A80AB']


# Trim leading and trailing spaces for all string columns
unique_order_ids = unique_order_ids.applymap(lambda x: x.strip() if isinstance(x, str) else x)
# Remove duplicate values
unique_order_ids = merged_order_ids.drop_duplicates().reset_index(drop=True)

# Create a pivot table
pivot_table_it = df_it.pivot_table(index='OrderI D (20 digit)', columns='Type.1', values='Tender Value', aggfunc='sum').reset_index()

# Replace NaN values with 0
pivot_table_it.fillna(0, inplace=True)




# Merge the unique_order_ids with the pivot_table on 'Order ID'
merged_data_it = pd.merge(unique_order_ids, pivot_table_it, how='left', on='OrderI D (20 digit)')

# Replace NaN values with 0
merged_data_it.fillna(0, inplace=True)


# Calculate the 'Net Invoice' column based on the specified logic
merged_data_it['Net Invoice'] = np.where(
    merged_data_it['Fwd'].notna(),
    np.where(merged_data_it['Rev'].notna(), merged_data_it['Fwd'] + merged_data_it['Rev'], merged_data_it['Fwd']),
    np.where(merged_data_it['Rev'].notna(), merged_data_it['Rev'], np.nan)
)





# Create a pivot table
pivot_table_st = df_st.pivot_table(index='OrderI D (20 digit)', columns='Type.1', values='Gross amount', aggfunc='sum')

# Reset the index to make 'Order ID' a column again (optional, depending on your needs)
pivot_table_st.reset_index(inplace=True)

# Replace NaN values with 0
pivot_table_st.fillna(0, inplace=True)



f_a=df_st[df_st['OrderI D (20 digit)']=='BB65CC58B60EF07A80AB']
f_b=df_st[df_st['OrderI D']=='BB65CC58B60EF07A80AB']




# Merge the unique_order_ids with the pivot_table on 'Order ID'
merged_data_st = pd.merge(unique_order_ids, pivot_table_st, how='left', on='OrderI D (20 digit)')





# Replace NaN values with 0
merged_data_st.fillna(0, inplace=True)


# Calculate the 'Net Invoice' column based on the specified logic
merged_data_st['Net Invoice'] = np.where(
    merged_data_st['Fwd'].notna(),
    np.where(merged_data_st['Rev'].notna(), merged_data_st['Fwd'] + merged_data_st['Rev'], merged_data_st['Fwd']),
    
    
    
    np.where(merged_data_st['Rev'].notna(), merged_data_st['Rev'], np.nan)
)





# Merge the DataFrames on the common column
all_orders = pd.merge(merged_data_it, merged_data_st, on="OrderI D (20 digit)", how='inner')


# Calculate the 'Net Invoice' column based on the specified logic
all_orders['Difference'] = np.where(
    all_orders['Net Invoice_x'].notna(),
    np.where(all_orders['Net Invoice_y'], all_orders['Net Invoice_x'] + all_orders['Net Invoice_y'], merged_data_it['Fwd']),
    np.where(all_orders['Net Invoice_y'], all_orders['Net Invoice_y'], np.nan)
)


all_orders['Difference']= all_orders['Net Invoice_x']-all_orders['Net Invoice_y']


all_orders.rename(columns={'OrderI D (20 digit)': 'Order List','Fwd_x':'POS +ve',	'Rev_x':'POS -ve',	'Net Invoice_x':'Net POS',	'Fwd_y':'MPR +ve',	'Rev_y':'MPR -ve',	'Net Invoice_y':'Net MPR',	'Difference':'Difference'}, inplace = True)
   


# Add the 'Remark' column based on the condition
all_orders['Remarks'] = np.where(all_orders['Difference'] == 0, 'Matched', '')# Get the counts of unique values

unique_counts = all_orders['Remarks'].value_counts()

# Convert the Series to a DataFrame to include counts and unique values
unique_counts_df = unique_counts.reset_index()
unique_counts_df.columns = ['Unique Value', 'Count']

# Print the DataFrame
print(unique_counts_df)


# Sum of individual columns
sum_column1 = all_orders['Net POS'].sum()
sum_column2 = all_orders['Net MPR'].sum()

print("Sum of column1:", sum_column1)
print("Sum of column2:", sum_column2)


a=all_orders


# a.columns
# List of columns to apply the logic to
columns_to_modify = ['Difference']


a[columns_to_modify] = a[columns_to_modify].applymap(lambda x: 0 if -10 <= x <= 10 else x)


a.loc[(a['Difference'] == 0) & (a['Remarks'] == ''), 'Remarks'] = 'Matched'

a1=a[a['Remarks']=='']


unique_counts = a1['Remarks'].value_counts()

# Convert the Series to a DataFrame to include counts and unique values
unique_counts_df = unique_counts.reset_index()
unique_counts_df.columns = ['Unique Value', 'Count']

# Print the DataFrame
print(unique_counts_df)

try:
    a1['quoted'] = '*' + a1['Order List'] + '*'
except:
    pass

# Set the maximum number of rows per sheet
max_rows_per_sheet = 700000

# Calculate the number of sheets needed
num_sheets = (len(a1) // max_rows_per_sheet) + 1

# Create an Excel writer object
with pd.ExcelWriter('output.xlsx', engine='xlsxwriter') as writer:
    for i in range(num_sheets):
        start_row = i * max_rows_per_sheet
        end_row = (i + 1) * max_rows_per_sheet
        
        # Slice the DataFrame for the current sheet
        chunk = a1.iloc[start_row:end_row]
        
        # Write the chunk to a new sheet
        chunk.to_excel(writer, sheet_name=f'Sheet{i + 1}', index=False)

print("Export completed successfully!")







# a1.to_excel('star_raw.xlsx')





