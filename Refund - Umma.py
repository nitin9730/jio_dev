import pandas as pd
import datetime
import numpy as np
import os
from openpyxl import load_workbook


file1=pd.read_excel('./input/PGR Utility Inc Data.xlsx')

file2=pd.read_excel('./input/Refund Validation.xlsx')

file3=pd.read_excel('./input/Roms Refund Trigger.xlsx')

file3.columns

file3['Source']="ROM'S"

file3=file3[['Source','airmail_id', 'shipment_id','refund_type',
       'Sum of AMOUNT']]

file3['Sum of AMOUNT'] = file3['Sum of AMOUNT'].apply(lambda x: -abs(x))

file1.columns

file1['Source']="UTILITY"

file1=file1[['Source','Airmail ID', 'Shipment ID','Order Item Status',
       'Amount To Be Given']]


file3=file3.rename(columns={'airmail_id':'Airmail ID','shipment_id':'Shipment ID','refund_type':'Order Item Status','Sum of AMOUNT':'Amount To Be Given'})


con_d=pd.concat([file1,file3],axis=0,ignore_index=True)









# Define the folder path
folder_path1 = '.'
folder_path2 = './RRA POS'
# Define the folder path
folder_path3 = './Reco'

# Initialize an empty list to store DataFrames
dfs1 = []
dfs2 = []
dfs3 = []

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


# Define a tolerance
tolerance = 1e-10

#For B2B_POS_DET_ASP_07202



# Replace 'file1.xlsx' and 'file2.xlsx' with the paths to your Excel files

# file1 = '/Users/nitin14.patil/Downloads/GST Reco Data.xlsx'
# # Read the Excel files
# main1 = pd.read_excel(file1, sheet_name='RRA POS')
# # Display the dataframes
# print("DataFrame 1:")
# print(main1)

main1 = result2


main1['Tax Amt'] = main1.apply(lambda row: (1 if row['BUSINESS_GROUP'] == "B2B" else -1) * (row['SGST_V'] + row['CGST_V'] + row['IGST_V']), axis=1)

# Add the "Total Amount" column
main1['Total Amount'] = main1.apply(lambda row: (1 if row['BUSINESS_GROUP'] == "B2B" else -1) * row['TAXABLE_AMT'] + row['Tax Amt'], axis=1)


# Filter the dataframe to exclude rows where the "DELETION FLAG" column has the value "x"
filtered_df = main1[main1['DELETION FLAG'] != 'x']


# Create the pivot table with the sum of "Total Amount" for each "Billing Document"
pivot1 = filtered_df.pivot_table(values='Total Amount', index='BILLING_DOCUMENT', aggfunc='sum').reset_index()








# # Load the workbook
# wb = load_workbook(file1)

# # Select the active sheet or a specific sheet
# sheet = wb['460']  # or wb['Sheet1'] for a specific sheet name

# # Read all data into a list of lists (each inner list represents a row)
# data = []
# for row in sheet.iter_rows(values_only=True):
#     data.append(row)






# # Convert the list of lists into a Pandas DataFrame
# main2 = pd.DataFrame(data, columns=sheet[1])


# # Use the first row as column headers and remove it from the data
# new_header = main2.iloc[0]  # Assuming the first row contains the headers
# main2 = main2[1:]  # Remove the first row (old headers)
# main2.columns = new_header  # Set the new headers




main2=result1


main2=main2.ffill()


print("\nDataFrame 2:")
print(main2)



# Convert columns to numeric format
main2['POS Number'] = main2['POS Number'].apply(pd.to_numeric)

# Convert columns to numeric format
main2['Transaction Number'] = main2['Transaction Number'].apply(pd.to_numeric)


# # Close the workbook after use
# wb.close()

# # Display the DataFrame (optional)
# print(main2.head())






# # Format the 'Posting Date' column to 'ddmmyyyy'
# main2['Posting Date'] = pd.to_datetime(main2['Posting Date'])





def parse_date(date_str):
    try:
        return pd.to_datetime(date_str, format='%d%m%Y')
    except ValueError:
        return pd.to_datetime(date_str, format='%d%m%Y')

main2['Posting Date'] = main2['Posting Date'].apply(parse_date)





# .dt.strftime('%d%m%Y')
# df['your_column'] = pd.to_datetime(df['your_column'])



main2.dtypes

# main2['Tender Value'] = main2['Tender Value'].str.replace(',', '').astype(float)


# Filter the dataframe to exclude rows where the "DELETION FLAG" column has the value "x"
filtered_df2 = main2[main2['Transaction Type'].isin([1183, 1233])]



filtered_df2['u_Posting Date'] = (
    filtered_df2['Posting Date'].astype(str)
    .str[-2:] + 
    filtered_df2['Posting Date'].astype(str).str[4:-2] + 
    filtered_df2['Posting Date'].astype(str).str[:4]
)



main2['BILLING_DOCUMENT'] = filtered_df2.apply(
    lambda row: f"{row['Store']}{row['POS Number']}{str(row['Transaction Number']).zfill(4)}{row['u_Posting Date']}",
    axis=1
)

main2['BILLING_DOCUMENT'] = main2['BILLING_DOCUMENT'].astype(str).str.replace("-","")

main2.dtypes





# main2['Tender Value']=main2.apply(lambda a: a.replace(',',''))




# Create the pivot table with the sum of "Total Amount" for each "Billing Document"
pivot2 = main2.pivot_table(values='Tender Value', index='BILLING_DOCUMENT', aggfunc='sum').reset_index()

# Lookup
# pivot1 to pivot2



# Merge DataFrame 1 and DataFrame 2 on 'transaction_no' with an indicator
merged1 = pd.merge(pivot1, pivot2, on='BILLING_DOCUMENT', how='left', indicator=True)

# # Find rows in df1 that do not have matching entries in df2
# exp1 = merged1[merged1['_merge'] != 'left_only']



merged1['diff'] = merged1['Total Amount'] - merged1['Tender Value']

# Use np.where to set values very close to zero to exactly 0
merged1['diff'] = np.where(np.abs(merged1['diff']) < tolerance, 0, merged1['diff'])

# Optionally, round the 'diff' column to 2 decimal places
merged1['diff'] = merged1['diff'].round(2)


pivot_1 =  merged1.drop(columns=['_merge'])

exp_1 = pivot_1[pivot_1['diff'] != 0]

# Merge the DataFrames on the 'Key' column
exp_report1 = pd.merge(exp_1, filtered_df, on='BILLING_DOCUMENT', how='left')
# Merge the DataFrames on the 'Key' column

exp_report_1=exp_report1[['COMPANY_CODE',	'BUSINESS_PLACE',	'GSTIN_S',	'COMPANY_NAME',	'PERIOD',	'BILLING_DOCUMENT',	'BILLING_TYPE',	'BILLING_DATE',	'TAXINVOICE',	'ORIGINAL_INVOICE',	'ORIGINAL_INVOICE_DATE',	'CUSTOMER',	'CUSTOMER_NAME',	'CUSTOMER_GSTN',	'CUSTOMER_REGION',	'PLACE_OF_SUPPLY',	'TOTAL_INVOICE_AMOUNT',	'TAXK2',	'TAXK3',	'REVERSE_CHARGE',	'GSTN_ECOM',	'CANCELLATION_FLAG',	'INVOICE_TYPE',	'EXPORT_TYPE',	'NOTE_TYPE',	'TYPE',	'SERVER',	'BUSINESS_GROUP',	'BUSINESS_LOCATION',	'BILLID',	'LINE_NUM',	'ARTICLE',	'ARTICLE_DESC',	'BILLING_QTY',	'UOM',	'TAXABLE_AMT',	'TAXM2',	'HSN_CODE',	'HSN_DESC',	'STORE',	'STORE_DESC',	'IGST_R',	'IGST_V',	'CGST_R',	'CGST_V',	'SGST_R',	'SGST_V',	'CESS_V',	'FKART',	'STORE_REGION',	'RECORD_DATE',	'LAST_MODIFIED_DATE',	'BEZEI',	'DISTRBUTION_CHANNEL',	'ACCOUNT_ASSIGNMENT_GROUP',	'POSTING_STATUS',	'EWAY_BILL_NO',	'EWAY_BILL_DATE',	'UPDATE_FLAG',	'UPDATE_DATE',	'CONFIRMATION_FLAG',	'FORMAT_FLAG',	'ID',	'IRNNUMBER',	'STEUC_NEW',	'DELETION FLAG',	'REASON',	'ERROR FLAG']]

# Dictionary of DataFrames and their corresponding sheet names
dfs1 = {
    'Main': main1,
    'Pivot': pivot_1,
    'Exp': exp_1,
    'Exp_Report': exp_report_1
}



# Create a Pandas Excel writer using XlsxWriter as the engine
with pd.ExcelWriter('./Reco/rra_data_pos_asp.xlsx', engine='xlsxwriter') as writer:
    for sheet_name, df in dfs1.items():
        df.to_excel(writer, sheet_name=sheet_name, index=False)

print("DataFrames have been written to 'rra_data_pos_asp.xlsx' successfully.")



# # print('Output file successfully exported.')
# # Set the maximum number of rows per sheet
# max_rows_per_sheet = 700000

# # Calculate the number of sheets needed
# num_sheets = (len(dfs1) // max_rows_per_sheet) + 1

# # Create an Excel writer object
# with pd.ExcelWriter('final_output.xlsx', engine='xlsxwriter') as writer:
#     for i in range(num_sheets):
#         start_row = i * max_rows_per_sheet
#         end_row = (i + 1) * max_rows_per_sheet
        
#         # Slice the DataFrame for the current sheet
#         chunk = dfs1.iloc[start_row:end_row]
        
#         # Write the chunk to a new sheet
#         chunk.to_excel(writer, sheet_name=f'Sheet{i + 1}', index=False)

# print("Export completed successfully!")











# Merge DataFrame 1 and DataFrame 2 on 'transaction_no' with an indicator
merged2 = pd.merge(pivot2, pivot1, on='BILLING_DOCUMENT', how='left', indicator=True)


# # Find rows in df1 that do not have matching entries in df2
# exp2 = merged2[merged2['_merge'] != 'left_only']


merged2['diff'] = merged2['Tender Value'] - merged2['Total Amount']


# Use np.where to set values very close to zero to exactly 0
merged2['diff'] = np.where(np.abs(merged2['diff']) < tolerance, 0, merged2['diff'])

# Optionally, round the 'diff' column to 2 decimal places
merged2['diff'] = merged2['diff'].round(2)

pivot_2 =  merged2.drop(columns=['_merge'])

exp_2 = pivot_2[pivot_2['diff'] != 0]

# Merge the DataFrames on the 'Key' column
exp_report2 = pd.merge(exp_2, main2, on='BILLING_DOCUMENT', how='left')
# Merge the DataFrames on the 'Key' column

exp_report_2=exp_report2[['Store',	'Posting Date',	'Transaction Index',	'POS Number',	'Transaction Number',	'Transaction Type',	'Means of Payment',	'Tender Value_y',	'Tender Currency',	'OrderI D',	'BILLING_DOCUMENT']]

# Dictionary of DataFrames and their corresponding sheet names
dfs2 = {
    'Main': main2,
    'Pivot': pivot_2,
    'Exp': exp_2,
    'Exp_Report': exp_report_2
}

# Create a Pandas Excel writer using XlsxWriter as the engine
with pd.ExcelWriter('./Reco/datamart_data_460_asp.xlsx', engine='xlsxwriter') as writer:
    for sheet_name, df in dfs2.items():
        df.to_excel(writer, sheet_name=sheet_name, index=False)


print("DataFrames have been written to 'datamart_data_460_asp.xlsx' successfully.")




# # print('Output file successfully exported.')
# # Set the maximum number of rows per sheet
# max_rows_per_sheet = 700000

# # Calculate the number of sheets needed
# num_sheets = (len(dfs2) // max_rows_per_sheet) + 1

# # Create an Excel writer object
# with pd.ExcelWriter('final_output.xlsx', engine='xlsxwriter') as writer:
#     for i in range(num_sheets):
#         start_row = i * max_rows_per_sheet
#         end_row = (i + 1) * max_rows_per_sheet
        
#         # Slice the DataFrame for the current sheet
#         chunk = dfs2.iloc[start_row:end_row]
        
#         # Write the chunk to a new sheet
#         chunk.to_excel(writer, sheet_name=f'Sheet{i + 1}', index=False)

# print("Export completed successfully!")















