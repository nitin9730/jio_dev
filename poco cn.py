import pandas as pd
import numpy as np
# Path to your Excel file
file1_path = '/Users/nitin14.patil/Library/CloudStorage/OneDrive-RelianceCorporateITParkLimited/Documents/python_work/poco dirstributers cn/billing_dump.xlsx'
file2_path = '/Users/nitin14.patil/Library/CloudStorage/OneDrive-RelianceCorporateITParkLimited/Documents/python_work/poco dirstributers cn/imei_details.xlsx'
file3_path = '/Users/nitin14.patil/Library/CloudStorage/OneDrive-RelianceCorporateITParkLimited/Documents/python_work/poco dirstributers cn/scheme_details.xlsx'

# Load the Excel file
ex1 = pd.ExcelFile(file1_path)
# Load the Excel file
ex2 = pd.ExcelFile(file2_path)
# Load the Excel file
ex3 = pd.ExcelFile(file3_path)


# Load specific sheets into DataFrames
billing_dump = pd.read_excel(ex1)
imei_details = pd.read_excel(ex2)
scheme_details = pd.read_excel(ex3)

imei_details['Duplicate IMEI Check'] = imei_details.groupby('IMEI')['IMEI'].transform('count')

imei_details['Dual IMEI Check']=imei_details['Duplicate IMEI Check'].apply(lambda x: 'Ok' if x==1  else 'On Hold')

# Assuming 'Article' is the column with counts
billing_qty_check = imei_details.groupby(['Billing Document', 'Distributor Code']).agg({'Article': 'count'}).reset_index()

# Rename 'Article' to 'Total'
billing_qty_check = billing_qty_check.rename(columns={'Article': 'article_count'})


# Sort the DataFrame by the 'Billing Document' column
billing_qty_check = billing_qty_check.sort_values(by='Billing Document')

# Optionally, reset the index if needed
billing_qty_check = billing_qty_check.reset_index(drop=True)

# Display the sorted DataFrame
print(billing_qty_check)







# Add the 'invoice_duplication' column based on the conditions
billing_qty_check['invoice_duplication'] = billing_qty_check.apply(
    lambda x: 'Yes' if (x['Billing Document'] == billing_qty_check['Billing Document'].shift(-1).loc[x.name]) or 
                        (x['Billing Document'] == billing_qty_check['Billing Document'].shift(1).loc[x.name]) 
              else '', 
    axis=1
)

billing_qty_check['Qty as per Primary']=billing_qty_check['Billing Document'].apply(
    lambda x: billing_dump.loc[billing_dump['Billing Document'] == x, 'Invoice Quantity'].sum()
    )







# Duplication Check

# billing_qty_check['Duplication Check']=billing_qty_check['Billing Do.apply(
#     lambda x: billing_dump.loc[billing_dump['Billing Document'] == x, 'Invoice Quantity'].sum()
#     )









# =IF(E5="Yes",IF(C5=F5,"Ok","To Hold"),"Ok")

















# # Create a new column 'C' in billing_qty_check with the sum results
# billing_qty_check['C'] = billing_qty_check['A'].apply(
#     lambda x: scheme_details.loc[scheme_details['D'] == x, 'R'].sum()
# )