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

con_d['Amount To Be Given']=con_d['Amount To Be Given'].round(0)




check=con_d[con_d['Shipment ID']=='BB66CC399F0E514C2392-01']


# Corrected version
pivot_df = con_d.pivot_table(values='Amount To Be Given', index=['Shipment ID'], columns=['Source'], aggfunc='sum', margins=True, margins_name='Grand Total', fill_value=0).reset_index()

pivot_df['Remarks'] = pivot_df.apply(
    lambda x: 'Matched' if x['Grand Total'] <= 1 and x['Grand Total'] >= -1
    else "Refund not came in ROM'S" if x["ROM'S"] == 0 
    else "Refund not came in UTILITY" if x['UTILITY'] == 0 
    else "Delivery Fee" if x['Grand Total'] == -29
    else "difference" if x['Grand Total'] <= 10 and x['Grand Total'] >= -10
    else '',
    axis=1
)

pivot_df["ROM'S"]=pivot_df["ROM'S"].astype(int)

pivot_df.dtypes


# pivot_df.to_csv('test.csv')


L1=pivot_df

df = L1

# Define the category based on the actual remarks
categorys = ["Matched","Refund not came in ROM'S","Delivery Fee","Refund not came in UTILITY","difference"]

# Initialize summary DataFrame
summary_data = {
    "Shipment level Summary": [],
    "No. of Shipment IDs": [],
    "As per ROMS": [],
    "As per Utility": [],
    "As per Difference": [],
    "Diff": [],
    "Per Count Diff": []
}


for category in categorys:
    # Calculating summary values
    count_ids = df[df['Remarks'] == category].shape[0]
    sum_roms = df[df['Remarks'] == category]['ROM\'S'].sum()
    sum_utility = df[df['Remarks'] == category]['UTILITY'].sum() + df[df['Remarks'] == category]['Grand Total'].sum()
    sum_difference=df[df['Remarks']==category]['Grand Total'].sum()
    diff = sum_roms + sum_utility
    per_count_diff = diff / count_ids if count_ids > 0 else 0
    
    summary_data["Shipment level Summary"].append(category)
    summary_data["No. of Shipment IDs"].append(count_ids)
    summary_data["As per ROMS"].append(sum_roms)
    summary_data["As per Utility"].append(sum_utility)
    summary_data["As per Difference"].append(sum_difference)
    
    summary_data["Diff"].append(diff)
    summary_data["Per Count Diff"].append(per_count_diff)



# Append the total row
summary_data["Shipment level Summary"].append("Total")
summary_data["No. of Shipment IDs"].append(sum(summary_data["No. of Shipment IDs"]))
summary_data["As per ROMS"].append(sum(summary_data["As per ROMS"]))
summary_data["As per Utility"].append(sum(summary_data["As per Utility"]))
summary_data['As per Difference'].append(sum(summary_data["As per Difference"]))
summary_data["Diff"].append(sum(summary_data["Diff"]))
summary_data["Per Count Diff"].append(
    sum(summary_data["Diff"]) / sum(summary_data["No. of Shipment IDs"]) if sum(summary_data["No. of Shipment IDs"]) > 0 else 0
)

# Create the summary DataFrame
summary_df = pd.DataFrame(summary_data)

# Display the summary DataFrame
print(summary_df)

# Create an Excel writer object
with pd.ExcelWriter('./output/output.xlsx', engine='openpyxl') as writer:
    # Write df1 to start at cell B2
    summary_df.to_excel(writer, sheet_name='Sheet1', startrow=1, startcol=1, index=False)
    # Write df2 to start at cell B10
    pivot_df.to_excel(writer, sheet_name='Sheet1', startrow=1, startcol=10, index=False)
    
print("DataFrames written to 'output.xlsx' successfully.")
