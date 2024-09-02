import pandas as pd
import os
import glob
import chardet
import numpy as np

file_path = "/Users/nitin14.patil/Library/CloudStorage/OneDrive-RelianceCorporateITParkLimited/Documents/python_work/SD Reco"


CoFoX01 = pd.read_excel(f"{file_path}/CoFoX01.xlsx", sheet_name='To Neeraj')

CoFoX01_D = pd.read_excel(f"{file_path}/CoFoX01_D.xlsx")

with open(f"{file_path}/STO.csv", 'rb') as f:
    result = chardet.detect(f.read())

print(result)


Sales = pd.read_csv(f"{file_path}/Sales.csv", delimiter='\t', encoding='utf-16')


GRDC= pd.read_csv(f"{file_path}/GRDC.csv" )


STO= pd.read_csv(f"{file_path}/STO.csv", delimiter='\t', encoding='UTF-16')


Deleted= pd.read_csv(f"{file_path}/Deleted.csv", delimiter='\t', encoding='UTF-16')


Short_Closed = pd.read_csv(f"{file_path}/Short_Closed.csv", delimiter='\t', encoding='UTF-16')


Sales_ASP2 = pd.read_excel(f"{file_path}/Sales_ASP2.xlsx")



CoFoX01_D.columns

CoFoX01_D['Article Code'] = CoFoX01_D['Seller Identifier']

CoFoX01_N=CoFoX01[['Article ID','STO Price']]

CoFoX01_N.rename(columns={'Article ID':'Article Code'}, inplace=True)

merged_1=pd.merge(CoFoX01_D, CoFoX01_N, how='left', on='Article Code')

merged_1.rename(columns={'STO Price':'old_price'},inplace=True)

# Create column C
merged_1['Final Selling Price to pick'] = merged_1['old_price'].replace('', pd.NA).fillna(merged_1['Selling Price'])

merged_1['Gap vs. COFOX01']=merged_1['Final Selling Price to pick']-merged_1['Selling Price']

CoFoX01_f=merged_1


Sales['article_id'] = Sales['Article Id'].astype(str).str.replace('.0', '', regex=False)

# Using map() function to combine two columns of text
Sales['Concat'] = Sales['Store No'].map(str) + "" + Sales['article_id']

Sales['Article Code Len'] = Sales['article_id'].str.len()

Sales['Article Starting Digit'] = Sales['article_id'].str.slice(0, 1)

Sales['Article to Include'] = np.where(
    ~Sales['Article Starting Digit'].astype(str).str.startswith('6'),
    np.where(Sales['Article Code Len'] == 9, 'Yes', 'No'),
    'No'
)


# List of specific locations
location_list = [
    'Reliance Retail Limited', 'Quartet', 'Erik sons', 'Lakhdatar Sales',
    'Sri Venkata Sai Enter', 'Parekh Telecom', 'SHRI GANESH ENTERPRISES'
]


# Function to apply the logic
def determine_column_value(row):
    if pd.isna(row) or row == "":
        return "Yes"
    elif row in location_list:
        return "Yes"
    else:
        return "No"

# Applying the function to create the new column
Sales['Delivery location to Include'] = Sales['Deliverylocation'].apply(determine_column_value)



# Creating 'Final inclusion' column based on the given logic
Sales['Final inclusion'] = np.where(
    Sales['Article to Include'] == 'Yes',
    np.where(Sales['Delivery location to Include'] == 'Yes', 'Yes', 'No'),
    np.nan  # Default value if 'Article to Include' is not 'Yes'
)


merged = pd.merge(Sales, CoFoX01_f, how='left', left_on='Article Id', right_on='Article Code')

# Create 'MAP Rate' column based on the condition
Sales['MAP Rate'] = np.where(merged['Final inclusion'] == "Yes", 
                             merged['Final Selling Price to pick'], 
                             np.nan)

# Ensure Quantity and Returnedqty are in compatible types
Sales['Quantity'] = Sales['Quantity'].replace(0,'').astype(float)  # Convert Quantity to float

Sales['Returnedqty'] = Sales['Returnedqty'].fillna(0)

# Ensure Quantity is float
Sales['Quantity'] = Sales['Quantity'].astype(float)

# Ensure Returnedqty is float
Sales['Returnedqty'] = Sales['Returnedqty'].astype(float)

# Debug: Print the data types to ensure they're correct
print(Sales.dtypes)

# Create 'Net Sales Qty' column based on the condition
Sales['Net Sales Qty'] = np.where(Sales['Final inclusion'] == 'Yes', 
                                  Sales['Quantity'] - Sales['Returnedqty'], 
                                  0)

Sales['Value']=np.where(Sales['Final inclusion'] == 'Yes',
                        Sales['MAP Rate']*Sales['Net Sales Qty'],
                        np.nan)


# Using map() function to combine two columns of text
GRDC['Concat'] = GRDC['store_no'].map(str) + "" + GRDC['article_no'].map(str)


GRDC['article_id'] = GRDC['article_no'].astype(str)
# .str.rstrip('.0')


GRDC['Article Code Len'] = GRDC['article_no'].apply(lambda x:len(str(x)))


GRDC['Article Starting Digit'] = GRDC['article_id'].str.slice(0, 1)


# Convert 'Article Starting Digit' to string
GRDC['Article Starting Digit'] = GRDC['Article Starting Digit'].astype(str)

# Initialize 'Article to Include' with 'No'
GRDC['Article to Include'] = 'No'

# Apply conditions
condition = (~GRDC['Article Starting Digit'].str.startswith('6')) & (GRDC['Article Code Len'] == 9)
GRDC.loc[condition, 'Article to Include'] = 'Yes'



merged_1 = pd.merge(GRDC, CoFoX01_f, how='left', left_on='article_no', right_on='Article Code')

# Create 'MAP Rate' column based on the condition
GRDC['MAP Rate'] = np.where(GRDC['Article to Include'] == "Yes", 
                             merged_1['Final Selling Price to pick'], 
                             np.nan)

GRDC['Value']=np.where(GRDC['Article to Include']=='Yes',
                       GRDC['quantity']*GRDC['MAP Rate'],
                       0)


#STO

# Using map() function to combine two columns of text
STO['Concat'] = STO['Store No'].map(str) + "" + STO['Article_no'].map(str)


STO['article_id'] = STO['Article_no'].astype(str).str.replace('.0', '', regex=False)


STO['Article Code Len'] = STO['article_id'].apply(lambda x:len(str(x)))


STO['Article Starting Digit'] = STO['article_id'].str.slice(0, 1)

# Initialize 'Article to Include' with 'No'
STO['Article to Include'] = 'No'

# Apply conditions
condition = (~STO['Article Starting Digit'].str.startswith('6')) & (STO['Article Code Len'] == 9)
STO.loc[condition, 'Article to Include'] = 'Yes'



merged_1 = pd.merge(STO, CoFoX01_f, how='left', left_on='Article_no', right_on='Article Code')

# Create 'MAP Rate' column based on the condition
STO['MAP Rate'] = np.where(STO['Article to Include'] == "Yes", 
                             merged_1['Final Selling Price to pick'], 
                             np.nan)

STO['STO Created']=np.where(STO['Article to Include']=='Yes',
                            STO['MAP Rate']*STO['STO Created Qty'],
                            0)
STO['STO Issued']=np.where(STO['Article to Include']=='Yes',
                            STO['MAP Rate']*STO['Actual issue Qty'],
                            0)

STO['STO Executed'] = STO.groupby('Concat')['Actual issue Qty'].transform('sum')

#Deleted

Deleted['Deleted Qty'] = Deleted['STO Created Qty'] - Deleted['Actual issue Qty']





# Using map() function to combine two columns of text
Deleted['Concat'] = Deleted['Store No'].map(str) + "" + Deleted['Article_no'].map(str)


Deleted['article_id'] = Deleted['Article_no'].astype(str).str.rstrip('.0')


Deleted['Article Code Len'] = Deleted['article_id'].apply(lambda x:len(str(x)))


Deleted['Article Starting Digit'] = Deleted['article_id'].str.slice(0, 1)

# Initialize 'Article to Include' with 'No'
Deleted['Article to Include'] = 'No'

# Apply conditions
condition = (~Deleted['Article Starting Digit'].str.startswith('6')) & (Deleted['Article Code Len'] == 9)
Deleted.loc[condition, 'Article to Include'] = 'Yes'



merged_2 = pd.merge(Deleted, CoFoX01_f, how='left', left_on='Article_no', right_on='Article Code')

# Create 'MAP Rate' column based on the condition
Deleted['MAP Rate'] = np.where(Deleted['Article to Include'] == "Yes", 
                             merged_2['Final Selling Price to pick'], 
                             np.nan)

Deleted['Value']=np.where(Deleted['Article to Include']=='Yes',
                       Deleted['Deleted Qty']*Deleted['MAP Rate'],
                       0)



# Using map() function to combine two columns of text
Short_Closed['Concat'] = Short_Closed['Store No'].map(str) + "" + Short_Closed['Article_no'].map(str)


Short_Closed['article_id'] = Short_Closed['Article_no'].astype(str).str.replace('.0', '', regex=False)


Short_Closed['Article Code Len'] = Short_Closed['article_id'].apply(lambda x:len(str(x)))


Short_Closed['Article Starting Digit'] = Short_Closed['article_id'].str.slice(0, 1)




# Initialize 'Article to Include' with 'No'
Short_Closed['Article to Include'] = 'No'

# Apply conditions
condition = (~Short_Closed['Article Starting Digit'].str.startswith('6')) & (Short_Closed['Article Code Len'] == 9)
Short_Closed.loc[condition, 'Article to Include'] = 'Yes'




merged_3 = pd.merge(Short_Closed, CoFoX01_f, how='left', left_on='Article_no', right_on='Article Code')

# Create 'MAP Rate' column based on the condition
Short_Closed['MAP Rate'] = np.where(Short_Closed['Article to Include'] == "Yes", 
                             merged_3['Final Selling Price to pick'], 
                             np.nan)

Short_Closed['Value']=np.where(Short_Closed['Article to Include']=='Yes',
                       Short_Closed['Short Close Qty']*Short_Closed['MAP Rate'],
                       0)



#Sales ASP

# Using map() function to combine two columns of text
Sales_ASP2['Concat'] = Sales_ASP2['Store Code'].map(str) + "" + Sales_ASP2['Item Code'].map(str)


Sales_ASP2['article_id'] = Sales_ASP2['Item Code'].astype(str).str.replace('.0', '', regex=False)


Sales_ASP2['Article Code Len'] = Sales_ASP2['article_id'].apply(lambda x:len(str(x)))


Sales_ASP2['Article Starting Digit'] = Sales_ASP2['article_id'].str.slice(0, 1)



# Initialize 'Article to Include' with 'No'
Sales_ASP2['Article to Include'] = 'No'

# Apply conditions
condition = (~Sales_ASP2['Article Starting Digit'].str.startswith('6')) & (Sales_ASP2['Article Code Len'] == 9)
Sales_ASP2.loc[condition, 'Article to Include'] = 'Yes'



merged_3 = pd.merge(Sales_ASP2, CoFoX01_f, how='left', left_on='Item Code', right_on='Article Code')

# Create 'MAP Rate' column based on the condition
Sales_ASP2['MAP Rate'] = np.where(Sales_ASP2['Article to Include'] == "Yes", 
                             merged_3['Final Selling Price to pick'], 
                             np.nan)

Sales_ASP2['Value']=np.where(Sales_ASP2['Article to Include']=='Yes',
                       Sales_ASP2['MAP Rate']*1,
                       0)





STO['Concat'] = STO['Concat'].astype(str).str.replace('.0', '', regex=False)




# Sum of 'Net Sales Qty' where 'Concat' matches 'STO.Concat' and 'Final inclusion' is "Yes"
sales_sum = Sales[Sales['Final inclusion'] == "Yes"].groupby('Concat')['Net Sales Qty'].sum().reset_index(name='sales_sum')

# Step 2: Calculate COUNTIFS equivalent for Sales_ASP2
# Count where 'Article to Include' is "Yes" and 'Concat' matches 'STO.Concat'
asp2_count = Sales_ASP2[Sales_ASP2['Article to Include'] == "Yes"].groupby('Concat').size().reset_index(name='asp2_count')

# Step 3: Calculate COUNTIFS equivalent for Sales
# Count where 'Concat' matches 'STO.Concat' and 'Article to Include' is "yes"
sto_count = STO[STO['Article to Include'] == "Yes"].groupby('Concat').size().reset_index(name='sto_count')

# Step 4: Merge the results into the STO DataFrame
# STO = pd.concat([STO,sales_sum], axis=1, join='left')
STO = STO.merge(sales_sum, on='Concat', how='left')
STO = STO.merge(asp2_count, on='Concat', how='left')
STO = STO.merge(sto_count, on='Concat', how='left')

# Step 5: Fill NaN values with 0 for safe calculations
STO['sales_sum'] = STO['sales_sum'].fillna(0)
STO['asp2_count'] = STO['asp2_count'].fillna(0)
STO['sto_count'] = STO['sto_count'].fillna(0)

# Step 6: Calculate 'Sales Qty' with error handling (avoid division by zero)
STO['Sales Qty'] = (
    (STO['sales_sum'] + STO['asp2_count']) /
    STO['sto_count'].replace(0, 1)  # Avoid division by zero
).fillna(0)  # Replace NaNs with 0

STO['Sales Value'] = np.where(
    STO['Article to Include']=='Yes',
    STO['Sales Qty'] * STO['MAP Rate'],
    0)

STO['Sales Adj']= np.where(
    STO['STO Executed'] < STO["Sales Qty"],
    STO['STO Executed'] - STO["Sales Qty"],
    0)
STO['Adj Value'] = np.where(
    STO['Article to Include']=='Yes',
    STO['MAP Rate'] * STO["Sales Adj"],
    0)

#export_output
# Dictionary of DataFrames and their corresponding sheet names
dfs2 = {
    'CoFoX01': CoFoX01_f,
    'Sales': Sales,
    'GRDC': GRDC,
    'STO': STO,
    'Deleted': Deleted,
    'Short Closed': Short_Closed,
    'Sales ASP2': Sales_ASP2
    
}

# Create a Pandas Excel writer using XlsxWriter as the engine
with pd.ExcelWriter('SD_Reco_07_Aug.xlsx', engine='xlsxwriter') as writer:
    for sheet_name, df in dfs2.items():
        df.to_excel(writer, sheet_name=sheet_name, index=False)


print("DataFrames have been written to 'SD_Reco_07_Aug.xlsx' successfully.")








