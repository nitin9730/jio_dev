import pandas as pd


df=pd.read_csv('/Users/nitin14.patil/Downloads/missing_mcc_code_metro 2.csv')


max_rows_per_sheet = 10000

# Calculate the number of sheets needed
num_sheets = (len(df) // max_rows_per_sheet) + 1

# Create an Excel writer object
with pd.ExcelWriter('Metro_files.xlsx', engine='xlsxwriter') as writer:
    for i in range(num_sheets):
        start_row = i * max_rows_per_sheet
        end_row = (i + 1) * max_rows_per_sheet
        
        # Slice the DataFrame for the current sheet
        chunk = df.iloc[start_row:end_row]
        
        # Write the chunk to a new sheet
        chunk.to_excel(writer, sheet_name=f'Sheet{i + 1}', index=False)

print("Export completed successfully!")







import pandas as pd

# Assuming you already have df
max_rows_per_file = 10000  # Example: 7 lakh rows per file

num_files = (len(df) // max_rows_per_file) + 1  # Calculate the number of files

for i in range(num_files):
    start_row = i * max_rows_per_file
    end_row = (i + 1) * max_rows_per_file
    
    # Slice the DataFrame for the current file
    chunk = df.iloc[start_row:end_row]
    
    # Create a new Excel file for each chunk
    chunk.to_excel(f'Metro_file_part_{i + 1}.xlsx', index=False)

print("File export completed successfully!")









import xlwings as xw

# Open Excel workbook
wb = xw.Book('Metro_files.xlsx')

# Access a sheet
sheet = wb.sheets['Sheet1']

# Read data
data = sheet.range('A1').value

# Write data
sheet.range('B1').value = 'Updated Value'
