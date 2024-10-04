import osimport pandas as pdimport calendarimport numpy as npM_Y='August_2024'#####File1# RRA DUMPfile_path1 = '/Users/nitin14.patil/Library/CloudStorage/OneDrive-RelianceCorporateITParkLimited/Documents/python_work/PNL Finance/input/RRA_dump.xlsx'excel_file = pd.ExcelFile(file_path1)sheet_names = excel_file.sheet_namesrra_dump = pd.read_excel(excel_file)print('rra_dump loaded!')rra_dump.columns= ['Channel Type', 'Channel Type.1', 'Store', 'Store.1', 'RR State','Profit Center', 'Article', 'Article.1', 'Family', 'Brick', 'Brand_ID','Calendar Year/Month', 'Qty', 'Total Gross Sales (B)','Total Category Discount (C)','Total Net Sales with tax', 'Tax amount','Total Net Sales Without Tax', 'COGS As Per MAP','COGS', 'Gross Margin', 'Margin (%)','Markdown Price Difference(L)','Markdown Price Difference(%)','Category Margin', 'Category % Gross Margin','Margin Per Unit']columns=rra_dump.columnsdf=rra_dump# Function to convert to 'Month Year' formatdef convert_to_month_year(value):    # Split the float into month and year    month, year = str(value).split('.')    month = int(month)  # Convert month to integer    year = int(year)    # Convert year to integer        # Get month name    month_name = calendar.month_name[month]        # Return in 'Month Year' format    return f"{month_name}_{year}"# Apply the function to the DataFramedf['Month_Year'] = df['Calendar Year/Month'].apply(convert_to_month_year)# Display the resultprint(df.tail())fofo_filtered = df[    (df['Channel Type'].isin(['ASP B2B FOFO', 'POS B2B SALES', 'ASP FOFO', 'POS SALES'])) |    (df['Channel Type.1'].isin(['ASP B2B FOFO', 'POS B2B SALES', 'ASP FOFO', 'POS SALES']))    ]fofo_filtered = fofo_filtered[fofo_filtered['Month_Year']==M_Y]# Assuming rra_dump is already loaded as your DataFramefofo_pivot = pd.pivot_table(    fofo_filtered,     values=['Qty', 'Total Net Sales with tax', 'Total Net Sales Without Tax', 'COGS'],    index=['Article', 'Store', 'RR State', 'Family', 'Brand_ID'],  # Use index to group by    aggfunc='sum'  # Aggregation method is 'sum').reset_index()#GST Ratejmd_filtered = df[    (df['Channel Type'].isin(['ASP B2B SALES','ASP SALES'])) |    (df['Channel Type.1'].isin(['ASP B2B SALES','ASP SALES']))    ]jmd_filtered = jmd_filtered[jmd_filtered['Month_Year']==M_Y]# Assuming rra_dump is already loaded as your DataFramejmd_gst_pivot = pd.pivot_table(    jmd_filtered,     values=['Total Net Sales with tax', 'Total Net Sales Without Tax'],    index=['Article'],  # Use index to group by    aggfunc='sum'  # Aggregation method is 'sum').reset_index()jmd_gst_pivot['GST Rate'] = np.where(jmd_gst_pivot['Total Net Sales Without Tax'] != 0, jmd_gst_pivot['Total Net Sales with tax'] / jmd_gst_pivot['Total Net Sales Without Tax'], 0)jmd_gst_pivot['GST Rate']=jmd_gst_pivot['GST Rate'].round(2)jmd_gst_pivot.to_excel('jmd_gst_pivot.xlsx')#COGS p.u.# COGS p.u.# check_df = df[df['Article'].isin([490614124, 490614125, 490614126])]# check_df = df[df['Article'].isin([490614124,# 490819365,# 490868957])]df.dtypesjmd_cogs_pu = df[    (df['Channel Type'].isin(['ASP B2B FOFO', 'POS B2B SALES', 'ASP FOFO', 'POS SALES','POS B2B SALES','POS SALES','ASP B2B FOFO','ASP FOFO','BULK SALES','ASP B2B SALES','ASP SALES','Tablet Rent','SAP Billing'])) |    (df['Channel Type.1'].isin(['ASP B2B FOFO', 'POS B2B SALES', 'ASP FOFO', 'POS SALES','POS B2B SALES','POS SALES','ASP B2B FOFO','ASP FOFO','BULK SALES','ASP B2B SALES','ASP SALES','Tablet Rent','SAP Billing']))    ]jmd_cogs_pu_f = jmd_cogs_pu[jmd_cogs_pu['Month_Year']==M_Y]# Assuming rra_dump is already loaded as your DataFramecogs_pu_pivot = pd.pivot_table(    jmd_cogs_pu_f,     values=['Qty', 'COGS'],    index=['Article'],  # Use index to group by    aggfunc='sum'  # Aggregation method is 'sum').reset_index()cogs_pu_pivot['COGS p. u.']=cogs_pu_pivot['COGS']/cogs_pu_pivot['Qty']cogs_pu_pivot.to_csv('COGS p.u..csv')#Family and brandfnb_f = df[df['Month_Year']==M_Y]# Assuming rra_dump is already loaded as your DataFramefnb_pivot = pd.pivot_table(    fnb_f,     values=['Qty','Total Net Sales with tax','Total Net Sales Without Tax','COGS'],    index=['Article','Family', 'Brand_ID'],  # Use index to group by    aggfunc='sum'  # Aggregation method is 'sum').reset_index()fnb_pivot.to_csv('fnb.csv')#Articl_desc#Family and brandart_des_f = df[df['Month_Year']==M_Y]art_des_g=art_des_f[['Article','Article.1','Family', 'Brand_ID']]# Remove duplicate rowsart_des_g = art_des_g.drop_duplicates()art_des_g.to_csv('article_desc.csv')#####File2#IB SAPfile_path2 = '/Users/nitin14.patil/Library/CloudStorage/OneDrive-RelianceCorporateITParkLimited/Documents/python_work/PNL Finance/input/IB SAP.xlsx'excel_file = pd.ExcelFile(file_path2)sheet_names = excel_file.sheet_nameseb_data = pd.read_excel(excel_file,'EB')print('IB SAP loaded!')# Columns to modifycols_to_modify = ['Invoice Quantity', 'Net Value', 'Tax Amount', 'Pricing Condition Amount', 'Cost(COGS)']# Apply the condition to each columneb_data[cols_to_modify] = eb_data[cols_to_modify].apply(    lambda col: np.where(eb_data['Billing Type'] == 'ZRRE', -col, col))check_df = eb_data[    (eb_data['Material Number'] == 493711913)        # & (eb_data['Plant'] == 'R396')    # & (EB_Calcn_merge['Family'] == 'MWO/OTG')    # & (EB_Calcn_merge['Brand_ID'] == 'LG')    # & (EB_Calcn_merge['R4G State_'] == 'KARNATAKA')    ]# Group by 'Row Labels' and sum the 'Pricing Condition Amount'result = check_df.groupby('Material Number')['Pricing Condition Amount'].sum().reset_index()print(result)eb_data1=eb_datafile_path3 = '/Users/nitin14.patil/Library/CloudStorage/OneDrive-RelianceCorporateITParkLimited/Documents/python_work/PNL Finance/input/State Master.xlsx'excel_file = pd.ExcelFile(file_path3)sheet_names = excel_file.sheet_namesstate_master = pd.read_excel(excel_file,'Sheet1')print('State Master loaded!')state_master_unique = state_master[['Customer Code', 'State']].drop_duplicates(subset='Customer Code')merged_df = pd.merge(    eb_data1,    state_master_unique,    how='left',    left_on='Customer Number',    right_on='Customer Code')check_df = merged_df[    (merged_df['Material Number'] == 491166959)        # & (eb_data['Plant'] == 'R396')    # & (EB_Calcn_merge['Family'] == 'MWO/OTG')    # & (EB_Calcn_merge['Brand_ID'] == 'LG')    # & (EB_Calcn_merge['R4G State_'] == 'KARNATAKA')    ]# Group by 'Row Labels' and sum the 'Pricing Condition Amount'result = check_df.groupby('Material Number')['Pricing Condition Amount'].sum().reset_index()print(result)eb_data1=merged_df# Perform the merge with suffixes to avoid duplicate column namesstate_merge = pd.merge(eb_data1, state_master[['Customer Code', 'State']], how='left', left_on='Payer', right_on='Customer Code', suffixes=('', '_state_master'))state_merge.dtypes# Fill missing values in 'State_x' with the 'State' from the merge result (from 'State_state_master')merged_df['State_x'] = merged_df['State_x'].fillna(state_merge['Customer Code_state_master'])check_df = merged_df[    (merged_df['Material Number'] == 491166959)        # & (eb_data['Plant'] == 'R396')    # & (EB_Calcn_merge['Family'] == 'MWO/OTG')    # & (EB_Calcn_merge['Brand_ID'] == 'LG')    # & (EB_Calcn_merge['R4G State_'] == 'KARNATAKA')    ]# Group by 'Row Labels' and sum the 'Pricing Condition Amount'result = check_df.groupby('Material Number')['Pricing Condition Amount'].sum().reset_index()print(result)columns=eb_data.columnseb_pivot=pd.pivot_table(    merged_df,     values=['Invoice Quantity', 'Pricing Condition Amount', 'Tax Amount', 'Net Value', 'Cost(COGS)'],    index=['Material Number', 'Plant', 'State_x'],  # Use index to group by    aggfunc='sum'  # Aggregation method is 'sum').reset_index()EB_Calcn=merged_dfEB_Calcn['biz']='IB'file_path4 = '/Users/nitin14.patil/Library/CloudStorage/OneDrive-RelianceCorporateITParkLimited/Documents/python_work/PNL Finance/input/Region.xlsx'excel_file = pd.ExcelFile(file_path4)sheet_names = excel_file.sheet_namesRegion = pd.read_excel(excel_file)Region=Region.drop_duplicates()EB_Calcn_merge = pd.merge(    EB_Calcn,    Region,    how='left',    left_on=EB_Calcn['State_x'].str.upper().str.replace(' ', ''),    right_on=Region['R4G State_'].str.upper().str.replace(' ', ''))check_df = EB_Calcn_merge[    (EB_Calcn_merge['Material Number'] == 491166959)        & (EB_Calcn_merge['Plant'] == 'R396')    # & (EB_Calcn_merge['Family'] == 'MWO/OTG')    # & (EB_Calcn_merge['Brand_ID'] == 'LG')    # & (EB_Calcn_merge['R4G State_'] == 'KARNATAKA')    ]Family=art_des_f[['Article','Family']]Family=Family.rename(columns={'Article':'Material Number'})Family_unique = Family.drop_duplicates(subset='Material Number', keep='first')# Creating a lookup dictionary from the 'Family' DataFramelookup_dict = dict(zip(Family_unique['Material Number'], Family_unique['Family']))# Replacing the merge with a lambda + map approachEB_Calcn_merge['Family'] = EB_Calcn_merge['Material Number'].map(lambda x: lookup_dict.get(x, None))Brand=art_des_f[['Article','Brand_ID']]Brand=Brand.rename(columns={'Article':'Material Number'})Brand_unique = Brand.drop_duplicates(subset='Material Number', keep='first')# Creating a lookup dictionary from the 'Family' DataFramelookup_dict = dict(zip(Brand_unique['Material Number'], Brand_unique['Brand_ID']))# Replacing the merge with a lambda + map approachEB_Calcn_merge['Brand_ID'] = EB_Calcn_merge['Material Number'].map(lambda x: lookup_dict.get(x, None))EB_Calcn_merge['Tax rate']=EB_Calcn_merge['Pricing Condition Amount']/EB_Calcn_merge['Net Value']EB_Calcn_merge['COGS p.u.']=EB_Calcn_merge['Cost(COGS)']/EB_Calcn_merge['Invoice Quantity']EB_Calcn_merge['DSS Margin']=EB_Calcn_merge['Net Value']-EB_Calcn_merge['Cost(COGS)']EB_Calcn_merge.to_csv('EB_Calcn_merge.csv')EB_Calcn_merge.dtypes# check_df = EB_Calcn_merge[#     (EB_Calcn_merge['Material Number'] == 490054031)    #     # & (EB_Calcn_merge['Plant'] == 'R396')#     & (EB_Calcn_merge['Family'] == 'LAPTOP')#     & (EB_Calcn_merge['Brand_ID'] == 'HP')#     & (EB_Calcn_merge['R4G State_'] == 'M&G')    # ]EB_Calcn_merge_pivot = pd.pivot_table(    EB_Calcn_merge,     values=['Invoice Quantity', 'Pricing Condition Amount', 'Tax rate', 'Net Value','COGS p.u.','Cost(COGS)', 'DSS Margin'],    index=['Family','Brand_ID','Material Number','Region_','State_x','biz'],  # Use index to group by    aggfunc='sum'  # Aggregation method is 'sum').reset_index()EB_Calcn_merge_pivot.to_csv('EB_Calcn_pivot.csv')####B2B SAP BILLING FILEfile_path5 = '/Users/nitin14.patil/Library/CloudStorage/OneDrive-RelianceCorporateITParkLimited/Documents/python_work/PNL Finance/input/B2B SAP Billing.xlsx'excel_file = pd.ExcelFile(file_path5)sheet_names = excel_file.sheet_namessap_data = pd.read_excel(excel_file,'SAP')# Columns to modifycols_to_modify = ['Invoice Quantity', 'Net Value', 'Tax Amount', 'Pricing Condition Amount', 'Cost(COGS)']# Apply the condition to each columnsap_data[cols_to_modify] = sap_data[cols_to_modify].apply(    lambda col: np.where(sap_data['Billing Type'].isin(['ZRRE', 'YG2W']), -col, col))check_df = sap_data[    (sap_data['Material Number'] == 493838828)        # & (eb_data['Plant'] == 'R396')    # & (EB_Calcn_merge['Family'] == 'MWO/OTG')    # & (EB_Calcn_merge['Brand_ID'] == 'LG')    # & (EB_Calcn_merge['R4G State_'] == 'KARNATAKA')    ]# Group by 'Row Labels' and sum the 'Pricing Condition Amount'result = check_df.groupby('Material Number')['Invoice Quantity'].sum().reset_index()print(result)# Perform the merge with suffixes to avoid duplicate column namesstate_merge = pd.merge(sap_data, state_master[['Customer Code', 'State']], how='left', left_on='Payer', right_on='Customer Code', suffixes=('', '_state_master'))# Fill missing values in 'State_x' with the 'State' from the merge result (from 'State_state_master')sap_data['State'] = sap_data['State'].fillna(state_merge['State_state_master'])sap_pvt = pd.pivot_table(    sap_data,     values=['Invoice Quantity', 'Pricing Condition Amount', 'Tax Amount', 'Net Value','Cost(COGS)'],    index=['Material Number',	'Plant',	'State'],  # Use index to group by    aggfunc='sum'  # Aggregation method is 'sum').reset_index()SAP_Calc=sap_pvtSAP_Calc['biz']='SAP'SAP_Calcn_merge = pd.merge(    SAP_Calc,    Region,    how='left',    left_on=SAP_Calc['State'].str.upper().str.replace(' ', ''),    right_on=Region['R4G State_'].str.upper().str.replace(' ', ''))SAP_Calcn_merge=pd.merge(    SAP_Calcn_merge,    fnb_pivot[['Article','Family']],    how='left',    left_on='Material Number',    right_on='Article' )SAP_Calcn_merge=pd.merge(    SAP_Calcn_merge,    fnb_pivot[['Article','Brand_ID']],    how='left',    left_on='Material Number',    right_on='Article' )SAP_Calcn_merge['Tax rate']=SAP_Calcn_merge['Pricing Condition Amount']/SAP_Calcn_merge['Net Value']SAP_Calcn_merge['COGS p.u.']=SAP_Calcn_merge['Cost(COGS)']/SAP_Calcn_merge['Invoice Quantity']SAP_Calcn_merge['DSS Margin']=SAP_Calcn_merge['Net Value']/SAP_Calcn_merge['Cost(COGS)']SAP_Calcn_merge.to_csv('SAP_Calc.csv')SAP_Calcn_pivot = pd.pivot_table(    SAP_Calcn_merge,     values=['Invoice Quantity', 'Pricing Condition Amount', 'Tax rate', 'Net Value','COGS p.u.','Cost(COGS)', 'DSS Margin'],    index=['Family','Brand_ID','Material Number','Region_','State','biz'],  # Use index to group by    aggfunc='sum'  # Aggregation method is 'sum').reset_index()SAP_Calcn_pivot.to_csv('SAP.csv')#### SAP CN PVTsap_data_yg2w = sap_data[sap_data['Billing Type']=='YG2W']# Columns to modifycols_to_modify = ['Invoice Quantity', 'Net Value', 'Tax Amount', 'Pricing Condition Amount', 'Cost(COGS)']# Apply the condition to each columnsap_data_yg2w[cols_to_modify] = sap_data_yg2w[cols_to_modify].apply(    lambda col: np.where(sap_data_yg2w['Billing Type'].isin(['ZRRE', 'YG2W']), -col, col))# Perform the merge with suffixes to avoid duplicate column namesstate_merge = pd.merge(sap_data_yg2w, state_master[['Customer Code', 'State']], how='left', left_on='Payer', right_on='Customer Code', suffixes=('', '_state_master'))# Fill missing values in 'State_x' with the 'State' from the merge result (from 'State_state_master')sap_data_yg2w['State'] = sap_data_yg2w['State'].fillna(state_merge['State_state_master'])sap_data_yg2w_merge = pd.merge(    sap_data_yg2w,    Region,    how='left',    left_on=sap_data_yg2w['State'].str.upper().str.replace(' ', ''),    right_on=Region['R4G State_'].str.upper().str.replace(' ', ''))sap_cn_pvt = pd.pivot_table(    sap_data_yg2w_merge,     values=['Invoice Quantity', 'Pricing Condition Amount', 'Tax Amount', 'Net Value','Cost(COGS)'],    index=['Material Number','Plant','R4G State_', 'Region_'],  # Use index to group by    aggfunc='sum'  # Aggregation method is 'sum').reset_index()# Group by 'Row Labels' and sum the 'Pricing Condition Amount'result = check_df.groupby('Material Number')['Invoice Quantity'].sum().reset_index()print(result)sap_data.dtypes#### SAP CN CalnSAP_CN_Calc=sap_cn_pvtSAP_CN_Calc['biz']='SAP'# SAP_CN_Calc.dtypes# Region.dtypes# SAP_CN_Calcn_merge = pd.merge(SAP_CN_Calc, Region, left_on='State.1', right_on='R4G State_', how='left')SAP_CN_Calcn_merge=pd.merge(    SAP_CN_Calc,    fnb_pivot[['Article','Family']],    how='left',    left_on='Material Number',    right_on='Article' )SAP_CN_Calcn_merge=pd.merge(    SAP_CN_Calcn_merge,    fnb_pivot[['Article','Brand_ID']],    how='left',    left_on='Material Number',    right_on='Article' )SAP_CN_Calcn_merge['Tax rate']=SAP_CN_Calcn_merge['Pricing Condition Amount']/SAP_CN_Calcn_merge['Net Value']SAP_CN_Calcn_merge['COGS p.u.']=SAP_CN_Calcn_merge['Cost(COGS)']/SAP_CN_Calcn_merge['Invoice Quantity']SAP_CN_Calcn_merge['DSS Margin']=SAP_CN_Calcn_merge['Net Value']-SAP_CN_Calcn_merge['Cost(COGS)']SAP_CN_Calcn_merge.to_csv('SAP_CN_Calc.csv')SAP_CN_Calcn_pivot = pd.pivot_table(    SAP_CN_Calcn_merge,     values=['Invoice Quantity', 'Pricing Condition Amount', 'Tax rate', 'Net Value','COGS p.u.','Cost(COGS)', 'DSS Margin'],    index=['Family','Brand_ID','Material Number','Region_','R4G State_','biz'],  # Use index to group by    aggfunc='sum'  # Aggregation method is 'sum').reset_index()SAP_CN_Calcn_pivot.to_csv('SAP CN.csv')