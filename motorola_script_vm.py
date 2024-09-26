import pandas as pd
from time import sleep
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select

# Set up Chrome options
chrome_options = Options()
chrome_options.add_argument('--disable-gpu')
# Uncomment below to run in headless moade
# chrome_options.add_argument('--headless')
chrome_options.add_argument('--headless')  # Run Chrome in headless mode
chrome_options.add_argument('--no-sandbox')  # Bypass OS security model
chrome_options.add_argument('--disable-dev-shm-usage')  # Overcome limited resource problems
#chrome_options.binary_location = '/usr/bin/google-chrome'
# Add the path to the Chrome binary
chrome_options.binary_location = '/usr/bin/google-chrome'

# Initialize the Chrome driver with service
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=chrome_options)

# Open the website
driver.get('https://moto.nuralsales.app/Login/motorola/Login.aspx')
driver.maximize_window()

# Log in
user_n = driver.find_element(By.ID, 'txtUsername')
user_p = driver.find_element(By.ID, 'txtPassword')
user_n.send_keys("LFRaccounts")
user_p.send_keys("admin@123")

submit = driver.find_element(By.ID, 'btnSubmit')
submit.click()

sleep(5)

# Navigate to the report
driver.get('https://moto.nuralsales.app/Reports/SalesChannel/TertioryReportFlatSMSWMCCMNCinfo.aspx')

# Set the desired dates
desired_date = '07/09/2024'

from_date_input = driver.find_element(By.ID, 'ctl00_contentHolderMain_ucDateFrom_txtDate')
to_date_input = driver.find_element(By.ID, 'ctl00_contentHolderMain_ucDateTo_txtDate')

# Set the dates using JavaScript
driver.execute_script("arguments[0].value = arguments[1];", from_date_input, desired_date)
driver.execute_script("arguments[0].value = arguments[1];", to_date_input, desired_date)

# Optionally, trigger a change event if necessary
driver.execute_script("arguments[0].dispatchEvent(new Event('change', { bubbles: true }));", from_date_input)
driver.execute_script("arguments[0].dispatchEvent(new Event('change', { bubbles: true }));", to_date_input)

sleep(5)

# Select "Tertiary Considered Date" from the dropdown
dropdown = WebDriverWait(driver, 10).until(
    EC.element_to_be_clickable((By.ID, "ctl00_contentHolderMain_ddlDateType"))
)   
dropdown_select = Select(dropdown)
dropdown_select.select_by_visible_text("Tertiary Considered Date")

# Click the download button
download_click = driver.find_element(By.ID, 'ctl00_contentHolderMain_btnSearch')
download_click.click()

# Add any additional steps or cleanup if needed

import os
from google.cloud import storage

# Path to your service account key file
service_account_key_path = '/home/fynd/python/service_account.json'

# Set the environment variable for authentication
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = service_account_key_path

# Set the path to the Downloads folder
downloads_folder = '/home/fynd/Downloads/'  # Replace with your actual Downloads path

# Initialize Google Cloud Storage client
client = storage.Client()

# Specify the bucket name and remote file path
bucket_name = 'datafiles_staging'
remote_file_path = 'IMEI_Details'
bucket = client.bucket(bucket_name)

def upload_files_to_gcs():
    # List all files in the Downloads folder
    files = [f for f in os.listdir(downloads_folder) if os.path.isfile(os.path.join(downloads_folder, f))]
    
    # Loop through each file and upload it to the GCS bucket
    for file_name in files:
        file_path = os.path.join(downloads_folder, file_name)
        
        # Create a blob object with the remote path prefix in GCS
        blob = bucket.blob(f'{remote_file_path}/{file_name}')
        
        # Upload the file to GCS
        blob.upload_from_filename(file_path)
        print(f'Uploaded {file_name} to GCS bucket {bucket_name}/{remote_file_path}.')
        
        # Delete the file from Downloads folder after successful upload
        os.remove(file_path)
        print(f'Deleted {file_name} from Downloads folder.')

# Call the function to upload files and delete them
upload_files_to_gcs()