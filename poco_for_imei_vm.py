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
browser = webdriver.Chrome(service=service, options=chrome_options)

browser.get('https://in.prm.mi.com/#/enterprise_center/imei-report')

browser.maximize_window()

print("browser open")


user_n=browser.find_elements(By.XPATH,"//input[@name='account']")
user_p=browser.find_elements(By.XPATH,"//input[@name='password']")
sleep(2)
user_n[0].send_keys("8951981136")
user_p[0].send_keys("Ril@2024")
# user_n.send_keys(Keys.ENTER)
sleep(2)
# captcha_text
 #   image_element = browser.find_element(By.ID,"Login1_imgCaptcha")

print("username and password added")


submit=browser.find_elements(By.XPATH,"//button[@type='submit']")
submit[0].click()


print("username and password submited")

sleep(5)

browser.get('https://in.prm.mi.com/#/enterprise_center/imei-report')
sleep(6)


expand=browser.find_elements(By.XPATH,"//div[contains(text(),'Expansion')]")
# expand[0].click()
sleep(2)


print("calender setting initiated")

calender=browser.find_elements(By.XPATH,"//body/div[@id='app']/div[@class='enterprise-center-layout new-layout-component']/div/div[@class='enterprise-center-container']/div[@class='right-part']/div[@class='report']/div[@class='report-form']/div/div[1]/div[2]/div[1]")
sleep(2)


calender[0].click()
date=browser.find_elements(By.XPATH,"//td[@title='2024-09-02']//div[@class='rc-picker-cell-inner'][normalize-space()='2']")
date[0].click()
date1=browser.find_elements(By.XPATH,"//td[@title='2024-09-02']//div[@class='rc-picker-cell-inner'][normalize-space()='2']")
date1[0].click()
sleep(2)
#date=browser.find_elements(By.XPATH,"//td[@title='2024-09-02']//div[@class='rc-picker-cell-inner'][normalize-space()='2']")
Search=browser.find_elements(By.XPATH,"//div[normalize-space()='Search']")
Search[0].click()

#task_center=browser.find_elements(By.XPATH,"//div[normalize-space()='Go To Task Center']")
browser.get('https://in.prm.mi.com/#/enterprise_center')


browser.get('https://in.prm.mi.com/#/enterprise_center/imei-report')

print("file download initiated")

sleep(150)

browser.refresh()
sleep(10)
task=browser.find_elements(By.XPATH,"//div[normalize-space()='Task Center']")
task[0].click()
sleep(10)



download=browser.find_elements(By.XPATH,"//tbody/tr[1]/td[4]/button[1]")
download[0].click()

print("file download completed")



