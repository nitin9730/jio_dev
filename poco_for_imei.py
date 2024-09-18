from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from time import sleep
import pandas as pd
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Update this path to the location where you placed the downloaded chromedriver
chromedriver_path = "/Users/nitin14.patil/Downloads/chromedriver-mac-arm64 2/chromedriver"

chrome_options = Options()
# chrome_options.add_argument('--headless')
# chrome_options.add_argument('--incognito')

chrome_service = ChromeService(executable_path=chromedriver_path)

browser = webdriver.Chrome(service=chrome_service, options=chrome_options) 
pattern = '\d+'
pattern2 = '([0-2]?[0-9]|3[0-1])'
 
# chrome_options = webdriver.ChromeOptions()
#chrome_options.add_argument('--headless')  # Add any other options you need
 
# Use ChromeDriverManager to get the path to the ChromeDriver executable
# chrome_path = ChromeDriverManager().install()
# Create an instance of the WebDriverManager with the desired browser
#web_driver_manager = ChromeDriverManager(browser='chrome')
# browser = webdriver.Chrome(executable_path=chrome_path, options=chrome_options)
#browser.get('https://moto.nuralsales.app/Login/motorola/Login.aspx')
     
        
# options = Options()
# options.headless = True
browser.get('https://in.prm.mi.com/#/enterprise_center/imei-report')

browser.maximize_window()

user_n=browser.find_elements(By.XPATH,"//input[@name='account']")
user_p=browser.find_elements(By.XPATH,"//input[@name='password']")
sleep(2)
user_n[0].send_keys("8951981136")
user_p[0].send_keys("Ril@2024")
# user_n.send_keys(Keys.ENTER)
sleep(2)
# captcha_text
 #   image_element = browser.find_element(By.ID,"Login1_imgCaptcha")

submit=browser.find_elements(By.XPATH,"//button[@type='submit']")
submit[0].click()

sleep(5)

browser.get('https://in.prm.mi.com/#/enterprise_center/imei-report')
sleep(6)


expand=browser.find_elements(By.XPATH,"//div[contains(text(),'Expansion')]")
expand[0].click()
sleep(2)


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


sleep(150)

browser.refresh()
sleep(10)
task=browser.find_elements(By.XPATH,"//div[normalize-space()='Task Center']")
task[0].click()
sleep(10)



download=browser.find_elements(By.XPATH,"//tbody/tr[1]/td[4]/button[1]")
download[0].click()


browser.close()



