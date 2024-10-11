from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from time import sleep
import pandas as pd
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime


chrome_options = Options()
# chrome_options.add_argument('--headless')
chrome_options.add_argument('--incognito')

browser = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=chrome_options)





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

Enterprise_center=browser.find_elements(By.XPATH,"//a[normalize-space()='Enterprise center']")

Enterprise_center[0].click()


sleep(10)

IMEI_SN=browser.find_elements(By.XPATH,"//a[normalize-space()='IMEI/SN Report']")

IMEI_SN[0].click()


sleep(5)

print('calender expanction initiated')



sleep(5)

expand=browser.find_elements(By.XPATH,"//div[@class='prm-btn info pe']")
expand[0].click()
sleep(5)

print('calender expanction success')


calender=browser.find_elements(By.XPATH,"//body/div[@id='app']/div[@class='enterprise-center-layout new-layout-component']/div/div[@class='enterprise-center-container']/div[@class='right-part']/div[@class='report']/div[@class='report-form']/div/div[1]/div[2]/div[1]")
sleep(2)


calender[0].click()

print('calender opened')

today_date = datetime.today().strftime('%Y-%m-%d')

date=browser.find_elements(By.XPATH,"//div[@class='rc-picker rc-picker-range rc-picker-focused']//div[@class='rc-picker-input rc-picker-input-active']//input")


date[0].send_keys(f"{today_date}")

date[0].click()


date_select=browser.find_elements(By.XPATH,f"//td[@title='{today_date}']//div[@class='rc-picker-cell-inner']")

date_select[0].click()


date_select[0].click()


print('dates selected')



#date=browser.find_elements(By.XPATH,"//td[@title='2024-09-02']//div[@class='rc-picker-cell-inner'][normalize-space()='2']")
Search=browser.find_elements(By.XPATH,"//div[normalize-space()='Search']")
Search[0].click()


print('dates searched')


sleep(10)

#task_center=browser.find_elements(By.XPATH,"//div[normalize-space()='Go To Task Center']")
browser.get('https://in.prm.mi.com/#/enterprise_center')


browser.get('https://in.prm.mi.com/#/enterprise_center/imei-report')


print('report download initiated wit fr some time')


sleep(150)

browser.refresh()
sleep(10)
task=browser.find_elements(By.XPATH,"//div[normalize-space()='Task Center']")
task[0].click()
sleep(10)



download=browser.find_elements(By.XPATH,"//tbody/tr[1]/td[4]/button[1]")
download[0].click()

print('report downloaded')