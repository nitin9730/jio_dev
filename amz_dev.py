#!/usr/bin/env python3
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from time import sleep
import pandas as pd
from selenium.webdriver.common.keys import Keys

chrome_options = Options()
# chrome_options.add_argument('--headless')
chrome_options.add_argument('--incognito')

browser = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=chrome_options)
browser.get('https://www.amazon.in/gp/bestsellers/electronics/1389432031/ref=zg_bs_pg_1?ie=UTF8&pg=1')

HREF2 = ['https://www.amazon.in/Apple-iPhone-15-128-GB/dp/B0CHX1W1XY/ref=sr_1_1_sspa?dib=eyJ2IjoiMSJ9.8-aKrERwPzdGyJWfWOa56O2yDcolF6kjNajUNykMKg-4nrBHk0q7-6xivRGXMniwcl1CVWUCX2hcdKw2kXdjE_v5GJyu164FJRYDmn3l-QRTnG9SFTSpfzp1nRsp76VV_KeY7CTOjbn_yZZt9RDpc-A2jMMFvyeJqlQU-AkspI5jSUNceIzcDrCsPf-U7zghRc9jy6Xi9Rf0uUkoxWWMkXbGdBG-MmVXPAJlQ7mEnfvz6jSXWT1vZspVfPbAid0XujEF6CGZR8AOA5QNL1fXON_3NOKG63r_e96wHgFmBPc.pY0bTiXhQYOiC8UzWR9aRhrBvRVWhKnpsiBJuF4AvdA&dib_tag=se&keywords=iPhone+15&qid=1712560976&s=electronics&sr=1-1-spons&sp_csd=d2lkZ2V0TmFtZT1zcF9hdGY&psc=1']

browser.get(HREF2[0])
Names = browser.find_element(By.XPATH, "//span[@id='productTitle']")
Naam = Names.text

print(Naam)

desc02 = browser.find_elements(By.XPATH, "//i[@class='a-icon a-accordion-radio a-icon-radio-inactive']")
desc02[0].click()
sleep(3)

desc02 = browser.find_elements(By.XPATH, "//input[@aria-labelledby='chooseButton-announce']")
desc02[0].click()
sleep(2)

list = browser.find_element(By.XPATH, "//div[@class='a-popover-wrapper']")
list_text = list.text
print(list_text)

brand_a = list_text.split('\nSelect Brand')
brand_list = brand_a[1].split('\n')
brand = [element.strip() for element in brand_list if element.strip() != ""]
print(brand)

brand=['10.or','Alcatel','Apple']



df = pd.DataFrame(columns=['Mobile_to_buy', 'brand', 'model_name', 'ram', 'Value_in_inr'])

for o in range(len(brand)):
    print(brand[o])
    brand_ss = browser.find_element(By.CSS_SELECTOR, f"span[id='buyBackDropDown1Id'] span[class='a-button-text a-declarative']")
    brand_ss.click()
    # o = 2
    if o == 2:
        brand_s = browser.find_elements(By.XPATH, f"//a[@id='buyBackDropDown1_{o}']")
        brand_s[0].click()

        element_t = browser.find_element(By.XPATH, f"//span[@id='AppleId']//span[@role='button']")
        element_t.click()

        ids = [element.get_attribute("id") for element in browser.find_elements(By.XPATH, "//li[contains(@class, 'a-dropdown-item')]/a")]

        # Remove strings containing "buyBackDropDown"
        model_ids = [id for id in ids if "buyBackDropDown" not in id]

        f_model_ids = [item for item in model_ids if brand[o] in item]

        

        element_t1 = browser.find_element(By.XPATH, f"//div[@id='rightColumn']//div[@class='a-section a-padding-large']")
        element_t1.click()



        for p in f_model_ids:
            element_tt = browser.find_element(By.CSS_SELECTOR, f"span[id='{brand[o]}Id'] span[class='a-dropdown-prompt']")
            element_tt.click()

            element_d = browser.find_elements(By.ID, f"{p}")
            element_d[0].click()

            test = browser.find_element(By.XPATH, "//span[@id='AppleId']//span[@role='button']")
            mdl_n = test.text
            
            print(p)
            
            

            try:
                for x in range(5):
                    mdl = browser.find_elements(By.CSS_SELECTOR, f"span[id='{brand[o]}{mdl_n}Id'] span[role='button']")
                    mdl[0].click()
                    element_r = browser.find_element(By.XPATH, f'//a[contains(@id, "{mdl_n}_{x}")]')
                    element_r.click()
                    ram_d = browser.find_element(By.XPATH, f"//span[@id='{brand[o]}{mdl_n}Id']//span[@role='button']")
                    ram_b = ram_d.text
                    sleep(2)
                    val = browser.find_elements(By.ID, "valueCommensurateUptoDiscountPrice")
                    val_1 = val[0].text
                    print(val_1)
                    mdl_name1 = browser.find_element(By.XPATH, f"//span[@id='AppleId']//span[@role='button']")
                    mdl_name=mdl_name1.text
                    df = pd.concat([df, pd.DataFrame({'Mobile_to_buy': [Naam], 'brand': [brand[o]], 'model_name': mdl_name, 'ram':ram_b, 'Value_in_inr': [val_1]})], ignore_index=True)

            except:
                pass

    else:
        brand_s = browser.find_elements(By.XPATH, f"//a[@id='buyBackDropDown1_{o}']")
        brand_s[0].click()

        element_t = browser.find_element(By.XPATH, f"//span[@id='{brand[o]}Id']//span[@role='button']")
        element_t.click()

        ids = [element.get_attribute("id") for element in browser.find_elements(By.XPATH, "//li[contains(@class, 'a-dropdown-item')]/a")]

        # Remove strings containing "buyBackDropDown"
        model_ids = [id for id in ids if "buyBackDropDown" not in id]

        f_model_ids = [item for item in model_ids if brand[o] in item]

        element_esc = browser.find_element(By.XPATH, "//div[@id='rightColumn']//div[@class='a-section a-padding-large']")
        element_esc.click()
        try:
            for p in f_model_ids:
                
                print(p)
                element_tt = browser.find_element(By.CSS_SELECTOR, f"span[id='{brand[o]}Id'] span[class='a-dropdown-prompt']")
                element_tt.click()
    
                element_d = browser.find_elements(By.ID, f"{p}")
                element_d[0].click()
    
                sleep(3)
                val = browser.find_elements(By.ID, "valueCommensurateUptoDiscountPrice")
                val_1 = val[0].text
                print(val_1)
                mdl_name1 = browser.find_element(By.XPATH, f"//span[@id='{brand[o]}Id']//span[@role='button']")
                mdl_name=mdl_name1.text
                df = pd.concat([df, pd.DataFrame({'Mobile_to_buy': [Naam], 'brand': [brand[o]], 'model_name': mdl_name, 'ram':'NA','Value_in_inr': [val_1]})], ignore_index=True)

        except:
            pass

browser.close()


print(p)


# df = pd.concat([df, pd.DataFrame({'Naam': [Naam], 'brand': [brand[o]], 'f_model_ids': [p], 'val1': [val_1]})], ignore_index=True)

print(df)
df.to_csv('test_apple0804.csv')


# //span[@id='AppleId']//span[@role='button']


# mdl_name1 = browser.find_element(By.XPATH, "//span[@id='AlcatelId']//span[@role='button']")

# mdl_name=mdl_name1.text

# print(mdl_name)
