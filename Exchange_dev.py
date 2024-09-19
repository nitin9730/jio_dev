#!/usr/bin/env python3
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from time import sleep
import pandas as pd
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC



chrome_options = Options()
# chrome_options.add_argument('--headless')
# chrome_options.add_argument('--incognito')

browser = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=chrome_options)
browser.get('https://www.amazon.in/gp/bestsellers/electronics/1389432031/ref=zg_bs_pg_1?ie=UTF8&pg=1')

loc = browser.find_elements(By.ID, "glow-ingress-line1")
loc[0].click()
sleep(2)


# Find the input field using XPath
input_field = WebDriverWait(browser, 5).until(
    EC.presence_of_element_located((By.ID, "GLUXZipUpdateInput"))
)


input_field.send_keys("400701")

input_field.send_keys(Keys.ENTER)


HREF2 = [
    # 'https://www.amazon.in/Apple-iPhone-15-128-GB/dp/B0CHX1W1XY/ref=sr_1_3?crid=16E3GIF446QA1&dib=eyJ2IjoiMSJ9.8-aKrERwPzdGyJWfWOa56FXJAJ42bLCV9nC8Be7iuRV6YYmEpHxp1ek5oho5l2JwbaO0qNKaICmZVnzdm7aTJtIgY9T4G2abAaO_t19eQtk0rH_ajlPYkBWTg6r1aloHsb9n9_9oZ-VPOQ7_r6qDg4_9wlUo9bpGkBzaJ7UWom_kjUyVS6KQR_7p82CYaweOCSMOIlQT2t8leg-nLXjCYSWW_JY_4_ve2z4gUAXNbSc.119B04bKbZmUJn18oKqHz03J6fQN1ORMlHjs1TaMRkg&dib_tag=se&keywords=Apple+iPhone+15+%28128+GB%29+-+Black&qid=1712657073&sprefix=apple+iphone+15+128+gb+-+black%2Caps%2C195&sr=8-3'
    #  ,
    # 'https://www.amazon.in/Samsung-Storage-Expandable-MediaTek-Dimensity/dp/B0CP7W9ZDZ/ref=sr_1_1?crid=3ASQ3N4Q20HQ2&dib=eyJ2IjoiMSJ9.x5KU9cG9-9ZMwrrKu6L2ezjRrfyKQwxZB7sLdqisaPisEIQkxBkBMQmgUIPcNqKC3qjumDEvafZ0Os6jKjFzUNhE0OlXBaOu_As3iGPhvNL8DtMaCiIpqaKeK91o6-ykGucmos-FwRxowx6u8UXx2D6909jmQwHtQEPTWqxY068KQU3YweU2W8cE_zxM81htvwt5s3_nwJ01lcYPdracw9zwazm57kh7K_vhjwazcbw.p5tLEWVvD3sF7qtI-4cMG1WKy7oGe1kupsDez1X3RyA&dib_tag=se&keywords=Samsung+Galaxy+A53+5G+%286+GB%2F128+GB%29&qid=1726732630&sprefix=samsung+galaxy+a53+5g+6+gb%2F128+gb+%2Caps%2C412&sr=8-1'
    # ,
    
    'https://www.amazon.in/realme-Metallica-Gold-64GB-Storage/dp/B0BBQZP6HM/ref=sr_1_1?crid=IS3WVCWQ9ME1&dib=eyJ2IjoiMSJ9.og8PxUYc6BNoiqmW4l0BKW4TqgDujZDB0v5ZhsAKqrPxpbWBXFVR1zB4te-8YIG1x6xlDN8ogz8hWUMTIiR1AxXPuny-49gRd-NxjVYBtj4sHV2F_qc-ZkWvHmL2zmPxF5OM0zTRNRxXlIDgxiyCQUKuyNoA1g8tbNlyFrX0EkIrOWcybbvUbwOWsKD1fG80cF7k6JFPgKWdT77oFIVkHHh2CbMszotteielbnCjMvo.deIR4QW99bQtLyfyxbaeSXFcijgKeXeHSOdgqAU6JM8&dib_tag=se&keywords=Redmi+9i+%284+GB%2F64+GB%29&qid=1726732755&sprefix=redmi+9i+4+gb%2F64+gb+%2Caps%2C180&sr=8-1'
    ]

for l in range(len(HREF2)):
    sleep(3)
    browser.get(HREF2[l])
    print(HREF2[l])
    Names = browser.find_element(By.XPATH, "//span[@id='productTitle']")
    Naam = Names.text
    
    print(Naam)
    
    desc02 = browser.find_elements(By.XPATH, "//i[@class='a-icon a-accordion-radio a-icon-radio-inactive']")
    desc02[0].click()
    sleep(2)
    
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
    
    Apple_p = brand.index("Apple")
    
    # brand=['10.or','Alcatel','Apple']
    
    df = pd.DataFrame(columns=['Mobile_to_buy', 'brand', 'model_name', 'ram', 'Value_in_inr'])
    
    for o in range(len(brand)):
        print(brand[o])
        brand_ss = browser.find_element(By.CSS_SELECTOR, f"span[id='buyBackDropDown1Id'] span[class='a-button-text a-declarative']")
        brand_ss.click()
        # o = 2
        if o == Apple_p:
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
                
                sleep(2)
                print(p)
                
                
    
                try:
                    for x in range(3):
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
    
                    sleep(2)                
    
            except:
                pass
    
    print(p)
    
    
    # df = pd.concat([df, pd.DataFrame({'Naam': [Naam], 'brand': [brand[o]], 'f_model_ids': [p], 'val1': [val_1]})], ignore_index=True)
    
    
    df.to_csv(f'test_exchange0904_2_{Naam}.csv')
    
    
browser.close()
