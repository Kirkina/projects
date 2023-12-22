#!/usr/bin/env python
# coding: utf-8

# In[5]:


from selenium import webdriver
from tqdm import tqdm_notebook as tqdmn
import pandas as pd
import folium
from selenium.webdriver.common.by import By


# In[6]:


company = pd.read_excel('адреса.xlsx')


# In[7]:


company


# In[8]:


company.columns=['address']
company.address = company.address.str.replace('/', ' ')
company['url'] = ['https://www.google.com/maps/search/' + i for i in company['address']]
url_with_coordinates = []


# In[9]:


option = webdriver.ChromeOptions()
prefs = {'profile.default_content_setting_values': {'images':2, 'javascript':2}}
option.add_experimental_option('prefs', prefs)


# In[10]:


driver = webdriver.Chrome("C:\\chromedriver.exe", options=option)

for url in tqdmn(company.url, leave=False):
    driver.get(url)
    url_with_coordinates.append(driver.find_element(By.CSS_SELECTOR, 'meta[itemprop=image]').get_attribute('content'))
    
driver.close()


# In[ ]:


driver.close()
C:\Users\Aleksandr\AppData\Local\Temp\ipykernel_21540\2119314296.py:7: DeprecationWarning: executable_path has been deprecated, please pass in a Service object
  driver = webdriver.Chrome("C:\\chromedriver.exe", options=option)
C:\Users\Aleksandr\AppData\Local\Temp\ipykernel_21540\2119314296.py:9: TqdmDeprecationWarning: This function will be removed in tqdm==5.0.0
Please use `tqdm.notebook.tqdm` instead of `tqdm.tqdm_notebook`
  for url in tqdmn(company.url, leave=False):
Ввод [8]:
x
company['url_with_coordinates'] = url_with_coordinates
Ввод [9]:
company['lat'] = [ url.split('?center=')[1].split('&zoom=')[0].split('%2C')[0] for url in company['url_with_coordinates'] ]
company['long'] = [url.split('?center=')[1].split('&zoom=')[0].split('%2C')[1] for url in company['url_with_coordinates'] ]
Ввод [13]:
from IPython.display import IFrame
​
company_map = folium.Map([56.9972, 40.9714], tiles='CartoDB positron' )
   
for lat, long, name, full_address, lpr, phone in zip(
    company.lat
    ,company.long
    ,company.name
    ,company.address
    ,company.lpr
    ,company.phone):
    folium.Marker( [lat, long] 
                   ,icon=folium.CustomIcon(
                       icon_image='https://i.imgur.com/CYx04oC.png'
                       ,icon_size=(10,10) )
                  ,popup=name+'\n\n' +full_address+'\n\n' +lpr+'\n\n' +phone).add_to(company_map)
​
company_map.save('company_map.html')
IFrame(src='company_map.html', width='100%', height=800)


# In[ ]:





# In[ ]:




