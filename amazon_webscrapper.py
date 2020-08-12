#!/usr/bin/env python
# coding: utf-8

# ### Importing Libraries

# In[5]:


import requests
from bs4 import BeautifulSoup
#import re
from datetime import datetime,date,time
#from datetime import timedelta
#from urllib.request import urlopen
#import pandas as pd
import json
from os import path
from csv import writer

#Data Loading 
import csv
import mysql.connector


# In[2]:


#Products details which will be fetched from amazon
my_prod = ['firestick','echodot','echo','fire','kindle']


# #### Function to fetch the data from the amazon.com

# In[8]:


def get_stock_price():        
        
    for product in my_prod:
        prod = []
                   
        today = datetime.now()
        today = today.strftime("%Y-%m-%d %H:%M:%S")
             
        #fname = str(date.today()) + ".csv"
        fname = str('amazon_product')+".csv"
                 
        #print(fname)          
                    
        file_exists = path.isfile('../Rhombus/' +fname)
        csv_file = open(fname,'a',newline='\n',encoding='UTF-8')
        
  #Headers to avoid the useragent detection for skipping the amazon.com/robotlist
        headers = {
    'authority': 'scrapeme.live',
    'dnt': '1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'sec-fetch-site': 'none',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-user': '?1',
    'sec-fetch-dest': 'document',
    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
}
        
        #PROXY = proxies = { "https": "https:139.255.17.170:443",}       
            
        prod_tracker_URLS = 'https://www.amazon.com/'+product
        r = requests.get(prod_tracker_URLS, headers=headers)
        #print('R ',r)
        # fetch the url
        page = requests.get(prod_tracker_URLS, headers=headers)       

# create the object that will contain all the info of the url
        soup = BeautifulSoup(page.content, features="lxml")    

#Webscrapping for different fields
        try:
            title = soup.find(id='productTitle').get_text().strip()
            #print(title)
            
        except:
            title= 'N/A'
        
        try:
            price = str(soup.find(id="priceblock_ourprice").get_text().strip())
            #print(price)
        
        except:
            price = str(soup.find(id="priceblock_dealprice").get_text().strip())
            #print(price)
    
        else:            
            price = 'N/A'
            #print(price)
            
        try:
            brand = soup.find(id="bylineInfo").get_text().replace(' ','').split(':')[0]
            #print(brand)
        
        except:
            brand = 'N/A'
            #print(brand)
            
        try:
            availability = soup.find(id='availability').get_text().strip()
            #print(availability)
        except:
            availability=0
            #print(availability)
            
        try:
            usr_review_count = soup.find(id='acrCustomerReviewText').get_text().replace(',','').split(' ')[0]
            #print(usr_review_count)
        
        except:
            user_review_count = 0
            #print('usr_review_count')
            
        try:
            #usr_rating = soup.find(id='reviewsMedley').get_text()
            usr_rating = soup.find(id='customer-ucc-review-stars').get_text().split(' ')[0]
            #print(usr_rating)
            
        except:
            usr_rating = '0.0'
            #print(usr_rating)
        
        tech_descr = soup.find(id="feature-bullets").get_text().replace('\n','')
        #print(tech_descr)
        
        merchant_info = soup.find(id="merchant-info").get_text().strip().split('sold by')[1]
        #print(merchant_info)
        
        delivery_message = soup.find(id='delivery-message').get_text().strip().split('choose')[1]
        print(delivery_message)
        
        asin = r.url.split("/dp/")[1][0:10]
        #print(asin)
        
        pre_owned_sale =  soup.find(class_='preOwnedCrossSell-price').get_text().strip()
        #print(pre_owned_sale)
        
        img_div = soup.find(id="imgTagWrapperId")
        imgs_str = img_div.img.get('data-a-dynamic-image').strip(',')
        imgs_dict = json.loads(imgs_str)
        img_link = list(imgs_dict.keys())[0]
        #print(img_link)
        
        '''image_class = soup.select("li",{"class":'image item itemNo0 maintain-height selected'}).strip('src')[0]
        print(image_class)
        '''
        '''decoded_content = soup.decode()
        asins = re.findall(r'/[^/]+/dp/([^\"?]+)', decoded_content).strip
        print(asins)'''
         
        '''top_comment = soup.find(id="#cr-summarization-attributes-list").get_text
        #print(top_comment)'''
        
       
    #Appending data to the file
    
        prod.append(date.today())
        prod.append(asin)
        prod.append(title)
        prod.append(tech_descr)
        prod.append(brand)
        prod.append(merchant_info)
        prod.append(img_link)
        prod.append(price)
        prod.append(pre_owned_sale)
        prod.append(availability)
        prod.append(usr_review_count)
        prod.append(usr_rating)
        prod.append(delivery_message)
        
    # CSV file with pipe(|) delimeter            
        csv_writer = writer(csv_file,delimiter='|')
        
        #CSV file with header logic creation
        
        if not file_exists:
            
            csv_writer.writerow(['Date','Product ID','Product Name','Description','Brand','Vendor','Product Image','Unit Price','Refurbished Price', 'Availability',
         'Review Count', 'Product Rating','Delivery Message'])
            
            csv_writer.writerow(prod)
        
        else:
            
            csv_writer.writerow(prod)        

        csv_file.close()

print('Process Started at  :',datetime.now())
            
get_stock_price()

print('Data Gather process Completed')

print('Process Completed at:',datetime.now())


# ### Data Loading to My SQL Database

# In[ ]:


cnx = mysql.connector.connect(user='root', password='test',host='35.224.14.68',database='TEST')
print('Database Connection successful')
cursor = cnx.cursor()
csv_data = csv.reader('amazon_product.csv',  delimiter='|')
#for row in csv_data:
    #cursor.execute("INSERT INTO AMAZON_DATA_LOAD ""(ORDER_DATE,PROD_NAME,PROD_DESCR,PRICE,BRAND,AVAILIBILITY,REVIEW_COUNT,RATING) " "VALUES(%s,%s,%s,%s,%s,%s,%s,%s)")
#cursor.execute(LOAD DATA INFILE '2020-07-23.csv' INTO TABLE AMAZON_DATA_LOAD IGNORE 1 LINES FIELDS TERMINATED BY '|')
sqlLoadData = 'LOAD DATA LOCAL INFILE "amazon_product.csv" INTO TABLE AMAZON_DATA_STG '   
sqlLoadData += 'FIELDS TERMINATED BY "|" LINES TERMINATED BY "\n"'     
sqlLoadData += 'IGNORE 1 LINES \n' 
cursor.execute(sqlLoadData)

#Final table insert statement
cursor.execute('insert into VENDOR (VENDOR_NM, PRODUCT_ID, VENDOR_STOCK, DELIVERY_MSG) select VENDOR, PROD_ID, AVAILIBILITY, DELIVERY_MESSAGE from AMAZON_DATA_STG')
cursor.execute('insert into PRODUCTS (PRODUCT_ID, PRODUCT_NM, PRODUCT_DESC, BRAND, PRODUCT_IMG) select PROD_ID, PROD_NAME, PROD_DESCR, BRAND, PROD_IMG from AMAZON_DATA_STG')
cursor.execute('insert into REVIEWS (PRODUCT_ID, REVIEW_COUNT, RATINGS) select PROD_ID, REVIEW_COUNT, PRODUCT_RATING from AMAZON_DATA_STG')
cursor.execute('insert into PRICING (PRODUCT_ID, EFF_DT, EXP_DT, UT_PRICE, REFURB_PRICE) select PROD_ID, ORDER_DATE, "9999-12-31" AS EXP_DT, UNIT_PRICE, REFURBISHED_PRICE from AMAZON_DATA_STG')


cnx.commit()
print('Data Inserted to Stage Table Successful')
#close the connection to the database.
cursor.close()


# References:
# 
# https://www.scrapehero.com/how-to-prevent-getting-blacklisted-while-scraping/
# 
# https://www.jeremyperkins.dev/python/2020/01/08/Python-Scraper-Problems.html
# 
# https://stackoverflow.com/questions/16129652/accessing-json-elements
# 
# https://stackoverflow.com/questions/10154633/load-csv-data-into-mysql-in-python
# 

# In[ ]:





# In[ ]:




