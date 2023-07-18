import pandas as pd
import numpy as np
import random
import time
import pymongo
from pymongo import MongoClient
from parsel import Selector
from datetime import datetime
from time import sleep
from selenium import webdriver
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
import itertools

def driverConfig():
    chrome_options = uc.ChromeOptions()
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--enable-javascript')
    chrome_options.add_argument('--disable-gpu')
    user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Safari/605.1.15'
    chrome_options.add_argument('User-Agent={0}'.format(user_agent))
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', True)
    
    driver = uc.Chrome(executable_path='../google-chrome-stable_current_amd64.deb',chrome_options=chrome_options,service_args=['--quiet'])

    return driver

drivers_dict={}   

driver = driverConfig()
driver.implicitly_wait(6.5)
cities = ['New York, NY', 'Houston, TX']
position = 'Restaurant'
no_of_pages = 20

def uniqueJobID(city, page, count):
    current_time = datetime.now()
    if "TX" in city:
        jobID = "TX"+str(current_time.day)+str(f"{current_time.month:02}")+str(current_time.year)+str(current_time.strftime('%A')).upper()[:3]+str(page)+f"{count:02}"
    if "NY" in city:
        jobID = "NY"+str(current_time.day)+str(f"{current_time.month:02}")+str(current_time.year)+str(current_time.strftime('%A')).upper()[:3]+str(page)+f"{count:02}"
    
    return str(jobID)

def main_scrapper(cities, position, no_of_pages):
    total_jobs = 0
    job_data = []
    for city in cities:
        print(f"Scrapping for {city}, position/skill: {position}")
        for page in range(0, no_of_pages*10, 10):
            driver.get(f'https://www.indeed.com/jobs?q={position}&l={city}&fromage=1&sort=date&start={page}')
            time.sleep(random.uniform(8.5, 10.9))

            try:
                close = driver.find_element(By.XPATH, '//div[@class="icl-CloseButton icl-Modal-close"]')
                close.click()
            except:
                pass

            soup = BeautifulSoup(driver.page_source, features="html.parser")
            divs = soup.find_all('div', 'slider_item css-kyg8or eu4oa1w0')
            count = 0
            total_jobs += len(divs)
            print(f"Total Jobs Scrapped: {total_jobs}")
            for item in divs:
                count += 1
                # Getting the Job Title
                title = item.find('a').text.strip()
                type_of_job = None
                salary = None
                schedule = None
                # Getting the job type
                try:
                    all_metadata = item.find('div', 'heading6 tapItem-gutter metadataContainer noJEMChips salaryOnly')
                    rows =[i.get_text(strip=True) for i  in all_metadata.find_all('div', 'metadata') if i.get_text(strip=True)!=""]
                    for element in rows:
                        if '$' in element:
                            salary = element.strip()
                            rows.remove(element)
                        else:
                            salary = None
                    for element in rows:
                        if 'time' in element:
                            if '+1' in element:
                                type_of_job = 'Full-time, Part-time'
                            else:
                                type_of_job = element
                            rows.remove(element)

                        else:
                            type_of_job = None
                    if len(rows) > 0:
                        schedule = rows[0]

                    else:
                        schedule = None
                except:
                    try:
                        salary = item.find('div', 'attribute_snippet').text.strip()
                        type_of_job = None
                    except:
                        salary = None

                try:
                    company = item.find('span', 'companyName').text.strip()
                except:
                    company = None

                #Getting the Location
                try:
                    location = item.find('div', 'companyLocation').text.strip()
                except:
                    location = None

                #Getting the link
                try:
                    link = 'https://www.indeed.com/' + item.h2.a.get('href')
                except:
                    link = None

                # Date Posted
                try:
                    date_posted = item.find('span', 'date').text.strip()
                except:
                    date_posted = None

                # Current Date
                try: 
                    current_date = datetime.today().strftime('%y-%m-%d')
                except:
                    current_date = None

                try:
                    description  = item.find('div', 'job-snippet').text.strip().replace('\n', ' ')
                except:
                    description = None
                    
                try:
                    jobID = uniqueJobID(city, page, count)
                except:
                    jobID = None
                print(jobID)

                data = {
                    'JobID' : jobID,
                    'Title' : title,
                    'Job_Type' : type_of_job,
                    'Company' : company,
                    'Salary' : salary,
                    'Schedule': schedule,
                    'Location' : location,
                    'Date_Posted' : date_posted.removeprefix('Posted'),
                    'Current_Date' : current_date,
                    'Summary' : description,
                    'Apply_link' : link
                }
                job_data.append(data)
                print('[*] Saving')


    df = pd.DataFrame(job_data)
    return df


print("Starting Scrapper..")
df = main_scrapper(cities, position, no_of_pages)

print("Scrpping Complete!")

# Connecting ot db
print("Connecting to MongoDB..")
connection = MongoClient("mongodb+srv://doadmin:g90l61F8EnQJ3m45@db-mongodb-blr1-90175-4f55a9f9.mongo.ondigitalocean.com/admin?tls=true&authSource=admin&replicaSet=db-mongodb-blr1-90175")
db = connection["JobsDB"]
print("Connected to MongoDB successfully.")

# inserting data into db
print("Inserting data into database.")
data = df.to_dict(orient="records")
db.JobDetails.insert_many(data, ordered=False)

print("Data Insertion Complete!")
print("Scrapping Complete.")

driver.quit()
