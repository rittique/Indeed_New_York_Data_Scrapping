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

driver = uc.Chrome()

cities = ['New York, NY', 'Houston, TX']
position = 'Restaurant'
no_of_pages = 20

def main_scrapper(cities, position, no_of_pages):
    total_jobs = 0
    job_data = []
    for city in cities:
        print(f"Scrapping for {city}, position/skill: {position}")
        for page in range(0, no_of_pages*10, 10):
            driver.get(f'https://www.indeed.com/jobs?q={position}&l={city}&sort=date&start={page}')
            time.sleep(random.uniform(8.5, 10.9))

            try:
                close = driver.find_element(By.XPATH, '//div[@class="icl-CloseButton icl-Modal-close"]')
                close.click()
            except:
                pass

            soup = BeautifulSoup(driver.page_source, features="html.parser")
            divs = soup.find_all('div', 'slider_item css-kyg8or eu4oa1w0')
            
            total_jobs += len(divs)
            print(f"Total Jobs Scrapped: {total_jobs}")
            for item in divs:

                # Getting the Job Title
                title = item.find('a').text.strip()
                type_of_job = ''
                salary = ''
                schedule = ''
                # Getting the job type
                try:
                    all_metadata = item.find('div', 'heading6 tapItem-gutter metadataContainer noJEMChips salaryOnly')
                    rows =[i.get_text(strip=True) for i  in all_metadata.find_all('div', 'metadata') if i.get_text(strip=True)!=""]
                    for element in rows:
                        if '$' in element:
                            salary = element.strip()
                            rows.remove(element)
                        else:
                            salary = ''
                    for element in rows:
                        if 'time' in element:
                            if '+1' in element:
                                type_of_job = 'Full-time, Part-time'
                            else:
                                type_of_job = element
                            rows.remove(element)

                        else:
                            type_of_job = ''
                    if len(rows) > 0:
                        schedule = rows[0]

                    else:
                        schedule = ''
                except:
                    try:
                        salary = item.find('div', 'attribute_snippet').text.strip()
                        type_of_job = ''
                    except:
                        salary = ''

                try:
                    company = item.find('span', 'companyName').text.strip()
                except:
                    company = ''

                #Getting the Location
                try:
                    location = item.find('div', 'companyLocation').text.strip()
                except:
                    location = ''

                #Getting the link
                try:
                    link = 'https://www.indeed.com/' + item.h2.a.get('href')
                except:
                    link = ''

                # Date Posted
                try:
                    date_posted = item.find('span', 'date').text.strip()
                except:
                    date_posted = ''

                # Current Date
                try: 
                    current_date = datetime.today().strftime('%y-%m-%d')
                except:
                    current_date = ''

                try:
                    description  = item.find('div', 'job-snippet').text.strip().replace('\n', ' ')
                except:
                    description = ''

                data = {
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
connection = MongoClient("mongodb://localhost:27017")
db = connection["IndeedJobPostDB"]
print("Connected to MongoDB successfully.")

# inserting data into db
print("Inserting data into database.")
data = df.to_dict(orient="records")
db.Job_Posts.insert_many(data, ordered=False)

print("Data Insertion Complete!")
print("Scrapping Complete.")

driver.quit()
