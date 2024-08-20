#!/usr/bin/env python
# coding: utf-8

## import
from selenium import webdriver
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import re
import pandas as pd
import duckdb
import time
import fuckit
import os
import json
from google.cloud import bigquery
from google.oauth2 import service_account

## window
driver = webdriver.Chrome()
driver.implicitly_wait(20)
driver.maximize_window()

## parse
def parse_flyer(site, offers, url):
    
    # item
    flyer_df = pd.DataFrame(columns = ["flyer_item", "sku", "offer", "discount_spend", "offer_price", "offer_price_limit_unit_weight", "platform", "url"])
    for offer in offers:  
        flyer_item = offer
    
        # seperate
        offer = offer[:-22].split(", ")
        sku = ", ".join(offer[0:-2])
        discount_offer = offer[-2]
        offer_price_info = offer[-1]

        # price + upto/unit/weight
        pattern = re.compile("\$\s*[0-9]+\.*[0-9]*")
        vals = pattern.findall(offer_price_info)
        # price
        try: price = vals[0][1:]
        except: price = None
        # upto/unit/weight
        try: offer_price_upto = vals[1][1:]
        except: offer_price_upto = None

        # discount - dollar - front
        pattern = re.compile("\$\s*[0-9]+\.*[0-9]*")
        try: discount = pattern.findall(discount_offer)[-1][1:]
        except: discount = None
        # discount - dollar - back
        if discount is None:
            pattern = re.compile("[0-9]+\.*[0-9]*\s*\$")
            try: discount = pattern.findall(discount_offer)[-1][:-1]
            except: discount = None
        # discount - cent
        if discount is None:
            pattern = re.compile("[0-9]+\.*[0-9]*\s*¢")
            try: discount = str(float(pattern.findall(discount_offer)[-1][:-1]) / 100)
            except: discount = None
        # discount - percent
        if discount is None:
            pattern = re.compile("[0-9]+\.*[0-9]*\s*%")
            try: discount = str((float(price)*100) / (100 - float(pattern.findall(discount_offer)[-1][:-1])) - float(price))
            except: discount = None

        # append
        flyer_df = pd.concat([pd.DataFrame([[flyer_item, sku, discount_offer, discount, price, offer_price_upto, site, url]], columns = flyer_df.columns), flyer_df], ignore_index = True)
    
    # refine
    qry = '''
    select 
        flyer_item, sku, offer, 
        discount_spend::numeric discount_spend, offer_price::numeric offer_price, discount_spend::numeric+offer_price::numeric regular_price, offer_price_limit_unit_weight::numeric offer_price_limit_unit_weight, 
        platform, url, strftime(now(), '%Y-%m-%d, %I:%M %p') report_time
    from flyer_df
    '''
    flyer_df = duckdb.query(qry).df()

    # return
    print(site + " items: " + str(flyer_df.shape[0]))
    return flyer_df

## scrape
def scrape_flyer(site, url, element):

    # iframe
    driver.get(url)
    driver.switch_to.frame(driver.find_element(By.XPATH, ".//iframe[@title='Main Panel']"))
    
    # soup
    time.sleep(3)
    soup = BeautifulSoup(driver.page_source, "html.parser").find("sfml-linear-layout").find_all(element)
    
    # data
    offers = []
    for s in soup:
        try: offer = s["aria-label"]
        except: continue
        if offer not in offers: offers.append(offer)
    
    # tabular
    flyer_df = parse_flyer(site, offers, url)
    return flyer_df
    
## caller
@fuckit
def scrape_call():

    # call
    flyer_df = pd.DataFrame()
    flyer_df = pd.concat([flyer_df, scrape_flyer("NoFrills", "https://www.nofrills.ca/print-flyer?navid=flyout-L2-Flyer", "button")], ignore_index = True)
    flyer_df = pd.concat([flyer_df, scrape_flyer("Sobeys", "https://www.sobeys.com/en/flyer", "button")], ignore_index = True)
    flyer_df = pd.concat([flyer_df, scrape_flyer("Freshco", "https://freshco.com/flyer", "button")], ignore_index = True)
    flyer_df = pd.concat([flyer_df, scrape_flyer("ThriftyFoods", "https://www.thriftyfoods.com/weekly-flyer", "button")], ignore_index = True)
    flyer_df = pd.concat([flyer_df, scrape_flyer("Foodland", "https://foodland.ca/flyer", "button")], ignore_index = True)
    flyer_df = pd.concat([flyer_df, scrape_flyer("Safeway", "https://www.safeway.ca/flyer", "button")], ignore_index = True)
    flyer_df = pd.concat([flyer_df, scrape_flyer("IGA", "https://www.iga.net/en/flyer", "a")], ignore_index = True)
    flyer_df = pd.concat([flyer_df, scrape_flyer("RealCanadian", "https://www.realcanadiansuperstore.ca/print-flyer?navid=flyout-L2-Flyer", "button")], ignore_index = True)
    flyer_df = pd.concat([flyer_df, scrape_flyer("Loblaws", "https://www.loblaws.ca/print-flyer?navid=flyout-L2-Flyer", "button")], ignore_index = True)
    flyer_df = pd.concat([flyer_df, scrape_flyer("Zehrs", "https://www.zehrs.ca/print-flyer?navid=flyout-L2-Flyer", "a")], ignore_index = True)
    
    # error
    to_scraped = set(["NoFrills", "Sobeys", "Freshco", "ThriftyFoods", "Foodland", "Safeway", "IGA", "RealCanadian", "Loblaws", "Zehrs"])
    is_scraped = set(duckdb.query('''select platform from flyer_df''').df()["platform"].tolist())
    err_scrape = to_scraped - is_scraped
    if len(err_scrape) > 0: print("\nGrocery stores threw error: " + str(err_scrape))

    # return
    return flyer_df

## results
flyer_df = scrape_call()
print("\nTotal flyer items: " + str(flyer_df.shape[0]))
driver.close()

# BigQuery
credentials = service_account.Credentials.from_service_account_info(json.loads(os.getenv("BIGQUERY_KEYS_JSON")))
client = bigquery.Client(credentials = credentials, project = credentials.project_id)
dataset, table = "dbt_smaitra", "landing_grocery_flyer_items"

# truncate
qry = "truncate table " + dataset + "." + table 
res = client.query(qry).result()

# ID
qry = "select row_number() over() id, * from flyer_df"
load_df = duckdb.query(qry).df()

# load
tbl = client.dataset(dataset).table(table)
job = client.load_table_from_dataframe(load_df, tbl)

# stats
job.result()
print("Loaded " + str(job.output_rows) + " rows into " + dataset + "." + table)
