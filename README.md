This repo contains my ongoing analytics of Ontario's top-10 grocery stores' flyers, to get myself the best weekly deals! 
<br><br>
Tech stack: ```dbt``` ```GCP BigQuery``` ```GitHub Actions``` ```Selenium``` ```BeautifulSoup```<br>
Data modeling & transformation scripts/jobs/tests on ```dbt```, can be found [here](https://github.com/shithi30/DBT_BigQuery_Analytics/tree/main). Latest analytics is served [here](https://docs.google.com/spreadsheets/d/1Fokcum9d__mAxw8PEN_djL34UL9l5Uq8j5LCPwjAE9Y/edit?gid=0#gid=0).

### Modeling & Transformation - dbt
<p align="center">
  <img width="1100" alt="c5" src="https://github.com/user-attachments/assets/f187187e-de3a-491b-a804-fad60bc2fc7e"><br>
</p>

### Warehoused Results - BigQuery
<p align="center">
  <img width="1000" alt="c5" src="https://github.com/user-attachments/assets/a7b10571-b2e6-4162-9170-2d784a4337ec"><br>
</p>

### Data Lineage & Documentation - dbt
<p align="center">
  <img width="950" alt="c5" src="https://github.com/user-attachments/assets/37319f4c-a24a-45c0-8a6f-30fb637932b6"><br>
</p>

### Flyers' Data - Caller Function
```Python
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
```  
</p>

### Output Notifiers: Top-03/Category
<p align="center">
  <img width="1020" alt="c5" src="https://github.com/user-attachments/assets/9e4df3c9-ad7d-4ee9-9fe8-136fce1e8302"><br>
</p>

<strong>Note</strong>: All scraping activities have been performed in compliance with robots.txt regulations.
