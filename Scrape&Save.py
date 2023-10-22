from bs4 import BeautifulSoup
import requests
from pymongo.mongo_client import MongoClient

uri = "mongodb+srv://VenkatSagi:mongodb.2004@cluster0.ijkgw8w.mongodb.net/?retryWrites=true&w=majority"

# Create a new client and connect to the server
client = MongoClient(uri)

# Access the 'mydatabase' database
db = client['WorkerBenefitsDB']

# Access the 'mycollection' collection
collection = db['WorkerBenefitsCo']

'''Data Collection & Scraping COMPANIES from Website'''

# Request in json format
data = requests.get('https://www.levels.fyi/js/salaryData.json').json()

# Extract company names from the json
companiesList = list(set([item['company'] for item in data]))

'''Data Collection & Scraping BENEFITS from Website'''

# Stores company data for all companies in the companiesList
companiesData = {}

# Loop to iterate and add info for all company data
for company in companiesList:
    url_temp = f'https://www.levels.fyi/companies/{company}/benefits'
    page = requests.get(url_temp)
    soup = BeautifulSoup(page.text, 'html.parser')  # specify parser to avoid warning

    # Check if the page exists
    if soup.title.text != 'Levels.fyi - Compare career levels across companies':
        table = soup.find_all('table', class_='table-modal_table__1D5g2')
        categories = soup.find_all('td')

        categories_title = [title.text for title in categories]
        categories_benefit = []
        categories_description = []
        for i in range(len(categories_title)):
            if i % 2 == 0:
                categories_benefit.append(categories_title[i])
            else:
                categories_description.append(categories_title[i])

        # Checks if list is empty (Removes bad Data)
        if len(categories_benefit) == 0:
            continue

        # Loop to get and add benefits data into a list
        benefits_dict = {categories_benefit[i]: categories_description[i] for i in range(len(categories_benefit))}
        companiesData[company] = benefits_dict

# Convert companiesData dictionary to a list of dictionaries
companies_list = [{'_id': company, **benefits} for company, benefits in companiesData.items()]

# Insert companies into MongoDB
collection.insert_many(companies_list)

print("FINISHED!!!")