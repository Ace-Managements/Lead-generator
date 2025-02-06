import time
import re
import sqlite3
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import logging
import os
from concurrent.futures import ThreadPoolExecutor
import googlemaps
from openpyxl.styles import PatternFill, Font
from openpyxl.formatting.rule import CellIsRule

class BusinessLeadFinder:
    def __init__(self):
        self.setup_logging()
        self.setup_driver_options()
        self.setup_database()
        self.setup_gmaps()
        self.collected_businesses = set()
        self.current_leads = 0
        self.target_leads = 0
        self.leads = []

    def setup_gmaps(self):
        self.gmaps = googlemaps.Client(key='AIzaSyBwEery-leiGjpvTJdWmPRJjGkM5Mf1bOw')

    def setup_logging(self):
        os.makedirs('leads', exist_ok=True)
        os.makedirs('logs', exist_ok=True)
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[logging.FileHandler('logs/lead_finder.log'), logging.StreamHandler()]
        )
        self.logger = logging.getLogger('LeadFinder')

    def setup_driver_options(self):
        self.chrome_options = Options()
        self.chrome_options.add_argument('--headless=new')
        self.chrome_options.add_argument('--no-sandbox')
        self.chrome_options.add_argument('--disable-dev-shm-usage')
        self.chrome_options.add_argument('--window-size=1920,1080')
        self.chrome_options.add_argument('--disable-notifications')
        self.chrome_options.add_argument('--incognito')
        self.chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])

    def setup_database(self):
        self.conn = sqlite3.connect('leads_database.db')
        cursor = self.conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS leads (
                business_name TEXT,
                phone TEXT,
                has_website TEXT,
                website_url TEXT,
                google_maps_url TEXT,
                business_hours TEXT,
                rating REAL,
                review_count INTEGER,
                called TEXT,
                deal_status TEXT,
                notes TEXT,
                city TEXT,
                UNIQUE(business_name, city)
            )
        ''')
        self.conn.commit()

    def initialize_driver(self):
        try:
            service = ChromeService(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=self.chrome_options)
            driver.set_page_load_timeout(30)
            return driver
        except Exception as e:
            self.logger.error(f"Driver initialization error: {str(e)}")
            return None

    def extract_business_info(self, element, driver):
        try:
            name = element.find_element(By.CSS_SELECTOR, "div.fontHeadlineSmall").text.strip()
            element.click()
            time.sleep(1)

            info = {
                'Business Name': name,
                'Phone': '',
                'Has Website': 'No',
                'Website URL': '',
                'Google Maps URL': driver.current_url,
                'Business Hours': 'Hours not available',
                'Rating': None,
                'Review Count': 0,
                'Called': 'No',
                'Deal Status': 'Not Contacted',
                'Notes': ''
            }

            try:
                rating_elem = WebDriverWait(driver, 3).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "span.MW4etd")))
                info['Rating'] = float(rating_elem.text.strip())

                reviews = driver.find_element(By.CSS_SELECTOR, "span.UY7F9").text
                if '(' in reviews:
                    count = re.search(r'\((\d+)\)', reviews)
                    if count:
                        info['Review Count'] = int(count.group(1))
            except:
                pass

            try:
                website_elem = driver.find_element(By.CSS_SELECTOR, "a[data-tooltip='Open website']")
                info['Has Website'] = 'Yes'
                info['Website URL'] = website_elem.get_attribute('href')
            except:
                pass

            try:
                phone_elems = driver.find_elements(By.CSS_SELECTOR, 
                    "button[data-tooltip*='phone'], div[aria-label*='Phone']")
                for elem in phone_elems:
                    text = elem.get_attribute("aria-label") or elem.text
                    if match := re.search(r'\(?([0-9]{3})\)?[-.\s]?([0-9]{3})[-.\s]?([0-9]{4})', text):
                        info['Phone'] = f"({match.group(1)}) {match.group(2)}-{match.group(3)}"
                        break
            except:
                pass

            try:
                hours_button = WebDriverWait(driver, 3).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "button[aria-label*='hours']")))
                driver.execute_script("arguments[0].click();", hours_button)
                time.sleep(1)
                hours = driver.find_elements(By.CSS_SELECTOR, "table tr")
                if hours:
                    info['Business Hours'] = "\n".join([h.text for h in hours if h.text.strip()])
            except:
                pass

            return info

        except Exception as e:
            self.logger.error(f"Extraction error: {str(e)}")
            return None

    def process_search_query(self, driver, query, niche, city):
        url = f"https://www.google.com/maps/search/{query.replace(' ', '+')}"
        driver.get(url)
        time.sleep(2)

        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.Nv2PK")))
        except TimeoutException:
            return

        last_height = 0
        scroll_attempts = 0
        processed_count = 0

        while scroll_attempts < 20 and self.current_leads < self.target_leads:
            results = driver.find_elements(By.CSS_SELECTOR, "div.Nv2PK")
            
            for result in results[processed_count:]:
                if self.current_leads >= self.target_leads:
                    return
                    
                info = self.extract_business_info(result, driver)
                if info:
                    info['city'] = city
                    self.leads.append(info)
                    self.current_leads += 1
                    print(f"\rCollected leads: {self.current_leads}/{self.target_leads}", end="", flush=True)
                
                processed_count += 1

            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1)

            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                scroll_attempts += 1
            else:
                scroll_attempts = 0
                last_height = new_height

    def search_location(self, niche, city, province):
        driver = None
        try:
            driver = self.initialize_driver()
            if not driver:
                return

            search_queries = [
                f"{niche} in {city}, {province}",
                f"local {niche} {city}",
                f"{niche} services {city}",
                f"best {niche} {city}",
                f"residential {niche} {city}"
            ]

            for query in search_queries:
                if self.current_leads >= self.target_leads:
                    break
                self.process_search_query(driver, query, niche, city)

        finally:
            if driver:
                driver.quit()

    def save_to_excel(self, leads, niche, city):
        if not leads:
            return None
            
        timestamp = datetime.now().strftime('%Y%m%d_%H%M')
        filename = os.path.abspath(f'leads/leads_{niche}_{city}_{timestamp}.xlsx')
        
        df = pd.DataFrame(leads)
        
        columns = [
            'Business Name',
            'Phone',
            'Has Website',
            'Website URL',
            'Google Maps URL',
            'Business Hours',
            'Rating',
            'Review Count',
            'Called',
            'Deal Status',
            'Notes'
        ]
        
        for col in columns:
            if col not in df.columns:
                df[col] = ''
                
        df = df[columns]
        
        writer = pd.ExcelWriter(filename, engine='openpyxl')
        df.to_excel(writer, index=False, sheet_name='Leads')
        
        worksheet = writer.sheets['Leads']
        
        for idx, col in enumerate(df.columns):
            max_length = max(
                df[col].astype(str).apply(len).max(),
                len(col)
            ) + 2
            worksheet.column_dimensions[chr(65 + idx)].width = min(max_length, 50)
        
        writer.close()
        print(f"\nSaved {len(leads)} leads to: {filename}")
        return filename

    def find_leads(self, niche, target_leads=50):
        city = input("Enter city name: ").strip()
        province = input("Enter province/state: ").strip()
        
        self.current_leads = 0
        self.target_leads = target_leads
        self.leads = []
        
        locations = self.get_expanded_locations(city, province)
        print(f"\nSearching for {target_leads} {niche} businesses...")
        
        for search_city, search_province in locations:
            if self.current_leads >= target_leads:
                break
            self.search_location(niche, search_city, search_province)
        
        if self.leads:
            self.save_to_excel(self.leads, niche, city)
        
        return self.leads

    def get_expanded_locations(self, city, province):
        nearby_locations = {
            'Toronto': ['North York', 'Scarborough', 'Etobicoke'],
            'Vancouver': ['Burnaby', 'Richmond', 'Surrey'],
            'Montreal': ['Laval', 'Longueuil', 'Brossard'],
            'Calgary': ['Airdrie', 'Cochrane', 'Chestermere'],
            'Mississauga': ['Brampton', 'Oakville', 'Milton']
        }
        locations = [(city, province)]
        if city in nearby_locations:
            for nearby_city in nearby_locations[city][:2]:
                locations.append((nearby_city, province))
        return locations

def main():
    print("Business Lead Finder v3.0")
    print("-" * 30)
    
    finder = BusinessLeadFinder()
    
    while True:
        try:
            niche = input("\nEnter business type (or 'quit' to exit): ").strip()
            if niche.lower() == 'quit':
                break
                
            target = input("Enter number of leads to find: ").strip()
            if not target.isdigit():
                print("Please enter a valid number")
                continue
                
            target_leads = int(target)
            leads = finder.find_leads(niche, target_leads)
            
            if leads:
                print(f"\nFound {len(leads)} leads")
                print("\nSample leads:")
                for idx, lead in enumerate(leads[:5], 1):
                    print(f"\n{idx}. {lead['Business Name']}")
                    print(f"   Phone: {lead['Phone']}")
                    print(f"   Rating: {lead['Rating']} ({lead['Review Count']} reviews)")
                    print(f"   Location: {lead['city']}")
                    if lead['Website URL']:
                        print(f"   Website: {lead['Website URL']}")
            else:
                print("\nNo qualified leads found")
                
        except KeyboardInterrupt:
            print("\nOperation cancelled")
            break
        except Exception as e:
            print(f"Error: {str(e)}")
            continue
            
    print("\nThank you for using Business Lead Finder!")

if __name__ == "__main__":
    main()