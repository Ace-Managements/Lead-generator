from flask import Flask, request, jsonify
from flask_cors import CORS
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
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize Flask app
app = Flask(__name__)
CORS(app)

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
        api_key = os.getenv('GOOGLE_MAPS_API_KEY')
        if not api_key:
            self.logger.warning("Google Maps API key not found in environment variables")
        self.gmaps = googlemaps.Client(key=api_key)

    def setup_logging(self):
        os.makedirs('logs', exist_ok=True)
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler('logs/lead_finder.log')
            ]
        )
        self.logger = logging.getLogger('LeadFinder')

    def setup_driver_options(self):
        self.chrome_options = Options()
        self.chrome_options.add_argument('--headless=new')
        self.chrome_options.add_argument('--no-sandbox')
        self.chrome_options.add_argument('--disable-dev-shm-usage')
        self.chrome_options.add_argument('--disable-gpu')
        self.chrome_options.add_argument('--window-size=1920,1080')
        self.chrome_options.add_argument('--disable-notifications')
        self.chrome_options.add_argument('--incognito')
        
        # Render-specific Chrome binary location
        chrome_bin = os.getenv('GOOGLE_CHROME_BIN')
        if chrome_bin:
            self.chrome_options.binary_location = chrome_bin

    def setup_database(self):
        db_path = os.getenv('DATABASE_PATH', 'leads_database.db')
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        cursor = self.conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS leads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
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
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
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

    def process_search_query(self, driver, query, city):
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
                    self.save_lead_to_db(info)
                    self.current_leads += 1
                    self.logger.info(f"Collected leads: {self.current_leads}/{self.target_leads}")
                
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
                self.process_search_query(driver, query, city)

        finally:
            if driver:
                driver.quit()

    def save_lead_to_db(self, lead):
        try:
            cursor = self.conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO leads (
                    business_name, phone, has_website, website_url, 
                    google_maps_url, business_hours, rating, review_count,
                    called, deal_status, notes, city
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                lead['Business Name'], lead['Phone'], lead['Has Website'],
                lead['Website URL'], lead['Google Maps URL'], lead['Business Hours'],
                lead['Rating'], lead['Review Count'], lead['Called'],
                lead['Deal Status'], lead['Notes'], lead.get('city', '')
            ))
            self.conn.commit()
            return True
        except Exception as e:
            self.logger.error(f"Database error: {str(e)}")
            return False

    def get_leads_from_db(self, limit=100):
        try:
            cursor = self.conn.cursor()
            cursor.execute('''
                SELECT * FROM leads 
                ORDER BY timestamp DESC 
                LIMIT ?
            ''', (limit,))
            columns = [description[0] for description in cursor.description]
            leads = [dict(zip(columns, row)) for row in cursor.fetchall()]
            return leads
        except Exception as e:
            self.logger.error(f"Database fetch error: {str(e)}")
            return []

    def clear_leads_db(self):
        try:
            cursor = self.conn.cursor()
            cursor.execute('DELETE FROM leads')
            self.conn.commit()
            return True
        except Exception as e:
            self.logger.error(f"Database clear error: {str(e)}")
            return False

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

# Initialize the BusinessLeadFinder
lead_finder = BusinessLeadFinder()

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'healthy', 'timestamp': datetime.now().isoformat()})

@app.route('/generate_leads', methods=['POST'])
def generate_leads():
    try:
        data = request.get_json()
        if not data:
            return jsonify({'error': 'No data provided'}), 400

        niche = data.get('niche')
        city = data.get('city')
        province = data.get('province')
        target_leads = int(data.get('target_leads', 50))

        if not all([niche, city, province]):
            return jsonify({'error': 'Missing required parameters'}), 400

        lead_finder.current_leads = 0
        lead_finder.target_leads = target_leads
        lead_finder.leads = []

        locations = lead_finder.get_expanded_locations(city, province)
        
        for search_city, search_province in locations:
            if lead_finder.current_leads >= target_leads:
                break
            lead_finder.search_location(niche, search_city, search_province)

        return jsonify({
            'success': True,
            'leads_found': len(lead_finder.leads),
            'message': f'Successfully generated {len(lead_finder.leads)} leads'
        })

    except Exception as e:
        app.logger.error(f"Generate leads error: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/fetch_leads', methods=['GET'])
def fetch_leads():
    try:
        limit = request.args.get('limit', 100, type=int)
        leads = lead_finder.get_leads_from_db(limit)
        return jsonify({
            'success': True,
            'leads': leads
        })
    except Exception as e:
        app.logger.error(f"Fetch leads error: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/clear_leads', methods=['POST'])
def clear_leads():
    try:
        success = lead_finder.clear_leads_db()
        return jsonify({
            'success': success,
            'message': 'Successfully cleared all leads' if success else 'Failed to clear leads'
        })
    except Exception as e:
        app.logger.error(f"Clear leads error: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
