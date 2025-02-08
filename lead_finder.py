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
import logging
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize Flask app
app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BusinessLeadFinder:
    def __init__(self):
        self.setup_driver_options()
        self.setup_database()
        self.collected_businesses = set()
        self.current_leads = 0
        self.target_leads = 0
        self.leads = []

    def setup_driver_options(self):
        try:
            self.chrome_options = Options()
            self.chrome_options.add_argument('--headless=new')
            self.chrome_options.add_argument('--no-sandbox')
            self.chrome_options.add_argument('--disable-dev-shm-usage')
            self.chrome_options.add_argument('--disable-gpu')
            self.chrome_options.add_argument('--window-size=1920,1080')
            self.chrome_options.add_argument('--disable-notifications')
            self.chrome_options.add_argument('--enable-javascript')
            
            # Add user agent
            self.chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
            
            chrome_bin = os.getenv('GOOGLE_CHROME_BIN')
            if chrome_bin:
                self.chrome_options.binary_location = chrome_bin
        except Exception as e:
            logger.error(f"Error setting up Chrome options: {str(e)}")

    def setup_database(self):
        try:
            self.conn = sqlite3.connect('leads.db', check_same_thread=False)
            cursor = self.conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS leads (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    business_name TEXT,
                    phone TEXT,
                    website_url TEXT,
                    google_maps_url TEXT,
                    rating REAL,
                    review_count INTEGER,
                    city TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(business_name, city)
                )
            ''')
            self.conn.commit()
        except Exception as e:
            logger.error(f"Database setup error: {str(e)}")
            raise

    def search_business(self, niche, city, province, max_leads=10):
        driver = None
        try:
            logger.info(f"Starting search for {niche} in {city}, {province}")
            
            for attempt in range(3):
                try:
                    service = ChromeService()
                    driver = webdriver.Chrome(service=service, options=self.chrome_options)
                    driver.set_page_load_timeout(30)
                    logger.info("Chrome driver initialized successfully")
                    break
                except Exception as e:
                    logger.error(f"Driver initialization attempt {attempt + 1} failed: {str(e)}")
                    if attempt == 2:
                        raise
            
            search_query = f"{niche} in {city}, {province}"
            url = f"https://www.google.com/maps/search/{search_query.replace(' ', '+')}"
            
            logger.info(f"Navigating to URL: {url}")
            driver.get(url)
            time.sleep(5)
            
            logger.info("Waiting for results...")
            try:
                results_container = WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div.Nv2PK"))
                )
                
                # Scroll to load more results
                last_height = driver.execute_script("return document.body.scrollHeight")
                scroll_attempts = 0
                while scroll_attempts < 3:
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(2)
                    new_height = driver.execute_script("return document.body.scrollHeight")
                    if new_height == last_height:
                        scroll_attempts += 1
                    else:
                        scroll_attempts = 0
                        last_height = new_height
                
                elements = driver.find_elements(By.CSS_SELECTOR, "div.Nv2PK")
                logger.info(f"Found {len(elements)} initial results")
                
                results = []
                for idx, element in enumerate(elements[:max_leads]):
                    try:
                        logger.info(f"Processing result {idx + 1}")
                        info = self.extract_business_info(element, driver)
                        if info:
                            info['city'] = city
                            results.append(info)
                            self.save_lead_to_db(info)
                            logger.info(f"Successfully processed: {info.get('business_name', 'Unknown')}")
                    except Exception as e:
                        logger.error(f"Error processing result {idx + 1}: {str(e)}")
                        continue
                
                return results
                
            except TimeoutException:
                logger.error("Timeout waiting for results to load")
                return []
                
        except Exception as e:
            logger.error(f"Search error: {str(e)}")
            return []
            
        finally:
            if driver:
                try:
                    driver.quit()
                    logger.info("Chrome driver closed successfully")
                except:
                    pass

    def extract_business_info(self, element, driver):
        try:
            name = element.find_element(By.CSS_SELECTOR, "div.fontHeadlineSmall").text.strip()
            info = {
                'business_name': name,
                'phone': '',
                'website_url': '',
                'google_maps_url': driver.current_url,
                'rating': None,
                'review_count': 0
            }
            
            try:
                element.click()
                time.sleep(2)
                
                # Get phone number
                phone_elements = driver.find_elements(By.CSS_SELECTOR, "button[data-tooltip*='phone']")
                for elem in phone_elements:
                    text = elem.get_attribute("aria-label") or elem.text
                    if match := re.search(r'\(?([0-9]{3})\)?[-.\s]?([0-9]{3})[-.\s]?([0-9]{4})', text):
                        info['phone'] = f"({match.group(1)}) {match.group(2)}-{match.group(3)}"
                        break
                
                # Get website
                try:
                    website_elem = driver.find_element(By.CSS_SELECTOR, "a[data-tooltip='Open website']")
                    info['website_url'] = website_elem.get_attribute('href')
                except:
                    pass
                
                # Get rating and reviews
                try:
                    rating_elem = driver.find_element(By.CSS_SELECTOR, "span.MW4etd")
                    info['rating'] = float(rating_elem.text.strip())
                    
                    reviews = driver.find_element(By.CSS_SELECTOR, "span.UY7F9").text
                    if match := re.search(r'\((\d+)\)', reviews):
                        info['review_count'] = int(match.group(1))
                except:
                    pass
                
            except Exception as e:
                logger.error(f"Error extracting details: {str(e)}")
            
            return info
            
        except Exception as e:
            logger.error(f"Error extracting business info: {str(e)}")
            return None

    def save_lead_to_db(self, lead):
        try:
            cursor = self.conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO leads (
                    business_name, phone, website_url, 
                    google_maps_url, rating, review_count, city
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                lead['business_name'], lead['phone'], lead['website_url'],
                lead['google_maps_url'], lead['rating'], lead['review_count'],
                lead.get('city', '')
            ))
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Database error: {str(e)}")
            return False

    def get_leads_from_db(self, limit=100):
        try:
            cursor = self.conn.cursor()
            cursor.execute('SELECT * FROM leads ORDER BY timestamp DESC LIMIT ?', (limit,))
            columns = [description[0] for description in cursor.description]
            leads = [dict(zip(columns, row)) for row in cursor.fetchall()]
            return leads
        except Exception as e:
            logger.error(f"Error fetching leads: {str(e)}")
            return []

# Initialize lead finder
lead_finder = BusinessLeadFinder()

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat()
    })

@app.route('/generate_leads', methods=['POST'])
def generate_leads():
    try:
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'error': 'No data provided'}), 400

        niche = data.get('niche')
        city = data.get('city')
        province = data.get('province')
        target_leads = int(data.get('target_leads', 10))

        if not all([niche, city, province]):
            return jsonify({'success': False, 'error': 'Missing required parameters'}), 400

        logger.info(f"Generating leads for {niche} in {city}, {province}")
        leads = lead_finder.search_business(niche, city, province, target_leads)

        return jsonify({
            'success': True,
            'leads_found': len(leads),
            'message': f'Successfully generated {len(leads)} leads'
        })

    except Exception as e:
        logger.error(f"Generate leads error: {str(e)}")
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
        logger.error(f"Fetch leads error: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
