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
            self.chrome_options.add_argument('--disable-software-rasterizer')
            self.chrome_options.add_argument('--disable-extensions')
            self.chrome_options.add_argument('--window-size=1920,1080')
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
                    has_website TEXT,
                    website_url TEXT,
                    google_maps_url TEXT,
                    business_hours TEXT,
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
            # Initialize driver
            service = ChromeService()
            driver = webdriver.Chrome(service=service, options=self.chrome_options)
            driver.set_page_load_timeout(30)

            # Construct search query
            search_query = f"{niche} in {city}, {province}"
            url = f"https://www.google.com/maps/search/{search_query.replace(' ', '+')}"
            
            logger.info(f"Searching: {url}")
            driver.get(url)
            time.sleep(2)  # Allow page to load

            # Find and process results
            results = []
            try:
                elements = WebDriverWait(driver, 10).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.Nv2PK"))
                )
                
                for element in elements[:max_leads]:
                    try:
                        info = self.extract_business_info(element, driver)
                        if info:
                            info['city'] = city
                            results.append(info)
                            self.save_lead_to_db(info)
                    except Exception as e:
                        logger.error(f"Error extracting business info: {str(e)}")
                        continue
            except TimeoutException:
                logger.warning("Timeout waiting for search results")
            
            return results

        except Exception as e:
            logger.error(f"Search error: {str(e)}")
            return []
        finally:
            if driver:
                driver.quit()

    def extract_business_info(self, element, driver):
        try:
            name = element.find_element(By.CSS_SELECTOR, "div.fontHeadlineSmall").text.strip()
            return {
                'business_name': name,
                'phone': self.extract_phone(element, driver),
                'website_url': self.extract_website(element, driver),
                'rating': self.extract_rating(element),
                'review_count': self.extract_reviews(element),
                'google_maps_url': driver.current_url
            }
        except Exception as e:
            logger.error(f"Error extracting info: {str(e)}")
            return None

    def extract_phone(self, element, driver):
        try:
            phone_elements = element.find_elements(By.CSS_SELECTOR, "[data-tooltip*='phone']")
            for elem in phone_elements:
                text = elem.get_attribute("aria-label") or elem.text
                if match := re.search(r'\(?([0-9]{3})\)?[-.\s]?([0-9]{3})[-.\s]?([0-9]{4})', text):
                    return f"({match.group(1)}) {match.group(2)}-{match.group(3)}"
        except:
            pass
        return ''

    def extract_website(self, element, driver):
        try:
            website_elem = element.find_element(By.CSS_SELECTOR, "a[data-tooltip='Open website']")
            return website_elem.get_attribute('href')
        except:
            return ''

    def extract_rating(self, element):
        try:
            rating_elem = element.find_element(By.CSS_SELECTOR, "span.MW4etd")
            return float(rating_elem.text.strip())
        except:
            return None

    def extract_reviews(self, element):
        try:
            reviews = element.find_element(By.CSS_SELECTOR, "span.UY7F9").text
            if match := re.search(r'\((\d+)\)', reviews):
                return int(match.group(1))
        except:
            return 0

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
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat()
    })

@app.route('/generate_leads', methods=['POST'])
def generate_leads():
    """Generate leads endpoint"""
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
    """Fetch leads endpoint"""
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
