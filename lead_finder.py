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
from flask import Flask, request, jsonify
from flask_cors import CORS

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

    def process_search_query(self, driver, query, niche, city):
        url = f"https://www.google.com/maps/search/{query.replace(' ', '+')}"
        driver.get(url)
        time.sleep(2)

        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.Nv2PK")))
        except TimeoutException:
            return

        results = driver.find_elements(By.CSS_SELECTOR, "div.Nv2PK")
        for result in results:
            if self.current_leads >= self.target_leads:
                return
            info = self.extract_business_info(result, driver)
            if info:
                info['city'] = city
                self.leads.append(info)
                self.current_leads += 1

        driver.quit()

    def extract_business_info(self, element, driver):
        try:
            name = element.find_element(By.CSS_SELECTOR, "div.fontHeadlineSmall").text.strip()
            return {'Business Name': name, 'Phone': '', 'city': ''}
        except Exception as e:
            self.logger.error(f"Extraction error: {str(e)}")
            return None

    def find_leads(self, niche, city, province, target_leads=50):
        self.current_leads = 0
        self.target_leads = target_leads
        self.leads = []
        driver = self.initialize_driver()
        self.process_search_query(driver, f"{niche} in {city}, {province}", niche, city)
        return self.leads

@app.route("/generate_leads", methods=["POST"])
def generate_leads():
    data = request.json
    niche = data.get('niche')
    city = data.get('city')
    province = data.get('province')
    target_leads = data.get('target_leads', 50)

    if not niche or not city or not province:
        return jsonify({"error": "Missing required fields"}), 400

    finder = BusinessLeadFinder()
    leads = finder.find_leads(niche, city, province, target_leads)

    return jsonify({"status": "success", "leads": leads})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
