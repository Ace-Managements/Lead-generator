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
from openpyxl.styles import PatternFill, Font
from openpyxl.formatting.rule import CellIsRule

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
        self.gmaps = googlemaps.Client(key='YOUR_GOOGLE_MAPS_API_KEY')

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

    # ... [Previous methods remain the same] ...

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

# Initialize the BusinessLeadFinder
lead_finder = BusinessLeadFinder()

# API Routes
@app.route('/generate_leads', methods=['POST'])
def generate_leads():
    try:
        data = request.get_json()
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

        # Save leads to database
        for lead in lead_finder.leads:
            lead_finder.save_lead_to_db(lead)

        return jsonify({
            'success': True,
            'leads_found': len(lead_finder.leads),
            'message': f'Successfully generated {len(lead_finder.leads)} leads'
        })

    except Exception as e:
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
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
