"""
Fetch and store fundamental data for stocks
Retrieves company profile data from Financial Modeling Prep API
"""

import logging
import sys
import requests
from datetime import date
import mysql.connector
from mysql.connector import Error
import config

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FundamentalDataFetcher:
    """Fetch and store fundamental stock data"""
    
    def __init__(self, db_config: dict, api_key: str):
        self.db_config = db_config
        self.api_key = api_key
        self.conn = None
        self.base_url = "https://financialmodelingprep.com/stable/profile"
        self.connect()
        
    def connect(self):
        """Establish database connection"""
        try:
            self.conn = mysql.connector.connect(**self.db_config, autocommit=True)
            logger.info("Connected to database")
        except Error as e:
            logger.error(f"Database connection error: {e}")
            raise
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
        logger.info("Database connection closed")
    
    def ensure_connection(self):
        """Ensure database connection is alive, reconnect if needed"""
        try:
            if self.conn is None or not self.conn.is_connected():
                logger.warning("Database connection lost, reconnecting...")
                self.connect()
        except Error:
            logger.warning("Database connection check failed, reconnecting...")
            self.connect()
    
    def create_fundamentals_table(self):
        """Create table for fundamental data if it doesn't exist"""
        create_table_query = """
        CREATE TABLE IF NOT EXISTS stock_fundamentals (
            id INT AUTO_INCREMENT PRIMARY KEY,
            symbol VARCHAR(20) NOT NULL,
            marketcap BIGINT,
            fiftytwoweeklow DECIMAL(10, 2),
            fiftytwoweekhigh DECIMAL(10, 2),
            averagevolume BIGINT,
            industry VARCHAR(255),
            sector VARCHAR(255),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY unique_symbol (symbol),
            INDEX idx_sector (sector),
            INDEX idx_industry (industry),
            INDEX idx_marketcap (marketcap)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
        
        try:
            cursor = self.conn.cursor()
            cursor.execute(create_table_query)
            cursor.close()
            logger.info("Table 'stock_fundamentals' created or already exists")
            return True
        except Error as e:
            logger.error(f"Error creating stock_fundamentals table: {e}")
            return False
    
    def get_symbol_list(self):
        """Get list of symbols from stockdatas with the most recent date"""
        try:
            cursor = self.conn.cursor()
            
            # Get the maximum date first
            cursor.execute("SELECT MAX(date) FROM stockdatas")
            max_date_result = cursor.fetchone()
            
            if not max_date_result or not max_date_result[0]:
                logger.error("No data found in stockdatas table")
                cursor.close()
                return []
            
            max_date = max_date_result[0]
            logger.info(f"Getting symbols for date: {max_date}")
            
            # Get all symbols for that date
            cursor.execute("SELECT DISTINCT symbol FROM stockdatas WHERE date = %s", (max_date,))
            symbols = [row[0] for row in cursor.fetchall()]
            cursor.close()
            
            logger.info(f"Found {len(symbols)} symbols to process")
            return symbols
            
        except Error as e:
            logger.error(f"Error getting symbol list: {e}")
            return []
    
    def fetch_profile_data(self, symbol: str):
        """Fetch profile data for a single symbol from API"""
        url = f"{self.base_url}?symbol={symbol}&apikey={self.api_key}"
        
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if not data or not isinstance(data, list) or len(data) == 0:
                logger.debug(f"No data returned for {symbol}")
                return None
            
            profile = data[0]
            
            # Extract the data we need
            marketcap = profile.get('marketCap')
            range_str = profile.get('range', '')
            averagevolume = profile.get('averageVolume')
            industry = profile.get('industry')
            sector = profile.get('sector')
            
            # Parse the range (format: "low-high")
            fiftytwoweeklow = None
            fiftytwoweekhigh = None
            if range_str and '-' in range_str:
                try:
                    parts = range_str.split('-')
                    if len(parts) == 2:
                        fiftytwoweeklow = float(parts[0].strip())
                        fiftytwoweekhigh = float(parts[1].strip())
                except (ValueError, IndexError) as e:
                    logger.debug(f"Could not parse range '{range_str}' for {symbol}: {e}")
            
            return {
                'symbol': symbol,
                'marketcap': marketcap,
                'fiftytwoweeklow': fiftytwoweeklow,
                'fiftytwoweekhigh': fiftytwoweekhigh,
                'averagevolume': averagevolume,
                'industry': industry,
                'sector': sector
            }
            
        except requests.RequestException as e:
            logger.debug(f"Error fetching data for {symbol}: {e}")
            return None
        except (KeyError, ValueError) as e:
            logger.debug(f"Error parsing data for {symbol}: {e}")
            return None
    
    def insert_fundamental_data(self, fundamental_data: dict):
        """Insert or update fundamental data in the database"""
        insert_query = """
        INSERT INTO stock_fundamentals (
            symbol, marketcap, fiftytwoweeklow, fiftytwoweekhigh, 
            averagevolume, industry, sector
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            marketcap = VALUES(marketcap),
            fiftytwoweeklow = VALUES(fiftytwoweeklow),
            fiftytwoweekhigh = VALUES(fiftytwoweekhigh),
            averagevolume = VALUES(averagevolume),
            industry = VALUES(industry),
            sector = VALUES(sector),
            updated_at = CURRENT_TIMESTAMP
        """
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                cursor = self.conn.cursor()
                cursor.execute(insert_query, (
                    fundamental_data['symbol'],
                    fundamental_data['marketcap'],
                    fundamental_data['fiftytwoweeklow'],
                    fundamental_data['fiftytwoweekhigh'],
                    fundamental_data['averagevolume'],
                    fundamental_data['industry'],
                    fundamental_data['sector']
                ))
                cursor.close()
                return True
            except Error as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Database error for {fundamental_data['symbol']}, retrying... ({attempt+1}/{max_retries})")
                    try:
                        self.connect()
                    except:
                        pass
                else:
                    logger.error(f"Error inserting fundamental data for {fundamental_data['symbol']}: {e}")
                    return False
        return False
    
    def process_all_symbols(self, batch_size: int = 100):
        """Fetch and store fundamental data for all symbols"""
        symbols = self.get_symbol_list()
        
        if not symbols:
            logger.error("No symbols to process")
            return 0
        
        total_processed = 0
        total_success = 0
        
        logger.info(f"Starting to process {len(symbols)} symbols in batches of {batch_size}...")
        
        # Process in batches to avoid long-running connections
        for batch_start in range(0, len(symbols), batch_size):
            batch_end = min(batch_start + batch_size, len(symbols))
            batch_symbols = symbols[batch_start:batch_end]
            
            logger.info(f"Processing batch {batch_start//batch_size + 1}: symbols {batch_start+1} to {batch_end}")
            
            for symbol in batch_symbols:
                # Fetch profile data
                profile_data = self.fetch_profile_data(symbol)
                
                if profile_data:
                    # Insert into database
                    if self.insert_fundamental_data(profile_data):
                        total_success += 1
                
                total_processed += 1
            
            # Close and reconnect after each batch to keep connection fresh
            logger.info(f"Batch complete: {total_success}/{total_processed} successful. Refreshing connection...")
            self.close()
            self.connect()
        
        logger.info(f"Completed: {total_processed} symbols processed, {total_success} successfully stored")
        return total_success


def main():
    """Main execution function"""
    try:
        # Initialize fetcher
        fetcher = FundamentalDataFetcher(config.REMOTE_DB, config.FMP_API_KEY)
        
        # Create fundamentals table if it doesn't exist
        if not fetcher.create_fundamentals_table():
            logger.error("Failed to create stock_fundamentals table")
            return 1
        
        # Fetch and store fundamental data for all symbols
        count = fetcher.process_all_symbols(batch_size=100)
        
        logger.info(f"Successfully processed {count} symbols")
        
        fetcher.close()
        return 0
        
    except Exception as e:
        logger.error(f"Error in main: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
