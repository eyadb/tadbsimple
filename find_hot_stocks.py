"""
Find and store hot stocks that meet criteria:
- Up more than 5% on the most recent day
- Volume > 2x average (a130 > 2 in stockindicators)
"""

import logging
import sys
from datetime import date
from decimal import Decimal
import mysql.connector
from mysql.connector import Error

import config

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class HotStockFinder:
    """Find stocks with high price movement and volume"""
    
    def __init__(self, db_config: dict):
        self.db_config = db_config
        self.conn = None
        self.connect()
        
    def connect(self):
        """Establish database connection"""
        try:
            self.conn = mysql.connector.connect(**self.db_config, autocommit=True)
            if self.conn.is_connected():
                logger.info("Hot Stock Finder connected to database")
            else:
                logger.error("Connection object created but not connected")
        except Error as e:
            logger.error(f"Database connection error: {e}")
            self.conn = None
            raise
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
        logger.info("Database connection closed")
    
    def create_hot_stocks_table(self):
        """Create table for hot stocks if it doesn't exist"""
        if not self.conn or not self.conn.is_connected():
            logger.error("MySQL Connection not available")
            return False
            
        create_table_query = """
        CREATE TABLE IF NOT EXISTS hot_stocks (
            id INT AUTO_INCREMENT PRIMARY KEY,
            symbol VARCHAR(20) NOT NULL,
            date DATE NOT NULL,
            open DECIMAL(10, 2),
            close DECIMAL(10, 2),
            price_change_pct DECIMAL(10, 2),
            volume BIGINT,
            volume_ratio DECIMAL(10, 2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY unique_symbol_date (symbol, date),
            INDEX idx_date (date),
            INDEX idx_price_change (price_change_pct),
            INDEX idx_volume_ratio (volume_ratio)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
        
        try:
            cursor = self.conn.cursor()
            cursor.execute(create_table_query)
            cursor.close()
            logger.info("Table 'hot_stocks' created or already exists")
            return True
        except Error as e:
            logger.error(f"Error creating hot_stocks table: {e}")
            return False
    
    def get_latest_date(self) -> date:
        """Get the most recent date from stockdatas"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT MAX(date) FROM stockdatas")
            result = cursor.fetchone()
            cursor.close()
            if result and result[0]:
                return result[0]
            return None
        except Error as e:
            logger.error(f"Error getting latest date: {e}")
            return None
    
    def find_hot_stocks(self, min_price_change_pct: float = 5.0, min_volume_ratio: float = 2.0):
        """
        Find stocks that meet criteria:
        - Current day's close is > min_price_change_pct% higher than previous day's close
        - Volume ratio (a130) > min_volume_ratio
        """
        latest_date = self.get_latest_date()
        if not latest_date:
            logger.error("Could not determine latest date")
            return []
        
        logger.info(f"Finding hot stocks for date: {latest_date}")
        
        # Query to find stocks meeting criteria
        # Compare today's close to previous day's close
        query = """
        SELECT 
            sd_today.symbol,
            sd_today.date,
            sd_today.open,
            sd_today.close,
            sd_today.volume,
            si.a130,
            CASE 
                WHEN sd_prev.close > 0 THEN ((sd_today.close - sd_prev.close) / sd_prev.close * 100)
                ELSE NULL
            END as price_change_pct
        FROM stockdatas sd_today
        INNER JOIN stockindicators si 
            ON sd_today.symbol = si.symbol AND sd_today.date = si.date
        INNER JOIN stockdatas sd_prev
            ON sd_today.symbol = sd_prev.symbol 
            AND sd_prev.date = (
                SELECT MAX(date) 
                FROM stockdatas 
                WHERE symbol = sd_today.symbol 
                AND date < sd_today.date
            )
        WHERE sd_today.date = %s
            AND sd_prev.close > 0
            AND sd_today.close > sd_prev.close
            AND ((sd_today.close - sd_prev.close) / sd_prev.close * 100) > %s
            AND si.a130 > %s
        ORDER BY price_change_pct DESC
        """
        
        try:
            cursor = self.conn.cursor()
            cursor.execute(query, (latest_date, min_price_change_pct, min_volume_ratio))
            results = cursor.fetchall()
            cursor.close()
            
            logger.info(f"Found {len(results)} hot stocks meeting criteria")
            return results
            
        except Error as e:
            logger.error(f"Error finding hot stocks: {e}")
            return []
    
    def insert_hot_stocks(self, hot_stocks: list):
        """Insert hot stocks into the hot_stocks table"""
        if not hot_stocks:
            logger.info("No hot stocks to insert")
            return 0
        
        insert_query = """
        INSERT INTO hot_stocks (
            symbol, date, open, close, price_change_pct, volume, volume_ratio
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            open = VALUES(open),
            close = VALUES(close),
            price_change_pct = VALUES(price_change_pct),
            volume = VALUES(volume),
            volume_ratio = VALUES(volume_ratio),
            created_at = CURRENT_TIMESTAMP
        """
        
        try:
            cursor = self.conn.cursor()
            
            # Prepare data for insertion
            insert_data = []
            for row in hot_stocks:
                symbol, date, open_price, close_price, volume, a130, price_change_pct = row
                insert_data.append((
                    symbol,
                    date,
                    open_price,
                    close_price,
                    price_change_pct,
                    volume,
                    a130
                ))
            
            cursor.executemany(insert_query, insert_data)
            count = cursor.rowcount
            cursor.close()
            
            logger.info(f"Inserted/updated {count} hot stocks")
            return count
            
        except Error as e:
            logger.error(f"Error inserting hot stocks: {e}")
            return 0
    
    def delete_old_records(self, days_to_keep: int = 7):
        """Delete records older than specified number of days"""
        delete_query = """
        DELETE FROM hot_stocks 
        WHERE date < DATE_SUB(CURDATE(), INTERVAL %s DAY)
        """
        
        try:
            cursor = self.conn.cursor()
            cursor.execute(delete_query, (days_to_keep,))
            deleted_count = cursor.rowcount
            cursor.close()
            
            logger.info(f"Deleted {deleted_count} records older than {days_to_keep} days")
            return deleted_count
            
        except Error as e:
            logger.error(f"Error deleting old records: {e}")
            return 0
    
    def display_hot_stocks(self, hot_stocks: list):
        """Display hot stocks in a formatted table"""
        if not hot_stocks:
            print("\nNo hot stocks found.")
            return
        
        print(f"\n{'='*100}")
        print(f"HOT STOCKS - Up >5% from Previous Day's Close with Volume >2x Average")
        print(f"{'='*100}")
        print(f"{'Symbol':<10} {'Date':<12} {'Open':<10} {'Close':<10} {'Change %':<12} {'Volume':<15} {'Vol Ratio':<10}")
        print(f"{'-'*100}")
        
        for row in hot_stocks:
            symbol, date, open_price, close_price, volume, a130, price_change_pct = row
            print(f"{symbol:<10} {str(date):<12} {float(open_price):>9.2f} {float(close_price):>9.2f} "
                  f"{float(price_change_pct):>11.2f}% {volume:>14,} {float(a130):>9.2f}")
        
        print(f"{'-'*100}")
        print(f"Total: {len(hot_stocks)} stocks\n")


def main():
    """Main execution function"""
    try:
        # Initialize finder
        finder = HotStockFinder(config.REMOTE_DB)
        
        # Create hot_stocks table if it doesn't exist
        if not finder.create_hot_stocks_table():
            logger.error("Failed to create hot_stocks table")
            return 1
        
        # Delete old records (older than 7 days)
        finder.delete_old_records(days_to_keep=7)
        
        # Find hot stocks (>5% gain, >2x volume)
        hot_stocks = finder.find_hot_stocks(min_price_change_pct=5.0, min_volume_ratio=2.0)
        
        # Display results
        finder.display_hot_stocks(hot_stocks)
        
        # Insert into database
        if hot_stocks:
            count = finder.insert_hot_stocks(hot_stocks)
            logger.info(f"Successfully processed {count} hot stocks")
        
        finder.close()
        return 0
        
    except Exception as e:
        logger.error(f"Error in main: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
