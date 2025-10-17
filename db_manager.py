"""
Simplified Database Manager
Handles remote database with both stockdatas and stockindicators tables
"""

import logging
from typing import List, Tuple, Optional
from datetime import date, timedelta
from decimal import Decimal
import mysql.connector
from mysql.connector import Error

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manages connection to remote MySQL database"""
    
    def __init__(self, db_config: dict):
        self.db_config = db_config
        self.conn = None
        self.connect()
    
    def connect(self):
        """Establish database connection"""
        try:
            self.conn = mysql.connector.connect(**self.db_config, autocommit=True)
            logger.info("Connected to remote database")
        except Error as e:
            logger.error(f"Database connection error: {e}")
            raise
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
        logger.info("Database connection closed")
    
    # ========== Price Data Methods ==========
    
    def batch_insert_prices(self, data: List[Tuple]) -> int:
        """Insert/update stock price data in batch"""
        if not data:
            return 0
        
        query = """
            INSERT INTO stockdatas (symbol, date, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                open=VALUES(open), high=VALUES(high), low=VALUES(low),
                close=VALUES(close), volume=VALUES(volume)
        """
        
        try:
            cursor = self.conn.cursor()
            cursor.executemany(query, data)
            count = cursor.rowcount
            cursor.close()
            return count
        except Error as e:
            logger.error(f"Batch price insert error: {e}")
            return 0
    
    def get_latest_date(self) -> Optional[date]:
        """Get most recent trading date"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT MAX(date) FROM stockdatas")
            result = cursor.fetchone()
            cursor.close()
            return result[0] if result and result[0] else None
        except Error as e:
            logger.error(f"Get latest date error: {e}")
            return None
    
    def get_all_symbols(self) -> List[str]:
        """Get list of symbols with data for the most recent date"""
        try:
            cursor = self.conn.cursor()
            # Get symbols that have data for the most recent date
            query = """
                SELECT DISTINCT symbol 
                FROM stockdatas 
                WHERE date = (SELECT MAX(date) FROM stockdatas)
                ORDER BY symbol
            """
            cursor.execute(query)
            symbols = [row[0] for row in cursor.fetchall()]
            cursor.close()
            return symbols
        except Error as e:
            logger.error(f"Get symbols error: {e}")
            return []
    
    def get_price_history(self, symbol: str, days: int = 250) -> List[Tuple]:
        """Get price history for symbol (date DESC) - keeps Decimal precision"""
        try:
            cursor = self.conn.cursor()
            query = """
                SELECT date, open, high, low, close, volume
                FROM stockdatas
                WHERE symbol = %s
                ORDER BY date DESC
                LIMIT %s
            """
            cursor.execute(query, (symbol, days))
            rows = cursor.fetchall()
            cursor.close()
            
            # Keep Decimal values as-is for precise calculations
            # Only convert volume to int
            converted_rows = []
            for row in rows:
                converted_rows.append((
                    row[0],  # date - keep as is
                    row[1],  # open - keep as Decimal
                    row[2],  # high - keep as Decimal
                    row[3],  # low - keep as Decimal
                    row[4],  # close - keep as Decimal
                    int(row[5]) if row[5] is not None else 0  # volume
                ))
            return converted_rows
        except Error as e:
            logger.error(f"Get price history error for {symbol}: {e}")
            return []
    
    # ========== Indicators Methods ==========
    
    def batch_insert_indicators(self, data: List[Tuple]) -> int:
        """Batch insert/update indicator data"""
        if not data:
            return 0
        
        query = """
            INSERT INTO stockindicators (
                symbol, date,
                sma5, sma10, sma20, sma50, sma100, sma200,
                sma5s1, sma10s1, sma20s1, sma50s1, sma100s1, sma200s1,
                adr20, avd20, atr14,
                a130, a260, a390,
                ftwh, ftwhdate, tswh, tswhdate
            ) VALUES (
                %s, %s,
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s
            )
            ON DUPLICATE KEY UPDATE
                sma5=VALUES(sma5), sma10=VALUES(sma10), sma20=VALUES(sma20),
                sma50=VALUES(sma50), sma100=VALUES(sma100), sma200=VALUES(sma200),
                sma5s1=VALUES(sma5s1), sma10s1=VALUES(sma10s1), sma20s1=VALUES(sma20s1),
                sma50s1=VALUES(sma50s1), sma100s1=VALUES(sma100s1), sma200s1=VALUES(sma200s1),
                adr20=VALUES(adr20), avd20=VALUES(avd20), atr14=VALUES(atr14),
                a130=VALUES(a130), a260=VALUES(a260), a390=VALUES(a390),
                ftwh=VALUES(ftwh), ftwhdate=VALUES(ftwhdate),
                tswh=VALUES(tswh), tswhdate=VALUES(tswhdate)
        """
        
        try:
            cursor = self.conn.cursor()
            cursor.executemany(query, data)
            count = cursor.rowcount
            cursor.close()
            logger.info(f"Inserted/updated {count} indicator rows")
            return count
        except Error as e:
            logger.error(f"Batch indicator insert error: {e}")
            return 0

    # ========== Indicator Cleanup Utilities ==========

    def truncate_indicators(self) -> bool:
        """Delete all rows from stockindicators (tries TRUNCATE, falls back to DELETE)."""
        try:
            cursor = self.conn.cursor()
            try:
                cursor.execute("TRUNCATE TABLE stockindicators")
                logger.info("Truncated table stockindicators")
            except Error as e:
                logger.warning(f"TRUNCATE failed, falling back to DELETE: {e}")
                cursor.execute("DELETE FROM stockindicators")
                logger.info("Deleted all rows from stockindicators")
            cursor.close()
            return True
        except Error as e:
            logger.error(f"Failed to clear stockindicators: {e}")
            return False

    def keep_only_indicator_date(self, keep_date: date) -> int:
        """Delete all indicator rows except for the specified date. Returns rows deleted."""
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                "DELETE FROM stockindicators WHERE date <> %s",
                (keep_date,)
            )
            deleted = cursor.rowcount
            cursor.close()
            logger.info(f"Deleted {deleted} rows from stockindicators (kept date {keep_date})")
            return deleted
        except Error as e:
            logger.error(f"Failed to keep only date {keep_date} in stockindicators: {e}")
            return 0

    def keep_only_latest_indicators(self) -> int:
        """Keep only the latest date's indicator rows; delete older ones. Returns rows deleted."""
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT MAX(date) FROM stockindicators")
            res = cursor.fetchone()
            cursor.close()
            latest = res[0] if res and res[0] else None
            if not latest:
                logger.info("No rows in stockindicators; nothing to delete")
                return 0
            return self.keep_only_indicator_date(latest)
        except Error as e:
            logger.error(f"Failed to prune stockindicators to latest: {e}")
            return 0