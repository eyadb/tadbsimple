"""
Stock Data Fetcher - Financial Modeling Prep API
Fetches current market data and stores in database
"""

import logging
import requests
import pandas as pd
from datetime import datetime
from typing import List, Tuple

logger = logging.getLogger(__name__)


class DataFetcher:
    """Fetches stock market data from Financial Modeling Prep API"""
    
    def __init__(self, api_key: str, base_url: str, db_manager):
        self.api_key = api_key
        self.base_url = base_url
        self.db = db_manager
        self.etf_symbols = set()
        self._load_etf_list()
    
    def _load_etf_list(self):
        """Load list of ETF symbols to exclude"""
        etf_url = f"https://financialmodelingprep.com/stable/etf-list?apikey={self.api_key}"
        
        try:
            response = requests.get(etf_url, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if data and isinstance(data, list):
                self.etf_symbols = {item['symbol'] for item in data if 'symbol' in item}
                logger.info(f"Loaded {len(self.etf_symbols)} ETF symbols to exclude")
            else:
                logger.warning("ETF list returned no data or unexpected format")
                
        except requests.RequestException as e:
            logger.error(f"Failed to load ETF list: {e}")
            logger.warning("Continuing without ETF filtering")
    
    def fetch_exchange_data(self, exchange: str) -> pd.DataFrame:
        """Fetch all stocks from one exchange"""
        url = f"{self.base_url}/quotes/{exchange}?apikey={self.api_key}"
        
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if not data:
                logger.warning(f"No data returned for {exchange}")
                return pd.DataFrame()
            
            df = pd.DataFrame(data)
            logger.info(f"Fetched {len(df)} symbols from {exchange}")
            return df
            
        except requests.RequestException as e:
            logger.error(f"API request failed for {exchange}: {e}")
            return pd.DataFrame()
    
    def prepare_price_data(self, df: pd.DataFrame, exchange: str) -> List[Tuple]:
        """Convert DataFrame to database insert format"""
        if df.empty:
            return []
        
        # Filter quality data and exclude ETFs
        df = df[
            (df['avgVolume'] > 0) &
            (~df['symbol'].str.contains("-", na=False)) &
            (df['price'] > 0) &
            (~df['symbol'].isin(self.etf_symbols))
        ].copy()
        
        logger.info(f"{exchange}: {len(df)} symbols after filtering (ETFs excluded)")
        
        today = datetime.now().strftime('%Y-%m-%d')
        
        batch_data = []
        for _, row in df.iterrows():
            try:
                symbol = row['symbol'].strip()
                price = round(float(row['price']), 2)
                high = round(float(row.get('dayHigh', price)), 2)
                low = round(float(row.get('dayLow', price)), 2)
                volume = int(row.get('volume', 0))
                
                batch_data.append((
                    symbol, today,
                    price,
                    high, low, price,
                    volume
                ))
            except (ValueError, KeyError) as e:
                logger.debug(f"Skipping {row.get('symbol', 'unknown')}: {e}")
                continue
        
        return batch_data
    
    def update_all_exchanges(self, exchanges: List[str]) -> int:
        """Fetch and store data from all exchanges"""
        total_inserted = 0
        
        for exchange in exchanges:
            logger.info(f"Processing {exchange}...")
            
            df = self.fetch_exchange_data(exchange)
            if df.empty:
                continue
            
            batch_data = self.prepare_price_data(df, exchange)
            if batch_data:
                inserted = self.db.batch_insert_prices(batch_data)
                total_inserted += inserted
                logger.info(f"{exchange}: Inserted {inserted} records")
        
        return total_inserted
