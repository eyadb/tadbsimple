"""
Technical Indicator Calculator
Computes 23 streamlined indicators and stores to database
"""

import math
import logging
from datetime import date, timedelta
from statistics import mean
from typing import Optional, Tuple, List, Union
from decimal import Decimal, ROUND_HALF_UP

logger = logging.getLogger(__name__)


class IndicatorCalculator:
    """Calculates technical indicators from price history"""
    
    def __init__(self, db_manager):
        self.db = db_manager
    
    @staticmethod
    def _sma(values: List[Union[Decimal, float]], window: int) -> Optional[float]:
        """Simple Moving Average - uses Decimal for precision, returns float"""
        if len(values) < window:
            return None
        # Calculate using Decimal precision
        total = Decimal(0)
        for val in values[:window]:
            if val is not None:
                total += Decimal(str(val)) if not isinstance(val, Decimal) else val
        avg = total / Decimal(window)
        # Round to 2 decimal places and convert to float for storage
        return float(avg.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
    
    @staticmethod
    def _sma_prev(values: List[Union[Decimal, float]], window: int) -> Optional[float]:
        """Previous day's SMA (offset by 1) - uses Decimal for precision, returns float"""
        if len(values) < window + 1:
            return None
        # Calculate using Decimal precision
        total = Decimal(0)
        for val in values[1:window + 1]:
            if val is not None:
                total += Decimal(str(val)) if not isinstance(val, Decimal) else val
        avg = total / Decimal(window)
        # Round to 2 decimal places and convert to float for storage
        return float(avg.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
    
    @staticmethod
    def _adr20(highs: List[Union[Decimal, float]], lows: List[Union[Decimal, float]]) -> Optional[float]:
        """Average Daily Range % over 20 days - uses Decimal for precision"""
        if len(highs) < 20 or len(lows) < 20:
            return None
        
        total_range = Decimal(0)
        count = 0
        for h, l in zip(highs[:20], lows[:20]):
            if h is not None and l is not None:
                h_dec = Decimal(str(h)) if not isinstance(h, Decimal) else h
                l_dec = Decimal(str(l)) if not isinstance(l, Decimal) else l
                total_range += (h_dec - l_dec)
                count += 1
        
        if count < 20:
            return None
        
        avg_range = (total_range / Decimal(20)) * Decimal(100)
        return float(avg_range.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
    
    @staticmethod
    def _avd20(closes: List[Union[Decimal, float]], volumes: List[int]) -> Optional[float]:
        """Average Dollar Volume over 20 days - uses Decimal for precision"""
        if len(closes) < 20 or len(volumes) < 20:
            return None
        
        # Calculate average close
        total_close = Decimal(0)
        for c in closes[:20]:
            if c is not None:
                total_close += Decimal(str(c)) if not isinstance(c, Decimal) else c
        avg_close = total_close / Decimal(20)
        
        # Calculate average volume
        total_volume = sum(volumes[:20])
        avg_volume = Decimal(total_volume) / Decimal(20)
        
        # Calculate dollar volume
        dollar_volume = avg_close * avg_volume
        return float(dollar_volume.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
    
    @staticmethod
    def _atr14(highs: List[float], lows: List[float], closes: List[float]) -> Optional[float]:
        """Average True Range (Wilder's smoothing) over 14 days"""
        # Need chronological order (oldest first)
        H = list(reversed(highs))
        L = list(reversed(lows))
        C = list(reversed(closes))
        
        n = min(len(H), len(L), len(C))
        if n < 15:
            return None
        
        # Calculate True Range
        TR = []
        for i in range(1, n):
            tr = max(
                H[i] - L[i],
                abs(H[i] - C[i - 1]),
                abs(L[i] - C[i - 1])
            )
            if math.isfinite(tr):
                TR.append(tr)
        
        if len(TR) < 14:
            return None
        
        # Wilder's smoothing
        atr = sum(TR[:14]) / 14
        for i in range(14, len(TR)):
            atr = ((13 * atr) + TR[i]) / 14
        
        return round(atr, 2)
    
    @staticmethod
    def _volume_ratio(volumes: List[int], recent: int, window: int) -> Optional[float]:
        """Volume ratio: sum(recent N days) / avg(window days)"""
        if len(volumes) < max(recent, window):
            return None
        
        recent_sum = sum(volumes[:recent])
        avg_vol = mean(volumes[:window])
        
        if avg_vol == 0:
            return None
        return round(recent_sum / avg_vol, 2)
    
    def _calculate_highs(self, symbol: str, target_date: date) -> Tuple:
        """Calculate 52-week and 6-month highs with dates"""
        rows = self.db.get_price_history(symbol, days=420)
        if not rows:
            return None, None, None, None
        
        # Convert to chronological
        rows = list(reversed(rows))
        
        cutoff_52w = target_date - timedelta(days=365)
        cutoff_6m = target_date - timedelta(days=182)
        
        year_rows = [r for r in rows if r[0] >= cutoff_52w]
        m6_rows = [r for r in rows if r[0] >= cutoff_6m]
        
        def find_max_close(data):
            max_close = None
            max_date = None
            for dt, _, _, _, close, _ in data:
                if close and (max_close is None or close > max_close):
                    max_close = close
                    max_date = dt
            return (round(float(max_close), 2), max_date) if max_close else (None, None)
        
        ftwh, ftwh_date = find_max_close(year_rows)
        tswh, tswh_date = find_max_close(m6_rows)
        
        return ftwh, ftwh_date, tswh, tswh_date
    
    def calculate_for_symbol(self, symbol: str, target_date: date) -> Optional[Tuple]:
        """Calculate all 23 indicators for one symbol"""
        try:
            # Fetch price history (DESC order)
            rows = self.db.get_price_history(symbol, days=250)
            if len(rows) < 200:
                return None
            
            # Extract series
            closes = [r[4] for r in rows if r[4]]
            opens = [r[1] for r in rows if r[1]]
            highs = [r[2] for r in rows if r[2]]
            lows = [r[3] for r in rows if r[3]]
            volumes = [r[5] for r in rows if r[5]]
            
            if not all([closes, highs, lows, volumes]):
                return None
            
            # Calculate SMAs
            sma5 = self._sma(closes, 5)
            sma10 = self._sma(closes, 10)
            sma20 = self._sma(closes, 20)
            sma50 = self._sma(closes, 50)
            sma100 = self._sma(closes, 100)
            sma200 = self._sma(closes, 200)
            
            # Previous day SMAs
            sma5s1 = self._sma_prev(closes, 5)
            sma10s1 = self._sma_prev(closes, 10)
            sma20s1 = self._sma_prev(closes, 20)
            sma50s1 = self._sma_prev(closes, 50)
            sma100s1 = self._sma_prev(closes, 100)
            sma200s1 = self._sma_prev(closes, 200)
            
            # Volatility metrics
            adr20 = self._adr20(highs, lows)
            avd20 = self._avd20(closes, volumes)
            atr14 = self._atr14(highs, lows, closes)
            
            # Volume ratios
            a130 = self._volume_ratio(volumes, 1, 30)
            a260 = self._volume_ratio(volumes, 2, 60)
            a390 = self._volume_ratio(volumes, 3, 90)
            
            # Highs
            ftwh, ftwh_date, tswh, tswh_date = self._calculate_highs(symbol, target_date)
            
            return (
                symbol, target_date.strftime('%Y-%m-%d'),
                sma5, sma10, sma20, sma50, sma100, sma200,
                sma5s1, sma10s1, sma20s1, sma50s1, sma100s1, sma200s1,
                adr20, avd20, atr14,
                a130, a260, a390,
                ftwh, ftwh_date, tswh, tswh_date
            )
            
        except Exception as e:
            logger.error(f"Error calculating indicators for {symbol}: {e}")
            return None
    
    def process_all_symbols(self, batch_size: int = 500) -> int:
        """Calculate and store indicators for all symbols"""
        try:
            target_date = self.db.get_latest_date()
            if not target_date:
                logger.warning("No latest date found")
                return 0
            
            symbols = self.db.get_all_symbols()
            if not symbols:
                logger.info("No symbols to process")
                return 0
            
            logger.info(f"Processing {len(symbols)} symbols for {target_date}")
            
            batch = []
            total_written = 0
            
            for symbol in symbols:
                result = self.calculate_for_symbol(symbol, target_date)
                if result:
                    batch.append(result)
                
                if len(batch) >= batch_size:
                    written = self.db.batch_insert_indicators(batch)
                    total_written += written
                    batch.clear()
            
            # Write remaining
            if batch:
                written = self.db.batch_insert_indicators(batch)
                total_written += written
            
            logger.info(f"Total indicators written: {total_written}")
            return total_written
            
        except Exception as e:
            logger.error(f"Error processing all symbols: {e}")
            return 0