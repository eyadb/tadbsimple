"""
Stock Indicators System - Main Orchestrator
Minimal, focused workflow: Fetch → (optional cleanup) → Calculate → Store → Find Hot Stocks
"""

import logging
import argparse
from datetime import datetime
import config
from db_manager import DatabaseManager
from data_fetcher import DataFetcher
from indicator_engine import IndicatorCalculator
from find_hot_stocks import HotStockFinder

# Configure logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format=config.LOG_FORMAT
)
logger = logging.getLogger(__name__)


class StockIndicatorSystem:
    """Main orchestrator for stock indicator calculation pipeline"""
    
    def __init__(self):
        logger.info("Initializing Stock Indicator System...")
        
        # Database connection
        self.db = DatabaseManager(config.REMOTE_DB)
        
        # Components
        self.fetcher = DataFetcher(config.FMP_API_KEY, config.FMP_BASE_URL, self.db)
        self.calculator = IndicatorCalculator(self.db)
        self.hot_stocks_finder = HotStockFinder(config.REMOTE_DB)
    
    def run(self,
            fetch_data: bool = True,
            calculate_indicators: bool = True,
            cleanup_mode: str = "none",
            cleanup_date: str | None = None,
            find_hot_stocks: bool = True):
        """Main execution pipeline"""
        start_time = datetime.now()
        logger.info(f"Starting run at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        try:
            # Step 1: Fetch latest market data
            if fetch_data:
                logger.info("=" * 60)
                logger.info("STEP 1: Fetching Market Data")
                logger.info("=" * 60)
                total_fetched = self.fetcher.update_all_exchanges(config.EXCHANGES)
                logger.info(f"Fetched {total_fetched} stock records\n")
            
            # Optional cleanup before calculating indicators
            if calculate_indicators and cleanup_mode and cleanup_mode != "none":
                logger.info("=" * 60)
                logger.info("PREP: Cleaning indicators table before calculation")
                logger.info("=" * 60)
                deleted = 0
                if cleanup_mode == "truncate":
                    ok = self.db.truncate_indicators()
                    logger.info("Truncate completed" if ok else "Truncate failed")
                elif cleanup_mode == "keep-latest":
                    deleted = self.db.keep_only_latest_indicators()
                    logger.info(f"Pruned indicators, deleted {deleted} rows; kept latest date only")
                elif cleanup_mode == "keep-date":
                    if not cleanup_date:
                        logger.warning("--cleanup-mode keep-date requires --cleanup-date YYYY-MM-DD; skipping cleanup")
                    else:
                        from datetime import datetime as _dt
                        try:
                            d = _dt.strptime(cleanup_date, "%Y-%m-%d").date()
                            deleted = self.db.keep_only_indicator_date(d)
                            logger.info(f"Pruned indicators, deleted {deleted} rows; kept {d}")
                        except ValueError:
                            logger.warning("Invalid --cleanup-date format; expected YYYY-MM-DD. Skipping cleanup.")

            # Step 2: Calculate indicators
            if calculate_indicators:
                logger.info("=" * 60)
                logger.info("STEP 2: Calculating Indicators")
                logger.info("=" * 60)
                total_calculated = self.calculator.process_all_symbols(config.BATCH_SIZE)
                logger.info(f"Calculated indicators for {total_calculated} symbols\n")
            
            # Step 3: Find hot stocks
            if find_hot_stocks:
                logger.info("=" * 60)
                logger.info("STEP 3: Finding Hot Stocks")
                logger.info("=" * 60)
                
                # Create table if needed
                if not self.hot_stocks_finder.create_hot_stocks_table():
                    logger.error("Failed to create hot_stocks table")
                else:
                    # Delete old records
                    self.hot_stocks_finder.delete_old_records(days_to_keep=7)
                    
                    # Find and process hot stocks
                    hot_stocks = self.hot_stocks_finder.find_hot_stocks(
                        min_price_change_pct=5.0, 
                        min_volume_ratio=2.0
                    )
                    
                    # Display results
                    self.hot_stocks_finder.display_hot_stocks(hot_stocks)
                    
                    # Insert into database
                    if hot_stocks:
                        count = self.hot_stocks_finder.insert_hot_stocks(hot_stocks)
                        logger.info(f"Processed {count} hot stocks\n")
                    else:
                        logger.info("No hot stocks found\n")
            
        except Exception as e:
            logger.error(f"Error in main pipeline: {e}")
            raise
        
        finally:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            logger.info("=" * 60)
            logger.info(f"Run completed at {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info(f"Total duration: {duration:.2f} seconds")
            logger.info("=" * 60)
            self.cleanup()
    
    def cleanup(self):
        """Clean up resources"""
        self.hot_stocks_finder.close()
        self.db.close()
        logger.info("System shutdown complete")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stock Indicators Orchestrator")
    parser.add_argument("--fetch", action="store_true", help="Fetch latest market data before calculation")
    parser.add_argument("--no-fetch", dest="fetch", action="store_false", help="Skip fetching market data")
    parser.set_defaults(fetch=True)
    parser.add_argument("--calc", action="store_true", help="Calculate and store indicators")
    parser.add_argument("--no-calc", dest="calc", action="store_false", help="Skip indicator calculation")
    parser.set_defaults(calc=True)
    parser.add_argument("--hot-stocks", action="store_true", help="Find and store hot stocks")
    parser.add_argument("--no-hot-stocks", dest="hot_stocks", action="store_false", help="Skip finding hot stocks")
    parser.set_defaults(hot_stocks=True)
    parser.add_argument("--cleanup-mode", choices=["none", "truncate", "keep-latest", "keep-date"], default="truncate",
                        help="Cleanup behavior for stockindicators before calculation")
    parser.add_argument("--cleanup-date", help="YYYY-MM-DD (used with --cleanup-mode keep-date)")
    args = parser.parse_args()

    system = StockIndicatorSystem()
    system.run(
        fetch_data=args.fetch,
        calculate_indicators=args.calc,
        find_hot_stocks=args.hot_stocks,
        cleanup_mode=args.cleanup_mode,
        cleanup_date=args.cleanup_date
    )