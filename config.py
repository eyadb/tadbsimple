"""
Configuration for Stock Indicators System
"""

# Remote Database (contains both stockdatas and stockindicators tables)
REMOTE_DB = {
    'host': '82.197.82.42',
    'port': 3306,
    'database': 'u360608955_YFskB',
    'user': 'u360608955_pJBq1',
    'password': 'TaSharmuta:51Sc'
}

# Financial Modeling Prep API
FMP_API_KEY = 'f58d8c91b6869bd5ab29d0a7c9e5e6b2'
FMP_BASE_URL = 'https://financialmodelingprep.com/api/v3'

# Processing Settings
BATCH_SIZE = 500
MIN_VOLUME = 100000  # Minimum average volume filter
EXCHANGES = ['NYSE', 'NASDAQ', 'AMEX']

# Logging
LOG_LEVEL = 'INFO'
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'