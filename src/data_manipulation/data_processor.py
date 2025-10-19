"""
Data Processor Module for Arbitrage Detection System

This module handles cleaning, validating, and synchronizing market data
from multiple exchanges. It provides comprehensive data processing pipeline
with quality checks and time synchronization.

Author: Arbitrage Detection Team
Date: 2025-10-19
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
from typing import Dict, List, Tuple, Optional
import warnings
import logging

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore')

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataProcessor:
    """
    Handles data processing pipeline for market data.
    
    This class provides methods to clean, validate, and synchronize
    market data from multiple exchanges with comprehensive quality checks.
    
    Attributes:
        data_dir: Directory containing raw data files
        processed_dir: Directory for storing processed data
    """
    
    def __init__(self, data_dir: str = "data/2025-10-01"):
        """
        Initialize the DataProcessor.
        
        Args:
            data_dir: Path to directory containing raw data files
        """
        self.data_dir = data_dir
        # Extract date from data_dir (e.g., "data/2025-10-01" -> "2025-10-01")
        date_part = os.path.basename(data_dir)
        self.processed_dir = f"data/processed/{date_part}"
        os.makedirs(self.processed_dir, exist_ok=True)
        
        # Required columns for validation
        self.required_columns = [
            'timestamp', 'exchange', 'symbol', 'price', 'quantity', 'side'
        ]
        
        # Price validation parameters
        self.max_price = 1000000  # Maximum reasonable price
        self.max_time_range = timedelta(hours=2)  # Maximum time range
    
    def load_exchange_data(self, exchange: str) -> Dict[str, pd.DataFrame]:
        """
        Load all data files for a specific exchange.
        
        Args:
            exchange: Exchange name (binance, coinbase, kraken)
            
        Returns:
            Dictionary mapping symbol names to DataFrames
        """
        exchange_dir = f"{self.data_dir}/{exchange}"
        data = {}
        
        if not os.path.exists(exchange_dir):
            logger.error(f"Exchange directory not found: {exchange_dir}")
            return data
            
        for filename in os.listdir(exchange_dir):
            if filename.endswith('.csv'):
                symbol = filename.replace('.csv', '')
                filepath = f"{exchange_dir}/{filename}"
                
                try:
                    df = pd.read_csv(filepath)
                    data[symbol] = df
                    logger.info(f"✅ Loaded {exchange}/{symbol}: {len(df)} records")
                except Exception as e:
                    logger.error(f"Error loading {exchange}/{symbol}: {e}")
                    
        return data
    
    def clean_data(self, df: pd.DataFrame, exchange: str, symbol: str) -> pd.DataFrame:
        """
        Clean and standardize market data.
        
        Args:
            df: Raw DataFrame to clean
            exchange: Exchange name for logging
            symbol: Symbol name for logging
            
        Returns:
            Cleaned DataFrame
        """
        logger.info(f"🧹 Cleaning {exchange}/{symbol}...")
        initial_count = len(df)
        
        # Remove duplicates
        df = df.drop_duplicates()
        duplicates_removed = initial_count - len(df)
        if duplicates_removed > 0:
            logger.info(f"   Removed {duplicates_removed} duplicates")
        
        # Handle missing values
        missing_before = df.isnull().sum().sum()
        df = df.dropna()
        if missing_before > 0:
            logger.info(f"   Removed {missing_before} missing values")
        
        # Convert timestamp to datetime
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.sort_values('timestamp')
        
        # Remove invalid prices and quantities
        df = df[df['price'] > 0]
        df = df[df['quantity'] > 0]
        
        # Remove price outliers (more than 3 standard deviations from mean)
        if len(df) > 0:
            price_mean = df['price'].mean()
            price_std = df['price'].std()
            if price_std > 0:  # Avoid division by zero
                df = df[abs(df['price'] - price_mean) <= 3 * price_std]
        
        final_count = len(df)
        logger.info(f"   Final records: {final_count}")
        return df
    
    def validate_data(self, df: pd.DataFrame, exchange: str, symbol: str) -> bool:
        """
        Validate data quality and structure.
        
        Args:
            df: DataFrame to validate
            exchange: Exchange name for logging
            symbol: Symbol name for logging
            
        Returns:
            True if data passes validation, False otherwise
        """
        logger.info(f"🔍 Validating {exchange}/{symbol}...")
        
        # Check required columns
        missing_columns = [col for col in self.required_columns if col not in df.columns]
        if missing_columns:
            logger.error(f"   ❌ Missing columns: {missing_columns}")
            return False
        
        # Check data types
        if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
            logger.error(f"   ❌ Timestamp column is not datetime")
            return False
            
        if not pd.api.types.is_numeric_dtype(df['price']):
            logger.error(f"   ❌ Price column is not numeric")
            return False
            
        if not pd.api.types.is_numeric_dtype(df['quantity']):
            logger.error(f"   ❌ Quantity column is not numeric")
            return False
        
        # Check for reasonable price ranges
        if df['price'].min() <= 0:
            logger.error(f"   ❌ Invalid prices found (<= 0)")
            return False
            
        if df['price'].max() > self.max_price:
            logger.error(f"   ❌ Suspiciously high prices found")
            return False
        
        # Check timestamp range
        if len(df) > 0:
            time_range = df['timestamp'].max() - df['timestamp'].min()
            if time_range > self.max_time_range:
                logger.warning(f"   ⚠️  Large time range: {time_range}")
        
        logger.info(f"   ✅ Data validation passed")
        return True
    
    def synchronize_time(self, exchange_data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """
        Synchronize timestamps across all symbols in an exchange.
        
        Args:
            exchange_data: Dictionary of symbol -> DataFrame mappings
            
        Returns:
            Dictionary with synchronized data
        """
        logger.info("🕐 Synchronizing timestamps...")
        
        # Find common time range across all symbols
        all_timestamps = []
        for symbol, df in exchange_data.items():
            if len(df) > 0:
                all_timestamps.extend(df['timestamp'].tolist())
        
        if not all_timestamps:
            logger.warning("No timestamps found for synchronization")
            return exchange_data
            
        min_time = min(all_timestamps)
        max_time = max(all_timestamps)
        
        logger.info(f"   Time range: {min_time} to {max_time}")
        
        # Filter all data to common time range
        synchronized_data = {}
        for symbol, df in exchange_data.items():
            if len(df) > 0:
                mask = (df['timestamp'] >= min_time) & (df['timestamp'] <= max_time)
                synchronized_data[symbol] = df[mask].copy()
                logger.info(f"   {symbol}: {len(synchronized_data[symbol])} records")
            else:
                synchronized_data[symbol] = df
        
        return synchronized_data
    
    def process_exchange(self, exchange: str) -> Dict[str, pd.DataFrame]:
        """
        Process all data for a specific exchange.
        
        Args:
            exchange: Exchange name to process
            
        Returns:
            Dictionary of processed data
        """
        logger.info(f"\n🔄 Processing {exchange} data...")
        logger.info("=" * 50)
        
        # Load raw data
        raw_data = self.load_exchange_data(exchange)
        if not raw_data:
            logger.error(f"❌ No data found for {exchange}")
            return {}
        
        # Clean and validate data
        cleaned_data = {}
        for symbol, df in raw_data.items():
            cleaned_df = self.clean_data(df, exchange, symbol)
            if self.validate_data(cleaned_df, exchange, symbol):
                cleaned_data[symbol] = cleaned_df
            else:
                logger.error(f"❌ Validation failed for {exchange}/{symbol}")
        
        # Synchronize timestamps
        synchronized_data = self.synchronize_time(cleaned_data)
        
        # Save processed data
        self._save_processed_data(exchange, synchronized_data)
        
        return synchronized_data
    
    def _save_processed_data(self, exchange: str, data: Dict[str, pd.DataFrame]):
        """
        Save processed data to CSV files.
        
        Args:
            exchange: Exchange name
            data: Dictionary of processed DataFrames
        """
        exchange_dir = f"{self.processed_dir}/{exchange}"
        os.makedirs(exchange_dir, exist_ok=True)
        
        for symbol, df in data.items():
            filename = f"{exchange_dir}/{symbol}.csv"
            df.to_csv(filename, index=False)
            logger.info(f"💾 Saved processed {exchange}/{symbol}: {len(df)} records")
    
    def process_all_data(self) -> Dict[str, Dict[str, pd.DataFrame]]:
        """
        Process data for all exchanges.
        
        Returns:
            Dictionary containing all processed data
        """
        logger.info("🚀 Starting data processing pipeline...")
        logger.info("=" * 60)
        
        exchanges = ["binance", "coinbase", "kraken"]
        all_processed_data = {}
        
        for exchange in exchanges:
            processed_data = self.process_exchange(exchange)
            all_processed_data[exchange] = processed_data
        
        logger.info("\n✅ Data processing completed!")
        logger.info(f"📁 Processed data saved in: {self.processed_dir}/")
        
        return all_processed_data


def main():
    """
    Main function to process all market data.
    
    Processes historical market data for BTC, ETH, and SOL across
    Binance, Coinbase, and Kraken exchanges with comprehensive
    cleaning, validation, and synchronization.
    """
    processor = DataProcessor()
    processed_data = processor.process_all_data()
    
    # Print processing summary
    print("\n📊 Processing Summary:")
    print("=" * 30)
    for exchange, data in processed_data.items():
        print(f"{exchange.upper()}:")
        for symbol, df in data.items():
            print(f"  {symbol}: {len(df)} records")
    
    return processed_data


if __name__ == "__main__":
    main()