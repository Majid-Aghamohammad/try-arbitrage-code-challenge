"""
Unit tests for DataProcessor module
"""

import pytest
import pandas as pd
import numpy as np
import os
import tempfile
from unittest.mock import Mock, patch
import sys

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from data.data_processor import DataProcessor


class TestDataProcessor:
    """Test cases for DataProcessor class"""
    
    def setup_method(self):
        """Setup test environment"""
        self.temp_dir = tempfile.mkdtemp()
        self.processor = DataProcessor(data_dir=self.temp_dir)
        self.processor.processed_dir = os.path.join(self.temp_dir, "processed")
    
    def teardown_method(self):
        """Cleanup test environment"""
        import shutil
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    def test_init(self):
        """Test DataProcessor initialization"""
        processor = DataProcessor(data_dir="test_data")
        assert processor.data_dir == "test_data"
        assert processor.processed_dir == "data/processed/test_data"
        assert len(processor.required_columns) == 6
        assert processor.max_price == 1000000
    
    def create_test_dataframe(self):
        """Create test DataFrame"""
        data = {
            'timestamp': pd.date_range('2025-10-01', periods=100, freq='1min'),
            'exchange': ['binance'] * 100,
            'symbol': ['btcusdt'] * 100,
            'price': np.random.uniform(40000, 50000, 100),
            'quantity': np.random.uniform(0.001, 1.0, 100),
            'side': ['buy', 'sell'] * 50
        }
        return pd.DataFrame(data)
    
    def test_clean_data(self):
        """Test data cleaning"""
        df = self.create_test_dataframe()
        
        # Add some duplicates and missing values
        df = pd.concat([df, df.iloc[:5]])  # Add duplicates
        df.loc[10:15, 'price'] = np.nan  # Add missing values
        df.loc[20:25, 'price'] = -1000  # Add negative prices
        
        cleaned_df = self.processor.clean_data(df, "binance", "btcusdt")
        
        assert len(cleaned_df) < len(df)
        assert cleaned_df['price'].min() > 0
        assert not cleaned_df['price'].isna().any()
    
    def test_clean_data_empty(self):
        """Test cleaning empty DataFrame"""
        df = pd.DataFrame()
        # Add required columns to avoid KeyError
        df['price'] = pd.Series(dtype='float64')
        df['quantity'] = pd.Series(dtype='float64')
        cleaned_df = self.processor.clean_data(df, "binance", "btcusdt")
        assert len(cleaned_df) == 0
    
    def test_validate_data_valid(self):
        """Test validation with valid data"""
        df = self.create_test_dataframe()
        result = self.processor.validate_data(df, "binance", "btcusdt")
        assert result is True
    
    def test_validate_data_missing_columns(self):
        """Test validation with missing columns"""
        df = pd.DataFrame({'timestamp': [1, 2, 3]})
        result = self.processor.validate_data(df, "binance", "btcusdt")
        assert result is False
    
    def test_validate_data_invalid_price(self):
        """Test validation with invalid prices"""
        df = self.create_test_dataframe()
        df.loc[0, 'price'] = -1000  # Negative price
        result = self.processor.validate_data(df, "binance", "btcusdt")
        assert result is False
    
    def test_validate_data_high_price(self):
        """Test validation with suspiciously high prices"""
        df = self.create_test_dataframe()
        df.loc[0, 'price'] = 2000000  # Too high
        result = self.processor.validate_data(df, "binance", "btcusdt")
        assert result is False
    
    def test_synchronize_time(self):
        """Test time synchronization"""
        # Create test data with different time ranges
        data1 = self.create_test_dataframe()
        data2 = self.create_test_dataframe()
        # Create data2 with same length but different timestamps
        data2['timestamp'] = pd.date_range('2025-10-01T01:00:00', periods=100, freq='1min')
        
        exchange_data = {
            'btcusdt': data1,
            'ethusdt': data2
        }
        
        synchronized = self.processor.synchronize_time(exchange_data)
        
        assert 'btcusdt' in synchronized
        assert 'ethusdt' in synchronized
        assert len(synchronized['btcusdt']) > 0
        assert len(synchronized['ethusdt']) > 0
    
    def test_synchronize_time_empty(self):
        """Test synchronization with empty data"""
        synchronized = self.processor.synchronize_time({})
        assert synchronized == {}
    
    def test_load_exchange_data(self):
        """Test loading exchange data"""
        # Create test CSV file
        exchange_dir = os.path.join(self.temp_dir, "binance")
        os.makedirs(exchange_dir, exist_ok=True)
        
        df = self.create_test_dataframe()
        df.to_csv(os.path.join(exchange_dir, "btcusdt.csv"), index=False)
        
        result = self.processor.load_exchange_data("binance")
        
        assert "btcusdt" in result
        assert len(result["btcusdt"]) == 100
    
    def test_load_exchange_data_missing_dir(self):
        """Test loading from missing directory"""
        result = self.processor.load_exchange_data("nonexistent")
        assert result == {}
    
    def test_save_processed_data(self):
        """Test saving processed data"""
        df = self.create_test_dataframe()
        data = {"btcusdt": df}
        
        self.processor._save_processed_data("binance", data)
        
        expected_file = os.path.join(self.processor.processed_dir, "binance", "btcusdt.csv")
        assert os.path.exists(expected_file)
        
        # Verify content
        loaded_df = pd.read_csv(expected_file)
        assert len(loaded_df) == 100
    
    def test_process_exchange(self):
        """Test processing single exchange"""
        # Create test data
        exchange_dir = os.path.join(self.temp_dir, "binance")
        os.makedirs(exchange_dir, exist_ok=True)
        
        df = self.create_test_dataframe()
        df.to_csv(os.path.join(exchange_dir, "btcusdt.csv"), index=False)
        
        result = self.processor.process_exchange("binance")
        
        assert "btcusdt" in result
        assert len(result["btcusdt"]) > 0
    
    def test_process_exchange_no_data(self):
        """Test processing exchange with no data"""
        result = self.processor.process_exchange("nonexistent")
        assert result == {}
    
    def test_process_all_data(self):
        """Test processing all data"""
        # Create test data for all exchanges
        for exchange in ["binance", "coinbase", "kraken"]:
            exchange_dir = os.path.join(self.temp_dir, exchange)
            os.makedirs(exchange_dir, exist_ok=True)
            
            df = self.create_test_dataframe()
            df.to_csv(os.path.join(exchange_dir, "btcusdt.csv"), index=False)
        
        result = self.processor.process_all_data()
        
        assert len(result) == 3
        for exchange in ["binance", "coinbase", "kraken"]:
            assert exchange in result
            assert "btcusdt" in result[exchange]
