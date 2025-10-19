"""
Unit tests for DataDownloader module
"""

import pytest
import pandas as pd
import os
import tempfile
from unittest.mock import Mock, patch, AsyncMock
import sys

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from data.data_downloader import DataDownloader


class TestDataDownloader:
    """Test cases for DataDownloader class"""
    
    def setup_method(self):
        """Setup test environment"""
        self.temp_dir = tempfile.mkdtemp()
        self.downloader = DataDownloader(date="2025-10-01")
        self.downloader.data_dir = self.temp_dir
    
    def teardown_method(self):
        """Cleanup test environment"""
        import shutil
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    def test_init(self):
        """Test DataDownloader initialization"""
        downloader = DataDownloader(date="2025-10-01")
        assert downloader.date == "2025-10-01"
        assert downloader.data_dir == "data/2025-10-01"
        assert "binance" in downloader.exchanges
        assert "coinbase" in downloader.exchanges
        assert "kraken" in downloader.exchanges
    
    def test_process_binance_message(self):
        """Test Binance message processing"""
        message = {
            "data": {
                "s": "BTCUSDT",
                "p": "50000.00",
                "q": "0.001",
                "m": False,
                "t": 12345
            }
        }
        timestamp = "2025-10-01T12:00:00"
        
        result = self.downloader._process_binance_message(message, timestamp)
        
        assert result is not None
        assert result["exchange"] == "binance"
        assert result["symbol"] == "BTCUSDT"
        assert result["price"] == 50000.00
        assert result["quantity"] == 0.001
        assert result["side"] == "buy"
        assert result["trade_id"] == 12345
    
    def test_process_coinbase_message(self):
        """Test Coinbase message processing"""
        message = {
            "product_id": "BTC-USD",
            "price": "50000.00",
            "size": "0.001",
            "side": "buy",
            "trade_id": 12345
        }
        timestamp = "2025-10-01T12:00:00"
        
        result = self.downloader._process_coinbase_message(message, timestamp)
        
        assert result is not None
        assert result["exchange"] == "coinbase"
        assert result["symbol"] == "BTC-USD"
        assert result["price"] == 50000.00
        assert result["quantity"] == 0.001
        assert result["side"] == "buy"
        assert result["trade_id"] == 12345
    
    def test_process_kraken_message(self):
        """Test Kraken message processing"""
        message = [123, [[50000.00, 0.001, 1633084800.0, 'b', 'market', '']], 'trade', 'XBT/USD']
        timestamp = "2025-10-01T12:00:00"
        
        result = self.downloader._process_kraken_message(message, timestamp)
        
        assert result is not None
        assert result["exchange"] == "kraken"
        assert result["symbol"] == "XBT/USD"
        assert result["price"] == 50000.00
        assert result["quantity"] == 0.001
        assert result["side"] == "buy"
        assert result["trade_id"] == 123
    
    def test_process_message_unknown_exchange(self):
        """Test processing message from unknown exchange"""
        message = {"test": "data"}
        timestamp = "2025-10-01T12:00:00"
        
        result = self.downloader._process_message("unknown", message, timestamp)
        
        assert result is None
    
    def test_save_data_to_csv(self):
        """Test saving data to CSV"""
        data = [
            {
                "timestamp": "2025-10-01T12:00:00",
                "exchange": "binance",
                "symbol": "BTCUSDT",
                "price": 50000.00,
                "quantity": 0.001,
                "side": "buy",
                "trade_id": 12345
            }
        ]
        
        filename = self.downloader._save_data_to_csv("binance", "btcusdt", data)
        
        assert filename is not None
        assert os.path.exists(filename)
        
        # Verify CSV content
        df = pd.read_csv(filename)
        assert len(df) == 1
        assert df.iloc[0]["price"] == 50000.00
    
    @pytest.mark.asyncio
    async def test_download_exchange_data_no_data(self):
        """Test downloading when no data is available"""
        with patch.object(self.downloader.tardis_client, 'replay') as mock_replay:
            mock_replay.return_value = AsyncMock()
            mock_replay.return_value.__aiter__.return_value = []
            
            result = await self.downloader.download_exchange_data("binance", "btcusdt", 60)
            
            assert result is None
    
    @pytest.mark.asyncio
    async def test_download_exchange_data_with_data(self):
        """Test downloading with actual data"""
        mock_message = {
            "data": {
                "s": "BTCUSDT",
                "p": "50000.00",
                "q": "0.001",
                "m": False,
                "t": 12345
            }
        }
        
        with patch.object(self.downloader.tardis_client, 'replay') as mock_replay:
            mock_replay.return_value = AsyncMock()
            mock_replay.return_value.__aiter__.return_value = [("2025-10-01T12:00:00", mock_message)]
            
            result = await self.downloader.download_exchange_data("binance", "btcusdt", 60)
            
            assert result is not None
            assert os.path.exists(result)
    
    @pytest.mark.asyncio
    async def test_download_exchange_data_exception(self):
        """Test downloading with exception"""
        with patch.object(self.downloader.tardis_client, 'replay') as mock_replay:
            mock_replay.side_effect = Exception("API Error")
            
            result = await self.downloader.download_exchange_data("binance", "btcusdt", 60)
            
            assert result is None
    
    @pytest.mark.asyncio
    async def test_download_all_data(self):
        """Test downloading all data"""
        with patch.object(self.downloader, 'download_exchange_data') as mock_download:
            mock_download.return_value = "test_file.csv"
            
            result = await self.downloader.download_all_data(60)
            
            assert len(result) == 9  # 3 exchanges * 3 symbols
            assert all(filename == "test_file.csv" for filename in result)
