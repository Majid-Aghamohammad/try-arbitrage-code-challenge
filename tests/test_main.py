"""
Unit tests for main script
"""

import pytest
import os
import tempfile
from unittest.mock import Mock, patch, AsyncMock
import sys

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from main import validate_date, get_user_input, download_and_process_data, detect_arbitrage


class TestMain:
    """Test cases for main script functions"""
    
    def test_validate_date_valid(self):
        """Test date validation with valid dates"""
        assert validate_date("2025-10-01") is True
        assert validate_date("2025-01-01") is True
        assert validate_date("2025-12-01") is True
    
    def test_validate_date_invalid(self):
        """Test date validation with invalid dates"""
        assert validate_date("2025-10-02") is False  # Not 1st of month
        assert validate_date("2025-10-15") is False  # Not 1st of month
        assert validate_date("invalid") is False    # Invalid format
        assert validate_date("2025-13-01") is False # Invalid month
    
    def test_validate_date_edge_cases(self):
        """Test date validation edge cases"""
        assert validate_date("2025-02-01") is True   # February 1st
        assert validate_date("2025-03-01") is True   # March 1st
        assert validate_date("2024-02-01") is True   # Leap year
    
    @patch('builtins.input')
    def test_get_user_input_valid(self, mock_input):
        """Test user input with valid data"""
        mock_input.side_effect = ["2025-10-01", "0.3", "1"]
        
        date, latency_risk, arbitrage_type = get_user_input()
        
        assert date == "2025-10-01"
        assert latency_risk == 0.3
        assert arbitrage_type == 1
    
    @patch('builtins.input')
    def test_get_user_input_defaults(self, mock_input):
        """Test user input with default values"""
        mock_input.side_effect = ["2025-10-01", "", "2"]
        
        date, latency_risk, arbitrage_type = get_user_input()
        
        assert date == "2025-10-01"
        assert latency_risk == 0.1  # Default value
        assert arbitrage_type == 2
    
    @patch('builtins.input')
    def test_get_user_input_invalid_date(self, mock_input):
        """Test user input with invalid date"""
        mock_input.side_effect = ["2025-10-02", "2025-10-01", "0.3", "1"]
        
        date, latency_risk, arbitrage_type = get_user_input()
        
        assert date == "2025-10-01"
        assert latency_risk == 0.3
        assert arbitrage_type == 1
    
    @patch('builtins.input')
    def test_get_user_input_invalid_latency(self, mock_input):
        """Test user input with invalid latency risk"""
        mock_input.side_effect = ["2025-10-01", "1.5", "0.3", "1"]
        
        date, latency_risk, arbitrage_type = get_user_input()
        
        assert date == "2025-10-01"
        assert latency_risk == 0.3
        assert arbitrage_type == 1
    
    @patch('builtins.input')
    def test_get_user_input_invalid_type(self, mock_input):
        """Test user input with invalid arbitrage type"""
        mock_input.side_effect = ["2025-10-01", "0.3", "3", "1"]
        
        date, latency_risk, arbitrage_type = get_user_input()
        
        assert date == "2025-10-01"
        assert latency_risk == 0.3
        assert arbitrage_type == 1
    
    @pytest.mark.asyncio
    async def test_download_and_process_data_success(self):
        """Test successful data download and processing"""
        with patch('main.DataDownloader') as mock_downloader_class, \
             patch('main.DataProcessor') as mock_processor_class:
            
            # Mock downloader
            mock_downloader = Mock()
            mock_downloader.download_all_data = AsyncMock(return_value=["file1.csv", "file2.csv"])
            mock_downloader_class.return_value = mock_downloader
            
            # Mock processor
            mock_processor = Mock()
            mock_processor.process_all_data.return_value = {"binance": {"btcusdt": "data"}}
            mock_processor_class.return_value = mock_processor
            
            result = await download_and_process_data("2025-10-01")
            
            assert result is True
            mock_downloader.download_all_data.assert_called_once()
            mock_processor.process_all_data.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_download_and_process_data_failure(self):
        """Test failed data download and processing"""
        with patch('main.DataDownloader') as mock_downloader_class:
            # Mock downloader failure
            mock_downloader = Mock()
            mock_downloader.download_all_data.return_value = []
            mock_downloader_class.return_value = mock_downloader
            
            result = await download_and_process_data("2025-10-01")
            
            assert result is False
    
    @pytest.mark.asyncio
    async def test_download_and_process_data_exception(self):
        """Test data download and processing with exception"""
        with patch('main.DataDownloader') as mock_downloader_class:
            # Mock downloader exception
            mock_downloader = Mock()
            mock_downloader.download_all_data.side_effect = Exception("API Error")
            mock_downloader_class.return_value = mock_downloader
            
            result = await download_and_process_data("2025-10-01")
            
            assert result is False
    
    def test_detect_arbitrage_regular(self):
        """Test regular arbitrage detection"""
        with patch('main.ArbitrageDetector') as mock_detector_class:
            # Mock detector
            mock_detector = Mock()
            mock_detector.load_processed_data.return_value = {"binance": {"btcusdt": "data"}}
            mock_detector.find_arbitrage_opportunities.return_value = []
            mock_detector.analyze_exchange_performance.return_value = {}
            mock_detector.generate_report.return_value = "Test Report"
            mock_detector_class.return_value = mock_detector
            
            result = detect_arbitrage("2025-10-01", 0.3, 1)
            
            assert result is True
            mock_detector.load_processed_data.assert_called_once()
            mock_detector.find_arbitrage_opportunities.assert_called_once()
    
    def test_detect_arbitrage_triangular(self):
        """Test triangular arbitrage detection"""
        with patch('main.TriangularArbitrageDetector') as mock_detector_class:
            # Mock detector
            mock_detector = Mock()
            mock_detector.load_processed_data.return_value = {"binance": {"btcusdt": "data"}}
            mock_detector.find_triangular_opportunities.return_value = []
            mock_detector.analyze_triangular_performance.return_value = {}
            mock_detector.generate_triangular_report.return_value = "Test Report"
            mock_detector_class.return_value = mock_detector
            
            result = detect_arbitrage("2025-10-01", 0.3, 2)
            
            assert result is True
            mock_detector.load_processed_data.assert_called_once()
            mock_detector.find_triangular_opportunities.assert_called_once()
    
    def test_detect_arbitrage_invalid_type(self):
        """Test arbitrage detection with invalid type"""
        result = detect_arbitrage("2025-10-01", 0.3, 3)
        assert result is False
    
    def test_detect_arbitrage_no_data(self):
        """Test arbitrage detection with no data"""
        with patch('main.ArbitrageDetector') as mock_detector_class:
            # Mock detector with no data
            mock_detector = Mock()
            mock_detector.load_processed_data.return_value = {}
            mock_detector_class.return_value = mock_detector
            
            result = detect_arbitrage("2025-10-01", 0.3, 1)
            
            assert result is False
    
    def test_detect_arbitrage_exception(self):
        """Test arbitrage detection with exception"""
        with patch('main.ArbitrageDetector') as mock_detector_class:
            # Mock detector with exception
            mock_detector = Mock()
            mock_detector.load_processed_data.side_effect = Exception("Data Error")
            mock_detector_class.return_value = mock_detector
            
            result = detect_arbitrage("2025-10-01", 0.3, 1)
            
            assert result is False
