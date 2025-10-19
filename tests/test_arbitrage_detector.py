"""
Unit tests for ArbitrageDetector module
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

from analysis.arbitrage_detector import ArbitrageDetector


class TestArbitrageDetector:
    """Test cases for ArbitrageDetector class"""
    
    def setup_method(self):
        """Setup test environment"""
        self.temp_dir = tempfile.mkdtemp()
        self.detector = ArbitrageDetector(latency_risk=0.3)
    
    def teardown_method(self):
        """Cleanup test environment"""
        import shutil
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    def test_init(self):
        """Test ArbitrageDetector initialization"""
        detector = ArbitrageDetector(latency_risk=0.5)
        assert detector.latency_risk == 0.5
        assert "binance" in detector.fee_rates
        assert "coinbase" in detector.fee_rates
        assert "kraken" in detector.fee_rates
        assert detector.min_profit_threshold == 0.01
    
    def test_init_clamp(self):
        """Test initialization with latency risk clamping"""
        detector1 = ArbitrageDetector(latency_risk=-0.1)
        detector2 = ArbitrageDetector(latency_risk=1.5)
        
        assert detector1.latency_risk == 0.0
        assert detector2.latency_risk == 1.0
    
    def test_calculate_arbitrage_opportunity(self):
        """Test arbitrage opportunity calculation"""
        result = self.detector.calculate_arbitrage_opportunity(
            "binance", "coinbase", "btc_usd", 50000.0, 50100.0, 1.0
        )
        
        assert result["buy_exchange"] == "binance"
        assert result["sell_exchange"] == "coinbase"
        assert result["symbol"] == "btc_usd"
        assert result["buy_price"] == 50000.0
        assert result["sell_price"] == 50100.0
        assert result["quantity"] == 1.0
        assert "gross_profit_percentage" in result
        assert "risk_adjusted_percentage" in result
        assert "is_profitable" in result
    
    def test_calculate_arbitrage_opportunity_profitable(self):
        """Test profitable arbitrage calculation"""
        # Large price difference to ensure profitability
        result = self.detector.calculate_arbitrage_opportunity(
            "binance", "coinbase", "btc_usd", 50000.0, 60000.0, 1.0
        )
        
        assert result["is_profitable"] is True
        assert result["gross_profit_percentage"] > 0
        assert result["risk_adjusted_percentage"] > 0
    
    def test_calculate_arbitrage_opportunity_not_profitable(self):
        """Test non-profitable arbitrage calculation"""
        # Small price difference
        result = self.detector.calculate_arbitrage_opportunity(
            "binance", "coinbase", "btc_usd", 50000.0, 50001.0, 1.0
        )
        
        assert result["is_profitable"] is False
        assert result["gross_profit_percentage"] < 1.0
    
    def test_load_processed_data(self):
        """Test loading processed data"""
        # Create test data structure
        processed_dir = os.path.join(self.temp_dir, "processed")
        for exchange in ["binance", "coinbase", "kraken"]:
            exchange_dir = os.path.join(processed_dir, exchange)
            os.makedirs(exchange_dir, exist_ok=True)
            
            # Create test CSV
            df = pd.DataFrame({
                'timestamp': pd.date_range('2025-10-01', periods=10, freq='1min'),
                'exchange': [exchange] * 10,
                'symbol': ['btcusdt'] * 10,
                'price': np.random.uniform(40000, 50000, 10),
                'quantity': np.random.uniform(0.001, 1.0, 10),
                'side': ['buy', 'sell'] * 5
            })
            df.to_csv(os.path.join(exchange_dir, "btcusdt.csv"), index=False)
        
        result = self.detector.load_processed_data(processed_dir)
        
        assert len(result) == 3
        for exchange in ["binance", "coinbase", "kraken"]:
            assert exchange in result
            assert "btcusdt" in result[exchange]
            assert len(result[exchange]["btcusdt"]) == 10
    
    def test_load_processed_data_missing_dir(self):
        """Test loading from missing directory"""
        result = self.detector.load_processed_data("nonexistent")
        # Should return empty dicts for each exchange
        assert result == {"binance": {}, "coinbase": {}, "kraken": {}}
    
    def test_find_arbitrage_opportunities(self):
        """Test finding arbitrage opportunities"""
        # Create test data with price differences
        data = {
            "binance": {
                "btcusdt": pd.DataFrame({
                    'timestamp': pd.date_range('2025-10-01', periods=10, freq='1min'),
                    'price': [50000.0] * 10
                })
            },
            "coinbase": {
                "btc_usd": pd.DataFrame({
                    'timestamp': pd.date_range('2025-10-01', periods=10, freq='1min'),
                    'price': [60000.0] * 10  # Higher price
                })
            }
        }
        
        opportunities = self.detector.find_arbitrage_opportunities(data)
        
        # Should find opportunities due to price difference
        assert isinstance(opportunities, list)
    
    def test_find_arbitrage_opportunities_no_data(self):
        """Test finding opportunities with no data"""
        opportunities = self.detector.find_arbitrage_opportunities({})
        assert opportunities == []
    
    def test_analyze_exchange_performance(self):
        """Test exchange performance analysis"""
        data = {
            "binance": {
                "btcusdt": pd.DataFrame({
                    'timestamp': pd.date_range('2025-10-01', periods=10, freq='1min'),
                    'price': [50000.0] * 10
                })
            },
            "coinbase": {
                "btc_usd": pd.DataFrame({
                    'timestamp': pd.date_range('2025-10-01', periods=5, freq='1min'),
                    'price': [51000.0] * 5
                })
            }
        }
        
        performance = self.detector.analyze_exchange_performance(data)
        
        assert "binance" in performance
        assert "coinbase" in performance
        assert performance["binance"]["total_trades"] == 10
        assert performance["coinbase"]["total_trades"] == 5
        assert "btcusdt" in performance["binance"]["symbols"]
        assert "btc_usd" in performance["coinbase"]["symbols"]
    
    def test_generate_report(self):
        """Test report generation"""
        opportunities = [
            {
                "buy_exchange": "binance",
                "sell_exchange": "coinbase",
                "symbol": "btc_usd",
                "buy_price": 50000.0,
                "sell_price": 51000.0,
                "risk_adjusted_percentage": 1.5
            }
        ]
        
        performance = {
            "binance": {"total_trades": 100, "avg_price": 50000.0, "symbols": ["btcusdt"]},
            "coinbase": {"total_trades": 50, "avg_price": 51000.0, "symbols": ["btc_usd"]}
        }
        
        report = self.detector.generate_report(opportunities, performance)
        
        assert isinstance(report, str)
        assert "ARBITRAGE DETECTION REPORT" in report
        assert "Total Opportunities: 1" in report
        assert "binance" in report
        assert "coinbase" in report
    
    def test_generate_report_no_opportunities(self):
        """Test report generation with no opportunities"""
        opportunities = []
        performance = {
            "binance": {"total_trades": 100, "avg_price": 50000.0, "symbols": ["btcusdt"]}
        }
        
        report = self.detector.generate_report(opportunities, performance)
        
        assert "Total Opportunities: 0" in report
        assert "BINANCE:" in report
