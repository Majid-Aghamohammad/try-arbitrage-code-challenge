"""
Unit tests for TriangularArbitrageDetector module
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

from analysis.triangular_arbitrage import TriangularArbitrageDetector


class TestTriangularArbitrageDetector:
    """Test cases for TriangularArbitrageDetector class"""
    
    def setup_method(self):
        """Setup test environment"""
        self.temp_dir = tempfile.mkdtemp()
        self.detector = TriangularArbitrageDetector(latency_risk=0.3)
    
    def teardown_method(self):
        """Cleanup test environment"""
        import shutil
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    def test_init(self):
        """Test TriangularArbitrageDetector initialization"""
        detector = TriangularArbitrageDetector(latency_risk=0.5)
        assert detector.latency_risk == 0.5
        assert "binance" in detector.fee_rates
        assert "coinbase" in detector.fee_rates
        assert "kraken" in detector.fee_rates
        assert detector.min_profit_threshold == 0.01
        assert len(detector.triangular_paths) == 3
    
    def test_init_clamp(self):
        """Test initialization with latency risk clamping"""
        detector1 = TriangularArbitrageDetector(latency_risk=-0.1)
        detector2 = TriangularArbitrageDetector(latency_risk=1.5)
        
        assert detector1.latency_risk == 0.0
        assert detector2.latency_risk == 1.0
    
    def test_calculate_triangular_arbitrage(self):
        """Test triangular arbitrage calculation"""
        prices = {
            "btc_usd": 50000.0,
            "eth_usd": 3000.0,
            "sol_usd": 200.0
        }
        
        result = self.detector.calculate_triangular_arbitrage(
            "binance", ["btc", "eth", "sol"], prices
        )
        
        assert result is not None
        assert result["exchange"] == "binance"
        assert result["path"] == ["btc", "eth", "sol"]
        assert result["start_crypto"] == "btc"
        assert "gross_profit_percentage" in result
        assert "fee_impact" in result
        assert "net_profit_percentage" in result
        assert "risk_adjusted_percentage" in result
        assert "is_profitable" in result
        assert "rates" in result
    
    def test_calculate_triangular_arbitrage_invalid_path(self):
        """Test triangular arbitrage with invalid path"""
        prices = {"btc_usd": 50000.0}
        
        result = self.detector.calculate_triangular_arbitrage(
            "binance", ["btc"], prices
        )
        
        assert result is None
    
    def test_calculate_triangular_arbitrage_missing_prices(self):
        """Test triangular arbitrage with missing prices"""
        prices = {"btc_usd": 50000.0}  # Missing ETH and SOL prices
        
        result = self.detector.calculate_triangular_arbitrage(
            "binance", ["btc", "eth", "sol"], prices
        )
        
        assert result is None
    
    def test_calculate_triangular_arbitrage_zero_prices(self):
        """Test triangular arbitrage with zero prices"""
        prices = {
            "btc_usd": 0.0,  # Zero price
            "eth_usd": 3000.0,
            "sol_usd": 200.0
        }
        
        result = self.detector.calculate_triangular_arbitrage(
            "binance", ["btc", "eth", "sol"], prices
        )
        
        assert result is None
    
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
    
    def test_find_triangular_opportunities(self):
        """Test finding triangular arbitrage opportunities"""
        # Create test data with price differences
        data = {
            "binance": {
                "btcusdt": pd.DataFrame({
                    'timestamp': pd.date_range('2025-10-01', periods=10, freq='1min'),
                    'price': [50000.0] * 10
                }),
                "ethusdt": pd.DataFrame({
                    'timestamp': pd.date_range('2025-10-01', periods=10, freq='1min'),
                    'price': [3000.0] * 10
                }),
                "solusdt": pd.DataFrame({
                    'timestamp': pd.date_range('2025-10-01', periods=10, freq='1min'),
                    'price': [200.0] * 10
                })
            }
        }
        
        opportunities = self.detector.find_triangular_opportunities(data)
        
        assert isinstance(opportunities, list)
    
    def test_find_triangular_opportunities_no_data(self):
        """Test finding opportunities with no data"""
        opportunities = self.detector.find_triangular_opportunities({})
        assert opportunities == []
    
    def test_find_triangular_opportunities_missing_prices(self):
        """Test finding opportunities with missing prices"""
        data = {
            "binance": {
                "btcusdt": pd.DataFrame({
                    'timestamp': pd.date_range('2025-10-01', periods=10, freq='1min'),
                    'price': [50000.0] * 10
                })
                # Missing ETH and SOL data
            }
        }
        
        opportunities = self.detector.find_triangular_opportunities(data)
        
        assert opportunities == []
    
    def test_analyze_triangular_performance(self):
        """Test triangular arbitrage performance analysis"""
        data = {
            "binance": {
                "btcusdt": pd.DataFrame({
                    'timestamp': pd.date_range('2025-10-01', periods=10, freq='1min'),
                    'price': [50000.0] * 10
                }),
                "ethusdt": pd.DataFrame({
                    'timestamp': pd.date_range('2025-10-01', periods=5, freq='1min'),
                    'price': [3000.0] * 5
                })
            }
        }
        
        performance = self.detector.analyze_triangular_performance(data)
        
        assert "binance" in performance
        assert performance["binance"]["total_trades"] == 15
        assert "btcusdt" in performance["binance"]["symbols"]
        assert "ethusdt" in performance["binance"]["symbols"]
        assert "btcusdt" in performance["binance"]["avg_prices"]
        assert "ethusdt" in performance["binance"]["avg_prices"]
    
    def test_generate_triangular_report(self):
        """Test triangular arbitrage report generation"""
        opportunities = [
            {
                "exchange": "binance",
                "path": ["btc", "eth", "sol"],
                "start_crypto": "btc",
                "gross_profit_percentage": 2.0,
                "fee_impact": 0.3,
                "net_profit_percentage": 1.7,
                "risk_adjusted_percentage": 1.2
            }
        ]
        
        performance = {
            "binance": {"total_trades": 100, "symbols": ["btcusdt", "ethusdt", "solusdt"]}
        }
        
        report = self.detector.generate_triangular_report(opportunities, performance)
        
        assert isinstance(report, str)
        assert "TRIANGULAR ARBITRAGE DETECTION REPORT" in report
        assert "Total Opportunities: 1" in report
        assert "BINANCE:" in report
    
    def test_generate_triangular_report_no_opportunities(self):
        """Test report generation with no opportunities"""
        opportunities = []
        performance = {
            "binance": {"total_trades": 100, "symbols": ["btcusdt"]}
        }
        
        report = self.detector.generate_triangular_report(opportunities, performance)
        
        assert "Total Opportunities: 0" in report
        assert "No triangular arbitrage opportunities found" in report
        assert "This is normal in efficient markets" in report
