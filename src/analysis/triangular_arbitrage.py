"""
Triangular Arbitrage Detection Module

This module handles detection of triangular arbitrage opportunities
across multiple exchanges and cryptocurrencies.

Author: Arbitrage Detection Team
Date: 2025-10-19
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TriangularArbitrageDetector:
    """
    Detects triangular arbitrage opportunities across exchanges.
    
    Triangular arbitrage involves three trades:
    1. Buy crypto A with crypto B
    2. Buy crypto C with crypto A  
    3. Buy crypto B with crypto C
    
    This class finds profitable triangular paths considering
    fees, latency risks, and market conditions.
    
    Attributes:
        fee_rates: Dictionary of fee rates for each exchange
        latency_risk: Risk factor for latency (0.0 to 1.0)
        min_profit_threshold: Minimum profit percentage to consider
    """
    
    def __init__(self, latency_risk: float = 0.1):
        """
        Initialize the TriangularArbitrageDetector.
        
        Args:
            latency_risk: Risk factor for latency (0.0 to 1.0)
                         Higher values mean more conservative
                         Default: 0.3 (300ms latency)
        """
        self.latency_risk = max(0.0, min(1.0, latency_risk))  # Clamp between 0 and 1
        
        # Fee rates for each exchange (maker/taker fees)
        self.fee_rates = {
            "binance": {"maker": 0.001, "taker": 0.001},  # 0.1%
            "coinbase": {"maker": 0.005, "taker": 0.005},  # 0.5%
            "kraken": {"maker": 0.0016, "taker": 0.0026}   # 0.16% / 0.26%
        }
        
        # Minimum profit threshold (percentage)
        self.min_profit_threshold = 0.01  # 1%
        
        # Triangular paths to check
        self.triangular_paths = [
            ['btc', 'eth', 'sol'],  # BTC -> ETH -> SOL -> BTC
            ['eth', 'sol', 'btc'],  # ETH -> SOL -> BTC -> ETH
            ['sol', 'btc', 'eth'],  # SOL -> BTC -> ETH -> SOL
        ]
        
        logger.info(f"TriangularArbitrageDetector initialized with latency_risk: {self.latency_risk}")
    
    def load_processed_data(self, processed_dir: str = "data/processed") -> Dict[str, Dict[str, pd.DataFrame]]:
        """
        Load processed data from all exchanges for a specific date.
        
        Args:
            processed_dir: Directory containing processed data for specific date
                          Format: "data/processed/YYYY-MM-DD"
            
        Returns:
            Dictionary of exchange -> symbol -> DataFrame mappings
        """
        logger.info(f"📂 Loading processed data for triangular arbitrage from: {processed_dir}")
        
        exchanges = ["binance", "coinbase", "kraken"]
        all_data = {}
        
        for exchange in exchanges:
            exchange_dir = f"{processed_dir}/{exchange}"
            exchange_data = {}
            
            try:
                for filename in os.listdir(exchange_dir):
                    if filename.endswith('.csv'):
                        symbol = filename.replace('.csv', '')
                        filepath = f"{exchange_dir}/{filename}"
                        
                        df = pd.read_csv(filepath)
                        df['timestamp'] = pd.to_datetime(df['timestamp'])
                        exchange_data[symbol] = df
                        
                        logger.info(f"✅ Loaded {exchange}/{symbol}: {len(df)} records")
                
                all_data[exchange] = exchange_data
                
            except FileNotFoundError:
                logger.error(f"❌ Processed data not found for {exchange} at {exchange_dir}")
                all_data[exchange] = {}
        
        return all_data
    
    def calculate_triangular_arbitrage(
        self,
        exchange: str,
        path: List[str],
        prices: Dict[str, float]
    ) -> Optional[Dict[str, float]]:
        """
        Calculate triangular arbitrage for a specific path.
        
        Args:
            exchange: Exchange name
            path: List of cryptocurrencies in order [A, B, C]
            prices: Dictionary of current prices
            
        Returns:
            Dictionary with arbitrage calculations or None
        """
        if len(path) != 3:
            return None
            
        crypto_a, crypto_b, crypto_c = path
        
        # Get prices
        price_a_usd = prices.get(f"{crypto_a}_usd", 0)
        price_b_usd = prices.get(f"{crypto_b}_usd", 0)
        price_c_usd = prices.get(f"{crypto_c}_usd", 0)
        
        if price_a_usd == 0 or price_b_usd == 0 or price_c_usd == 0:
            return None
        
        # Calculate exchange rates
        rate_a_to_b = price_a_usd / price_b_usd  # How many B for 1 A
        rate_b_to_c = price_b_usd / price_c_usd  # How many C for 1 B
        rate_c_to_a = price_c_usd / price_a_usd  # How many A for 1 C
        
        # Calculate triangular arbitrage
        # Start with 1 unit of A
        # Step 1: A -> B (get rate_a_to_b units of B)
        # Step 2: B -> C (get rate_a_to_b * rate_b_to_c units of C)
        # Step 3: C -> A (get rate_a_to_b * rate_b_to_c * rate_c_to_a units of A)
        
        final_a_units = rate_a_to_b * rate_b_to_c * rate_c_to_a
        profit_percentage = (final_a_units - 1) * 100
        
        # Apply fees (3 trades)
        fee_rate = self.fee_rates.get(exchange, {}).get("taker", 0.001)
        total_fee_impact = 3 * fee_rate * 100  # 3 trades
        
        # Apply latency risk
        net_profit = profit_percentage - total_fee_impact
        risk_adjusted_profit = net_profit * (1 - self.latency_risk)
        
        return {
            "exchange": exchange,
            "path": path,
            "start_crypto": crypto_a,
            "gross_profit_percentage": profit_percentage,
            "fee_impact": total_fee_impact,
            "net_profit_percentage": net_profit,
            "risk_adjusted_percentage": risk_adjusted_profit,
            "is_profitable": risk_adjusted_profit > self.min_profit_threshold,
            "rates": {
                f"{crypto_a}_to_{crypto_b}": rate_a_to_b,
                f"{crypto_b}_to_{crypto_c}": rate_b_to_c,
                f"{crypto_c}_to_{crypto_a}": rate_c_to_a
            }
        }
    
    def find_triangular_opportunities(
        self, 
        data: Dict[str, Dict[str, pd.DataFrame]]
    ) -> List[Dict[str, float]]:
        """
        Find all triangular arbitrage opportunities.
        
        Args:
            data: Processed data from all exchanges
            
        Returns:
            List of triangular arbitrage opportunities
        """
        logger.info("🔍 Searching for triangular arbitrage opportunities...")
        
        opportunities = []
        exchanges = list(data.keys())
        
        # Symbol mapping for different exchanges
        symbol_mapping = {
            'btc': ['btcusdt', 'btc_usd', 'xbt_usd'],
            'eth': ['ethusdt', 'eth_usd', 'eth_usd'],
            'sol': ['solusdt', 'sol_usd', 'sol_usd']
        }
        
        for exchange in exchanges:
            logger.info(f"Analyzing {exchange} for triangular arbitrage...")
            
            # Get current prices for this exchange
            prices = {}
            for crypto, symbols in symbol_mapping.items():
                for symbol in symbols:
                    if symbol in data[exchange] and len(data[exchange][symbol]) > 0:
                        latest_price = data[exchange][symbol]['price'].iloc[-1]
                        prices[f"{crypto}_usd"] = latest_price
                        break
            
            # Check if we have all required prices
            required_prices = ['btc_usd', 'eth_usd', 'sol_usd']
            if not all(price in prices for price in required_prices):
                logger.warning(f"Missing prices for {exchange}")
                continue
            
            # Test each triangular path
            for path in self.triangular_paths:
                opportunity = self.calculate_triangular_arbitrage(exchange, path, prices)
                
                if opportunity and opportunity['is_profitable']:
                    opportunities.append(opportunity)
                    logger.info(f"  ✅ Found triangular opportunity: {path} - {opportunity['risk_adjusted_percentage']:.2f}%")
                    print(f"🔺 TRIANGULAR ARBITRAGE FOUND!")
                    path_str = " → ".join(opportunity['path']) + " → " + opportunity['start_crypto']
                    print(f"   {opportunity['exchange'].upper()}: {path_str}")
                    print(f"   Gross Profit: {opportunity['gross_profit_percentage']:.2f}%")
                    print(f"   Fee Impact: {opportunity['fee_impact']:.2f}%")
                    print(f"   Net Profit: {opportunity['net_profit_percentage']:.2f}%")
                    print(f"   Risk Adjusted: {opportunity['risk_adjusted_percentage']:.2f}%")
                    print(f"   Exchange Rates:")
                    for rate_name, rate_value in opportunity['rates'].items():
                        print(f"     {rate_name}: {rate_value:.6f}")
                    print()
        
        # Sort by profitability
        opportunities.sort(key=lambda x: x['risk_adjusted_percentage'], reverse=True)
        
        logger.info(f"✅ Found {len(opportunities)} triangular arbitrage opportunities")
        return opportunities
    
    def analyze_triangular_performance(self, data: Dict[str, Dict[str, pd.DataFrame]]) -> Dict[str, Dict]:
        """
        Analyze triangular arbitrage performance across exchanges.
        
        Args:
            data: Processed data from all exchanges
            
        Returns:
            Dictionary with triangular arbitrage analysis
        """
        logger.info("📊 Analyzing triangular arbitrage performance...")
        
        analysis = {}
        exchanges = list(data.keys())
        
        for exchange in exchanges:
            exchange_data = data[exchange]
            total_trades = sum(len(df) for df in exchange_data.values())
            
            # Calculate average prices
            avg_prices = {}
            for symbol, df in exchange_data.items():
                if len(df) > 0:
                    avg_prices[symbol] = df['price'].mean()
            
            analysis[exchange] = {
                "total_trades": total_trades,
                "avg_prices": avg_prices,
                "symbols": list(exchange_data.keys())
            }
        
        return analysis
    
    def generate_triangular_report(self, opportunities: List[Dict], performance: Dict) -> str:
        """
        Generate comprehensive triangular arbitrage report.
        
        Args:
            opportunities: List of triangular arbitrage opportunities
            performance: Exchange performance analysis
            
        Returns:
            Formatted report string
        """
        report = []
        report.append("=" * 60)
        report.append("🔺 TRIANGULAR ARBITRAGE DETECTION REPORT")
        report.append("=" * 60)
        
        # Summary
        report.append(f"\n📊 SUMMARY:")
        report.append(f"   Total Opportunities: {len(opportunities)}")
        report.append(f"   Latency Risk Factor: {self.latency_risk}")
        report.append(f"   Min Profit Threshold: {self.min_profit_threshold}%")
        
        # Top opportunities
        if opportunities:
            report.append(f"\n🏆 TOP TRIANGULAR ARBITRAGE OPPORTUNITIES:")
            for i, opp in enumerate(opportunities[:5], 1):
                path_str = " → ".join(opp['path']) + " → " + opp['start_crypto']
                report.append(f"   {i}. {opp['exchange'].upper()}: {path_str}")
                report.append(f"      Profit: {opp['risk_adjusted_percentage']:.2f}%")
                report.append(f"      Gross: {opp['gross_profit_percentage']:.2f}%, Fees: {opp['fee_impact']:.2f}%")
        else:
            report.append(f"\n❌ No triangular arbitrage opportunities found")
            report.append(f"   This is normal in efficient markets")
        
        # Exchange performance
        report.append(f"\n📈 EXCHANGE PERFORMANCE:")
        for exchange, stats in performance.items():
            report.append(f"   {exchange.upper()}:")
            report.append(f"      Total Trades: {stats['total_trades']:,}")
            report.append(f"      Symbols: {', '.join(stats['symbols'])}")
        
        return "\n".join(report)


def main():
    """
    Main function to run triangular arbitrage detection.
    """
    # Get latency risk from user
    try:
        latency_risk = float(input("Enter latency risk factor (0.0-1.0, default 0.3 for 300ms): ") or "0.3")
    except ValueError:
        latency_risk = 0.3
        print("Invalid input, using default latency risk: 0.3 (300ms)")
    
    # Initialize detector
    detector = TriangularArbitrageDetector(latency_risk=latency_risk)
    
    # Load data
    data = detector.load_processed_data()
    
    # Find opportunities
    opportunities = detector.find_triangular_opportunities(data)
    
    # Analyze performance
    performance = detector.analyze_triangular_performance(data)
    
    # Generate report
    report = detector.generate_triangular_report(opportunities, performance)
    print(report)
    
    return opportunities, performance


if __name__ == "__main__":
    main()
