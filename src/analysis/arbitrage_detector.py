"""
Arbitrage Detector Module for Arbitrage Detection System

This module handles detection of arbitrage opportunities between exchanges
with comprehensive fee and latency risk analysis.

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


class ArbitrageDetector:
    """
    Detects arbitrage opportunities between exchanges.
    
    This class provides methods to identify profitable arbitrage opportunities
    considering fees, latency risks, and market conditions.
    
    Attributes:
        fee_rates: Dictionary of fee rates for each exchange
        latency_risk: Risk factor for latency (0.0 to 1.0)
        min_profit_threshold: Minimum profit percentage to consider
    """
    
    def __init__(self, latency_risk: float = 0.1):
        """
        Initialize the ArbitrageDetector.
        
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
        
        logger.info(f"ArbitrageDetector initialized with latency_risk: {self.latency_risk}")
    
    def load_processed_data(self, processed_dir: str = "data/processed") -> Dict[str, Dict[str, pd.DataFrame]]:
        """
        Load processed data from all exchanges for a specific date.
        
        Args:
            processed_dir: Directory containing processed data for specific date
                          Format: "data/processed/YYYY-MM-DD"
            
        Returns:
            Dictionary of exchange -> symbol -> DataFrame mappings
        """
        logger.info(f"📂 Loading processed data from: {processed_dir}")
        
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
    
    def calculate_arbitrage_opportunity(
        self, 
        buy_exchange: str, 
        sell_exchange: str, 
        symbol: str,
        buy_price: float, 
        sell_price: float,
        quantity: float = 1.0
    ) -> Dict[str, float]:
        """
        Calculate arbitrage opportunity between two exchanges.
        
        Args:
            buy_exchange: Exchange to buy from
            sell_exchange: Exchange to sell to
            symbol: Trading symbol
            buy_price: Price to buy at
            sell_price: Price to sell at
            quantity: Quantity to trade
            
        Returns:
            Dictionary with arbitrage calculations
        """
        # Get fee rates
        buy_fee_rate = self.fee_rates.get(buy_exchange, {}).get("taker", 0.001)
        sell_fee_rate = self.fee_rates.get(sell_exchange, {}).get("taker", 0.001)
        
        # Calculate costs
        buy_cost = buy_price * (1 + buy_fee_rate)
        sell_revenue = sell_price * (1 - sell_fee_rate)
        
        # Calculate gross profit
        gross_profit = sell_revenue - buy_cost
        gross_profit_percentage = (gross_profit / buy_cost) * 100
        
        # Apply latency risk
        risk_adjusted_profit = gross_profit * (1 - self.latency_risk)
        risk_adjusted_percentage = (risk_adjusted_profit / buy_cost) * 100
        
        # Calculate absolute profit
        absolute_profit = risk_adjusted_profit * quantity
        
        return {
            "buy_exchange": buy_exchange,
            "sell_exchange": sell_exchange,
            "symbol": symbol,
            "buy_price": buy_price,
            "sell_price": sell_price,
            "gross_profit_percentage": gross_profit_percentage,
            "risk_adjusted_percentage": risk_adjusted_percentage,
            "absolute_profit": absolute_profit,
            "is_profitable": risk_adjusted_percentage > self.min_profit_threshold,
            "quantity": quantity
        }
    
    def find_arbitrage_opportunities(
        self, 
        data: Dict[str, Dict[str, pd.DataFrame]]
    ) -> List[Dict[str, float]]:
        """
        Find all arbitrage opportunities across exchanges.
        
        Args:
            data: Processed data from all exchanges
            
        Returns:
            List of arbitrage opportunities
        """
        logger.info("🔍 Searching for arbitrage opportunities...")
        
        opportunities = []
        exchanges = list(data.keys())
        
        # Symbol mapping for different exchanges
        symbol_mapping = {
            'btc': ['btcusdt', 'btc_usd', 'xbt_usd'],
            'eth': ['ethusdt', 'eth_usd', 'eth_usd'],
            'sol': ['solusdt', 'sol_usd', 'sol_usd']
        }
        
        # Compare all pairs of exchanges
        for i, exchange1 in enumerate(exchanges):
            for j, exchange2 in enumerate(exchanges):
                if i >= j:  # Avoid duplicate comparisons
                    continue
                
                logger.info(f"Comparing {exchange1} vs {exchange2}")
                
                # Check each cryptocurrency
                for crypto, symbols in symbol_mapping.items():
                    symbol1 = None
                    symbol2 = None
                    
                    # Find matching symbols in each exchange
                    for symbol in symbols:
                        if symbol in data[exchange1]:
                            symbol1 = symbol
                        if symbol in data[exchange2]:
                            symbol2 = symbol
                    
                    if symbol1 and symbol2:
                        df1 = data[exchange1][symbol1]
                        df2 = data[exchange2][symbol2]
                        
                        if len(df1) == 0 or len(df2) == 0:
                            continue
                        
                        # Get latest prices
                        latest_price1 = df1['price'].iloc[-1]
                        latest_price2 = df2['price'].iloc[-1]
                        
                        logger.info(f"  {crypto}: {exchange1}({symbol1})=${latest_price1:.2f}, {exchange2}({symbol2})=${latest_price2:.2f}")
                        
                        # Check both directions
                        opp1 = self.calculate_arbitrage_opportunity(
                            exchange1, exchange2, f"{crypto}_usd", latest_price1, latest_price2
                        )
                        opp2 = self.calculate_arbitrage_opportunity(
                            exchange2, exchange1, f"{crypto}_usd", latest_price2, latest_price1
                        )
                        
                        if opp1['is_profitable']:
                            opportunities.append(opp1)
                            logger.info(f"    ✅ Opportunity: {opp1['risk_adjusted_percentage']:.2f}%")
                            print(f"🎯 ARBITRAGE FOUND!")
                            print(f"   {opp1['buy_exchange']} → {opp1['sell_exchange']} ({opp1['symbol']})")
                            print(f"   Buy: ${opp1['buy_price']:.2f} → Sell: ${opp1['sell_price']:.2f}")
                            print(f"   Gross Profit: {opp1['gross_profit_percentage']:.2f}%")
                            print(f"   Net Profit: {opp1['risk_adjusted_percentage']:.2f}%")
                            print(f"   Absolute Profit: ${opp1['absolute_profit']:.2f}")
                            print()
                        if opp2['is_profitable']:
                            opportunities.append(opp2)
                            logger.info(f"    ✅ Opportunity: {opp2['risk_adjusted_percentage']:.2f}%")
                            print(f"🎯 ARBITRAGE FOUND!")
                            print(f"   {opp2['buy_exchange']} → {opp2['sell_exchange']} ({opp2['symbol']})")
                            print(f"   Buy: ${opp2['buy_price']:.2f} → Sell: ${opp2['sell_price']:.2f}")
                            print(f"   Gross Profit: {opp2['gross_profit_percentage']:.2f}%")
                            print(f"   Net Profit: {opp2['risk_adjusted_percentage']:.2f}%")
                            print(f"   Absolute Profit: ${opp2['absolute_profit']:.2f}")
                            print()
        
        # Sort by profitability
        opportunities.sort(key=lambda x: x['risk_adjusted_percentage'], reverse=True)
        
        logger.info(f"✅ Found {len(opportunities)} arbitrage opportunities")
        return opportunities
    
    def analyze_exchange_performance(self, data: Dict[str, Dict[str, pd.DataFrame]]) -> Dict[str, Dict]:
        """
        Analyze which exchanges are leading/lagging in price discovery.
        
        Args:
            data: Processed data from all exchanges
            
        Returns:
            Dictionary with exchange performance analysis
        """
        logger.info("📊 Analyzing exchange performance...")
        
        analysis = {}
        exchanges = list(data.keys())
        
        for exchange in exchanges:
            exchange_data = data[exchange]
            total_trades = sum(len(df) for df in exchange_data.values())
            avg_price = np.mean([df['price'].mean() for df in exchange_data.values()])
            
            analysis[exchange] = {
                "total_trades": total_trades,
                "avg_price": avg_price,
                "symbols": list(exchange_data.keys())
            }
        
        return analysis
    
    def generate_report(self, opportunities: List[Dict], performance: Dict) -> str:
        """
        Generate comprehensive arbitrage report.
        
        Args:
            opportunities: List of arbitrage opportunities
            performance: Exchange performance analysis
            
        Returns:
            Formatted report string
        """
        report = []
        report.append("=" * 60)
        report.append("🎯 ARBITRAGE DETECTION REPORT")
        report.append("=" * 60)
        
        # Summary
        report.append(f"\n📊 SUMMARY:")
        report.append(f"   Total Opportunities: {len(opportunities)}")
        report.append(f"   Latency Risk Factor: {self.latency_risk}")
        report.append(f"   Min Profit Threshold: {self.min_profit_threshold}%")
        
        # Top opportunities
        if opportunities:
            report.append(f"\n🏆 TOP ARBITRAGE OPPORTUNITIES:")
            for i, opp in enumerate(opportunities[:5], 1):
                report.append(f"   {i}. {opp['buy_exchange']} → {opp['sell_exchange']} ({opp['symbol']})")
                report.append(f"      Profit: {opp['risk_adjusted_percentage']:.2f}%")
                report.append(f"      Buy: ${opp['buy_price']:.2f} → Sell: ${opp['sell_price']:.2f}")
        
        # Exchange performance
        report.append(f"\n📈 EXCHANGE PERFORMANCE:")
        for exchange, stats in performance.items():
            report.append(f"   {exchange.upper()}:")
            report.append(f"      Total Trades: {stats['total_trades']:,}")
            report.append(f"      Avg Price: ${stats['avg_price']:.2f}")
            report.append(f"      Symbols: {', '.join(stats['symbols'])}")
        
        return "\n".join(report)


def main():
    """
    Main function to run arbitrage detection.
    """
    # Get latency risk from user
    try:
        latency_risk = float(input("Enter latency risk factor (0.0-1.0, default 0.3 for 300ms): ") or "0.3")
    except ValueError:
        latency_risk = 0.3
        print("Invalid input, using default latency risk: 0.3 (300ms)")
    
    # Initialize detector
    detector = ArbitrageDetector(latency_risk=latency_risk)
    
    # Load data
    data = detector.load_processed_data()
    
    # Find opportunities
    opportunities = detector.find_arbitrage_opportunities(data)
    
    # Analyze performance
    performance = detector.analyze_exchange_performance(data)
    
    # Generate report
    report = detector.generate_report(opportunities, performance)
    print(report)
    
    return opportunities, performance


if __name__ == "__main__":
    main()
