"""
Main Script for Arbitrage Detection System

This is the main entry point for the arbitrage detection system.
Users can run this script to download data, process it, and detect
arbitrage opportunities for any date.

Author: Arbitrage Detection Team
Date: 2025-10-19
"""

import asyncio
import sys
import os
from datetime import datetime
from typing import Optional, Tuple

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from data_manipulation.data_downloader import DataDownloader
from data_manipulation.data_processor import DataProcessor
from analysis.arbitrage_detector import ArbitrageDetector
from analysis.triangular_arbitrage import TriangularArbitrageDetector


def validate_date(date_str: str) -> bool:
    """
    Validate that the date is the 1st of a month.
    
    Args:
        date_str: Date string in YYYY-MM-DD format
        
    Returns:
        True if valid, False otherwise
    """
    try:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        return date_obj.day == 1
    except ValueError:
        return False


def get_user_input() -> Tuple[str, float, int]:
    """
    Get user input for date, latency risk, and arbitrage type.
    
    Returns:
        Tuple of (date, latency_risk, arbitrage_type)
    """
    print("🚀 ARBITRAGE DETECTION SYSTEM")
    print("=" * 50)
    print()
    
    # Get date
    while True:
        date_input = input("📅 Enter date (DDMMYYYY format, e.g., 01102025 for Oct 1st 2025) or press Enter for default: ")
        
        if not date_input:
            # Default to October 1st, 2025
            date_input = "2025-10-01"
            print(f"Using default date: {date_input}")
            break
        
        # Convert DDMMYYYY to YYYY-MM-DD
        if len(date_input) == 8 and date_input.isdigit():
            day = date_input[:2]
            month = date_input[2:4]
            year = date_input[4:8]
            date_input = f"{year}-{month}-{day}"
            print(f"Converted to: {date_input}")
        
        if not validate_date(date_input):
            print("❌ Date must be the 1st of a month (e.g., 01102025 for Oct 1st 2025)")
            print("💡 Only 1st of each month has free data on tardis.dev")
            continue
            
        break
    
    # Get latency risk
    while True:
        try:
            latency_input = input("⚡ Enter latency risk factor (0.0-1.0, default 0.1 for 100ms): ")
            if not latency_input:
                latency_risk = 0.1
                break
            latency_risk = float(latency_input)
            if 0.0 <= latency_risk <= 1.0:
                break
            else:
                print("❌ Latency risk must be between 0.0 and 1.0")
        except ValueError:
            print("❌ Please enter a valid number")
    
    # Get arbitrage type
    print("\n🎯 Choose Arbitrage Type:")
    print("1. Regular Arbitrage (Exchange-to-Exchange)")
    print("2. Triangular Arbitrage (Three-step trading)")
    
    while True:
        try:
            type_input = input("Enter choice (1 or 2): ")
            if type_input in ['1', '2']:
                arbitrage_type = int(type_input)
                break
            else:
                print("❌ Please enter 1 or 2")
        except ValueError:
            print("❌ Please enter a valid number")
    
    return date_input, latency_risk, arbitrage_type


async def download_and_process_data(date: str) -> bool:
    """
    Download and process data for the given date.
    
    Args:
        date: Date string in YYYY-MM-DD format
        
    Returns:
        True if successful, False otherwise
    """
    print(f"\n📥 DOWNLOADING DATA FOR {date}")
    print("=" * 50)
    
    try:
        # Download data
        downloader = DataDownloader(date=date)
        files = await downloader.download_all_data(duration_minutes=1440)
        
        if not files:
            print("❌ No data downloaded!")
            return False
            
        print(f"✅ Downloaded {len(files)} files")
        
        # Process data
        print(f"\n🔄 PROCESSING DATA")
        print("=" * 50)
        
        processor = DataProcessor(data_dir=f"data/{date}")
        processed_data = processor.process_all_data()
        
        if not processed_data:
            print("❌ No data processed!")
            return False
            
        print("✅ Data processing completed!")
        return True
        
    except Exception as e:
        print(f"❌ Error in download/process: {e}")
        return False


def detect_arbitrage(date: str, latency_risk: float, arbitrage_type: int) -> bool:
    """
    Detect arbitrage opportunities based on type.
    
    Args:
        date: Date string in YYYY-MM-DD format
        latency_risk: Latency risk factor
        arbitrage_type: 1 for regular, 2 for triangular
        
    Returns:
        True if successful, False otherwise
    """
    if arbitrage_type == 1:
        print(f"\n🎯 DETECTING REGULAR ARBITRAGE OPPORTUNITIES")
        print("=" * 50)
        
        try:
            # Initialize regular detector
            detector = ArbitrageDetector(latency_risk=latency_risk)
            
            # Load processed data
            data = detector.load_processed_data(f"data/processed/{date}")
            
            if not data:
                print("❌ No processed data found!")
                return False
            
            # Find opportunities
            opportunities = detector.find_arbitrage_opportunities(data)
            
            # Analyze performance
            performance = detector.analyze_exchange_performance(data)
            
            # Generate report
            report = detector.generate_report(opportunities, performance)
            print(report)
            
            # Save results
            results_dir = f"results/{date}"
            os.makedirs(results_dir, exist_ok=True)
            
            # Save opportunities to CSV
            if opportunities:
                import pandas as pd
                df = pd.DataFrame(opportunities)
                df.to_csv(f"{results_dir}/regular_arbitrage_opportunities.csv", index=False)
                print(f"\n💾 Results saved to: {results_dir}/")
            
            return True
            
        except Exception as e:
            print(f"❌ Error in regular arbitrage detection: {e}")
            return False
    
    elif arbitrage_type == 2:
        print(f"\n🔺 DETECTING TRIANGULAR ARBITRAGE OPPORTUNITIES")
        print("=" * 50)
        
        try:
            # Initialize triangular detector
            detector = TriangularArbitrageDetector(latency_risk=latency_risk)
            
            # Load processed data
            data = detector.load_processed_data(f"data/processed/{date}")
            
            if not data:
                print("❌ No processed data found!")
                return False
            
            # Find opportunities
            opportunities = detector.find_triangular_opportunities(data)
            
            # Analyze performance
            performance = detector.analyze_triangular_performance(data)
            
            # Generate report
            report = detector.generate_triangular_report(opportunities, performance)
            print(report)
            
            # Save results
            results_dir = f"results/{date}"
            os.makedirs(results_dir, exist_ok=True)
            
            # Save opportunities to CSV
            if opportunities:
                import pandas as pd
                df = pd.DataFrame(opportunities)
                df.to_csv(f"{results_dir}/triangular_arbitrage_opportunities.csv", index=False)
                print(f"\n💾 Results saved to: {results_dir}/")
            
            return True
            
        except Exception as e:
            print(f"❌ Error in triangular arbitrage detection: {e}")
            return False
    
    else:
        print(f"❌ Invalid arbitrage type: {arbitrage_type}")
        return False


async def main():
    """
    Main function to run the complete arbitrage detection pipeline.
    """
    try:
        # Get user input
        date, latency_risk, arbitrage_type = get_user_input()
        
        arbitrage_type_name = "Regular" if arbitrage_type == 1 else "Triangular"
        
        print(f"\n📋 CONFIGURATION:")
        print(f"   Date: {date}")
        print(f"   Latency Risk: {latency_risk}")
        print(f"   Arbitrage Type: {arbitrage_type_name}")
        print(f"   Exchanges: Binance, Coinbase, Kraken")
        print(f"   Coins: BTC, ETH, SOL")
        
        # Download and process data
        if not await download_and_process_data(date):
            print("❌ Failed to download/process data")
            return
        
        # Detect arbitrage
        if not detect_arbitrage(date, latency_risk, arbitrage_type):
            print("❌ Failed to detect arbitrage")
            return
        
        print(f"\n🎉 {arbitrage_type_name.upper()} ARBITRAGE DETECTION COMPLETED FOR {date}!")
        print("=" * 50)
        
    except KeyboardInterrupt:
        print("\n\n⏹️ Process interrupted by user")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
