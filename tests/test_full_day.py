"""
Test arbitrage detection with full day data (24 hours)
"""

import asyncio
import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from data.data_downloader import DataDownloader
from data.data_processor import DataProcessor
from analysis.arbitrage_detector import ArbitrageDetector
from analysis.triangular_arbitrage import TriangularArbitrageDetector


async def test_full_day_arbitrage():
    """Test arbitrage detection with full day data"""
    print("🧪 TESTING FULL DAY ARBITRAGE DETECTION")
    print("=" * 60)
    
    # Test with a date that has data
    test_date = "2024-10-01"
    
    print(f"📅 Testing date: {test_date}")
    print("⏱️ Duration: 24 hours (full day)")
    print("-" * 40)
    
    try:
        # Download full day data
        print("📥 Downloading full day data...")
        downloader = DataDownloader(date=test_date)
        files = await downloader.download_all_data(duration_minutes=1440)
        
        if not files:
            print("❌ No data downloaded!")
            return
        
        print(f"✅ Downloaded {len(files)} files")
        
        # Process data
        print("\n🔄 Processing full day data...")
        processor = DataProcessor(data_dir=f"data/{test_date}")
        processed_data = processor.process_all_data()
        
        if not processed_data:
            print("❌ No data processed!")
            return
        
        print("✅ Data processing completed!")
        
        # Test Regular Arbitrage with low latency
        print(f"\n🎯 Testing Regular Arbitrage (latency_risk=0.1)...")
        detector = ArbitrageDetector(latency_risk=0.1)
        data = detector.load_processed_data(f"data/processed/{test_date}")
        
        if data:
            opportunities = detector.find_arbitrage_opportunities(data)
            print(f"✅ Found {len(opportunities)} regular arbitrage opportunities")
            
            if opportunities:
                print("🎯 REGULAR ARBITRAGE OPPORTUNITIES FOUND!")
                for i, opp in enumerate(opportunities[:5], 1):
                    print(f"   {i}. {opp['buy_exchange']} → {opp['sell_exchange']} ({opp['symbol']})")
                    print(f"      Buy: ${opp['buy_price']:.2f} → Sell: ${opp['sell_price']:.2f}")
                    print(f"      Gross Profit: {opp['gross_profit_percentage']:.2f}%")
                    print(f"      Net Profit: {opp['risk_adjusted_percentage']:.2f}%")
                    print(f"      Absolute Profit: ${opp['absolute_profit']:.2f}")
                    print()
            else:
                print("ℹ️  No regular arbitrage opportunities found")
        else:
            print("❌ Failed to load data")
        
        # Test Triangular Arbitrage
        print(f"\n🔺 Testing Triangular Arbitrage (latency_risk=0.1)...")
        triangular_detector = TriangularArbitrageDetector(latency_risk=0.1)
        data = triangular_detector.load_processed_data(f"data/processed/{test_date}")
        
        if data:
            opportunities = triangular_detector.find_triangular_opportunities(data)
            print(f"✅ Found {len(opportunities)} triangular arbitrage opportunities")
            
            if opportunities:
                print("🔺 TRIANGULAR ARBITRAGE OPPORTUNITIES FOUND!")
                for i, opp in enumerate(opportunities[:5], 1):
                    path_str = " → ".join(opp['path']) + " → " + opp['start_crypto']
                    print(f"   {i}. {opp['exchange'].upper()}: {path_str}")
                    print(f"      Gross Profit: {opp['gross_profit_percentage']:.2f}%")
                    print(f"      Fee Impact: {opp['fee_impact']:.2f}%")
                    print(f"      Net Profit: {opp['net_profit_percentage']:.2f}%")
                    print(f"      Risk Adjusted: {opp['risk_adjusted_percentage']:.2f}%")
                    print()
            else:
                print("ℹ️  No triangular arbitrage opportunities found")
        else:
            print("❌ Failed to load data")
        
        print(f"\n🎉 FULL DAY TEST COMPLETED!")
        print("✅ 24 hours of data downloaded and processed!")
        print("✅ More data = More opportunities!")
        
    except Exception as e:
        print(f"❌ Error in full day test: {e}")


if __name__ == "__main__":
    asyncio.run(test_full_day_arbitrage())
