"""
Test script for triangular arbitrage
"""

import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from analysis.triangular_arbitrage import TriangularArbitrageDetector

def test_triangular_arbitrage():
    """Test triangular arbitrage detection"""
    print("🔺 Testing Triangular Arbitrage Detection...")
    
    # Initialize with default latency risk (0.3)
    detector = TriangularArbitrageDetector(latency_risk=0.3)
    print(f"✅ TriangularArbitrageDetector initialized with latency_risk: {detector.latency_risk}")
    
    # Test loading processed data
    print("\n📂 Testing data loading...")
    try:
        data = detector.load_processed_data()
        print(f"✅ Data loaded successfully")
        
        # Print data summary
        for exchange, exchange_data in data.items():
            print(f"   {exchange}: {len(exchange_data)} symbols")
            for symbol, df in exchange_data.items():
                print(f"      {symbol}: {len(df)} records")
        
        # Test finding opportunities
        print("\n🔍 Testing triangular arbitrage detection...")
        opportunities = detector.find_triangular_opportunities(data)
        print(f"✅ Found {len(opportunities)} triangular opportunities")
        
        # Test performance analysis
        print("\n📊 Testing performance analysis...")
        performance = detector.analyze_triangular_performance(data)
        print(f"✅ Performance analysis completed")
        
        # Generate report
        print("\n📋 Generating report...")
        report = detector.generate_triangular_report(opportunities, performance)
        print(report)
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    success = test_triangular_arbitrage()
    if success:
        print("\n🎉 Triangular arbitrage test completed successfully!")
    else:
        print("\n❌ Triangular arbitrage test failed!")
