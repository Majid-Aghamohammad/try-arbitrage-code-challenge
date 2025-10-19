import asyncio
from tardis_client import TardisClient, Channel

async def test_tardis_connection():
    """Test connection to tardis.dev and explore available data"""
    tardis_client = TardisClient()
    
    print("Testing tardis.dev connection...")
    
    # Test with Binance
    try:
        print("\n=== Testing Binance ===")
        messages = tardis_client.replay(
            exchange="binance",
            from_date="2025-10-01",
            to_date="2025-10-01T00:05:00",  # Just 5 minutes
            filters=[Channel(name="trade", symbols=["btcusdt"])],
        )
        
        count = 0
        async for local_timestamp, message in messages:
            print(f"Binance BTC/USDT Trade: {message}")
            count += 1
            if count >= 3:  # Just show first 3 messages
                break
                
    except Exception as e:
        print(f"Binance error: {e}")
    
    # Test with Coinbase
    try:
        print("\n=== Testing Coinbase ===")
        messages = tardis_client.replay(
            exchange="coinbase",
            from_date="2025-10-01",
            to_date="2025-10-01T00:05:00",  # Just 5 minutes
            filters=[Channel(name="match", symbols=["BTC-USD"])],
        )
        
        count = 0
        async for local_timestamp, message in messages:
            print(f"Coinbase BTC/USD Trade: {message}")
            count += 1
            if count >= 3:  # Just show first 3 messages
                break
                
    except Exception as e:
        print(f"Coinbase error: {e}")
    
    # Test with Kraken
    try:
        print("\n=== Testing Kraken ===")
        messages = tardis_client.replay(
            exchange="kraken",
            from_date="2025-10-01",
            to_date="2025-10-01T00:05:00",  # Just 5 minutes
            filters=[Channel(name="trade", symbols=["XBT/USD"])],
        )
        
        count = 0
        async for local_timestamp, message in messages:
            print(f"Kraken BTC/USD Trade: {message}")
            count += 1
            if count >= 3:  # Just show first 3 messages
                break
                
    except Exception as e:
        print(f"Kraken error: {e}")

if __name__ == "__main__":
    asyncio.run(test_tardis_connection())
