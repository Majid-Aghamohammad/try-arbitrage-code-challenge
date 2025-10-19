"""
Data Downloader Module for Arbitrage Detection System

This module handles downloading historical market data from tardis.dev API
for multiple exchanges and cryptocurrencies. It provides async data fetching
with proper error handling and data organization.

Author: Arbitrage Detection Team
Date: 2025-10-19
"""

import asyncio
import pandas as pd
from datetime import datetime, timedelta
from tardis_client import TardisClient, Channel
import os
from typing import List, Dict, Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataDownloader:
    """
    Handles downloading and organizing market data from tardis.dev API.
    
    This class provides methods to download historical market data for
    multiple exchanges and cryptocurrencies, with proper data organization
    and error handling.
    
    Attributes:
        tardis_client: TardisClient instance for API communication
        date: Target date for data download (YYYY-MM-DD format)
        data_dir: Directory path for storing downloaded data
    """
    
    def __init__(self, date: str = "2025-10-01"):
        """
        Initialize the DataDownloader.
        
        Args:
            date: Target date for data download in YYYY-MM-DD format
        """
        self.tardis_client = TardisClient()
        self.date = date
        self.data_dir = f"data/{date}"
        os.makedirs(self.data_dir, exist_ok=True)
        
        # Exchange and symbol mappings
        self.exchanges = ["binance", "coinbase", "kraken"]
        self.symbols = {
            "binance": ["btcusdt", "ethusdt", "solusdt"],
            "coinbase": ["BTC-USD", "ETH-USD", "SOL-USD"],
            "kraken": ["XBT/USD", "ETH/USD", "SOL/USD"]
        }
        
        # Channel names for each exchange
        self.channel_names = {
            "binance": "trade",
            "coinbase": "match", 
            "kraken": "trade"
        }
    
    async def download_exchange_data(
        self,
        exchange: str,
        symbol: str,
        duration_minutes: int = 1440  # 24 hours = 1440 minutes
    ) -> Optional[str]:
        """
        Download data for a specific exchange and symbol.
        
        Args:
            exchange: Exchange name (binance, coinbase, kraken)
            symbol: Trading symbol (e.g., 'btcusdt', 'BTC-USD')
            duration_minutes: Duration of data to download in minutes
            
        Returns:
            Path to saved CSV file if successful, None otherwise
        """
        logger.info(f"Downloading {exchange} {symbol} data for {self.date}...")
        
        try:
            # Calculate time range
            start_time = datetime.fromisoformat(f"{self.date}T00:00:00")
            end_time = start_time + timedelta(minutes=duration_minutes)
            end_date = end_time.strftime("%Y-%m-%dT%H:%M:%S")
            
            # Get appropriate channel name
            channel_name = self.channel_names.get(exchange, "trade")
            
            # Fetch data from tardis.dev
            messages = self.tardis_client.replay(
                exchange=exchange,
                from_date=self.date,
                to_date=end_date,
                filters=[Channel(name=channel_name, symbols=[symbol])],
            )
            
            # Process and save data
            data = []
            async for local_timestamp, message in messages:
                processed_data = self._process_message(exchange, message, local_timestamp)
                if processed_data:
                    data.append(processed_data)
            
            if data:
                return self._save_data_to_csv(exchange, symbol, data)
            else:
                logger.warning(f"No data found for {exchange} {symbol}")
                return None
                
        except Exception as e:
            logger.error(f"Error downloading {exchange} {symbol}: {e}")
            return None
    
    def _process_message(self, exchange: str, message: dict, timestamp: str) -> Optional[dict]:
        """
        Process raw message from exchange into standardized format.
        
        Args:
            exchange: Exchange name
            message: Raw message from exchange
            timestamp: Local timestamp
            
        Returns:
            Processed data dictionary or None if processing fails
        """
        try:
            if exchange == "binance":
                return self._process_binance_message(message, timestamp)
            elif exchange == "coinbase":
                return self._process_coinbase_message(message, timestamp)
            elif exchange == "kraken":
                return self._process_kraken_message(message, timestamp)
            else:
                logger.warning(f"Unknown exchange: {exchange}")
                return None
                
        except Exception as e:
            logger.error(f"Error processing {exchange} message: {e}")
            return None
    
    def _process_binance_message(self, message: dict, timestamp: str) -> dict:
        """Process Binance message format."""
        data = message.get("data", {})
        return {
            "timestamp": timestamp,
            "exchange": "binance",
            "symbol": data.get("s", ""),
            "price": float(data.get("p", 0)),
            "quantity": float(data.get("q", 0)),
            "side": "buy" if not data.get("m", False) else "sell",
            "trade_id": data.get("t", 0)
        }
    
    def _process_coinbase_message(self, message: dict, timestamp: str) -> dict:
        """Process Coinbase message format."""
        return {
            "timestamp": timestamp,
            "exchange": "coinbase",
            "symbol": message.get("product_id", ""),
            "price": float(message.get("price", 0)),
            "quantity": float(message.get("size", 0)),
            "side": message.get("side", ""),
            "trade_id": message.get("trade_id", 0)
        }
    
    def _process_kraken_message(self, message: dict, timestamp: str) -> dict:
        """Process Kraken message format."""
        # Kraken format: [channel_id, data, channel_name, pair]
        if len(message) >= 3 and isinstance(message[1], list):
            trade_data = message[1][0]  # First trade in the batch
            return {
                "timestamp": timestamp,
                "exchange": "kraken",
                "symbol": message[3] if len(message) > 3 else "",
                "price": float(trade_data[0]),
                "quantity": float(trade_data[1]),
                "side": "buy" if trade_data[3] == "b" else "sell",
                "trade_id": message[0]
            }
        return None
    
    def _save_data_to_csv(self, exchange: str, symbol: str, data: List[dict]) -> str:
        """
        Save processed data to CSV file.
        
        Args:
            exchange: Exchange name
            symbol: Trading symbol
            data: List of processed data records
            
        Returns:
            Path to saved CSV file
        """
        # Create exchange subdirectory
        exchange_dir = f"{self.data_dir}/{exchange}"
        os.makedirs(exchange_dir, exist_ok=True)
        
        # Clean symbol name for filename
        clean_symbol = symbol.replace('/', '_').replace('-', '_').lower()
        filename = f"{exchange_dir}/{clean_symbol}.csv"
        
        # Save to CSV
        df = pd.DataFrame(data)
        df.to_csv(filename, index=False)
        
        logger.info(f"✅ Saved {len(data)} records to {filename}")
        return filename
    
    async def download_all_data(self, duration_minutes: int = 1440) -> List[str]:
        """
        Download data for all exchanges and symbols.
        
        Args:
            duration_minutes: Duration of data to download in minutes
            
        Returns:
            List of paths to downloaded CSV files
        """
        downloaded_files = []
        
        for exchange in self.exchanges:
            for symbol in self.symbols[exchange]:
                filename = await self.download_exchange_data(
                    exchange, symbol, duration_minutes
                )
                if filename:
                    downloaded_files.append(filename)
        
        logger.info(f"📊 Downloaded {len(downloaded_files)} files total")
        return downloaded_files


async def main():
    """
    Main function to download all market data.
    
    Downloads historical market data for BTC, ETH, and SOL across
    Binance, Coinbase, and Kraken exchanges for October 1st, 2025.
    """
    downloader = DataDownloader(date="2025-10-01")
    
    print("🚀 Starting data download...")
    print("📅 Date: 2025-10-01")
    print("⏱️ Duration: 24 hours (full day)")
    print("=" * 50)
    
    files = await downloader.download_all_data(duration_minutes=1440)
    
    print("\n✅ Data download completed!")
    print(f"📁 Files saved in: {downloader.data_dir}/")
    print("\n📂 Directory structure:")
    print("data/2025-10-01/")
    print("├── binance/")
    print("│   ├── btcusdt.csv")
    print("│   ├── ethusdt.csv")
    print("│   └── solusdt.csv")
    print("├── coinbase/")
    print("│   ├── btc_usd.csv")
    print("│   ├── eth_usd.csv")
    print("│   └── sol_usd.csv")
    print("└── kraken/")
    print("    ├── xbt_usd.csv")
    print("    ├── eth_usd.csv")
    print("    └── sol_usd.csv")
    
    return files


if __name__ == "__main__":
    asyncio.run(main())