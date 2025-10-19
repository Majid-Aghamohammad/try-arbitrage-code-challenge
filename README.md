# 🎯 Arbitrage Detection System

A comprehensive system for detecting arbitrage opportunities across multiple cryptocurrency exchanges.

## 🚀 Quick Start

```bash
# Navigate to project directory
cd arbitrage-detection

# Activate virtual environment
source venv/bin/activate

# Run the main script
python main.py
```

## 📋 Requirements

- Python 3.8+
- Virtual environment activated
- Internet connection for data download

## 🔧 Setup

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Run the system:**
   ```bash
   python main.py
   ```

## 📅 Usage

### Input Requirements:
- **Date**: Must be the 1st of any month (e.g., 2025-10-01)
- **Latency Risk**: 0.0-1.0 (default: 0.3 for 300ms latency)

### Example Session:
```
🚀 ARBITRAGE DETECTION SYSTEM
==================================================

📅 Enter date (YYYY-MM-DD) - MUST be 1st of month: 2025-10-01
⚡ Enter latency risk factor (0.0-1.0, default 0.3 for 300ms): 0.3
```

## 📊 What the System Does

### 1. **Data Download**
- Downloads historical market data from tardis.dev
- Supports 3 exchanges: Binance, Coinbase, Kraken
- Supports 3 cryptocurrencies: BTC, ETH, SOL
- Downloads 24 hours of data (full day)

### 2. **Data Processing**
- Cleans and validates data
- Removes duplicates and outliers
- Synchronizes timestamps across exchanges
- Saves processed data to `data/processed/`

### 3. **Arbitrage Detection**
- Compares prices across exchanges
- Calculates profit after fees
- Applies latency risk factor
- Identifies profitable opportunities

### 4. **Results**
- Generates comprehensive report
- Saves opportunities to CSV
- Analyzes exchange performance

## 📁 Project Structure

```
arbitrage-detection/
├── main.py                          # Main script
├── src/
│   ├── data/
│   │   ├── data_downloader.py      # Download data from tardis.dev
│   │   └── data_processor.py       # Clean and process data
│   └── analysis/
│       └── arbitrage_detector.py   # Detect arbitrage opportunities
├── data/
│   ├── 2025-10-01/                 # Raw data by date
│   └── processed/                  # Processed data
├── results/                        # Analysis results
└── requirements.txt
```

## ⚙️ Configuration

### Exchange Fees:
- **Binance**: 0.1% (maker/taker)
- **Coinbase**: 0.5% (maker/taker)
- **Kraken**: 0.16% / 0.26% (maker/taker)

### Latency Risk:
- **Default**: 0.3 (300ms latency)
- **Range**: 0.0-1.0
- **Higher values**: More conservative (less opportunities)

## 📈 Output Example

```
🎯 ARBITRAGE DETECTION REPORT
============================================================

📊 SUMMARY:
   Total Opportunities: 15
   Latency Risk Factor: 0.3
   Min Profit Threshold: 1%

🏆 TOP ARBITRAGE OPPORTUNITIES:
   1. binance → coinbase (btcusdt)
      Profit: 2.45%
      Buy: $114,000.00 → Sell: $114,500.00
   2. coinbase → kraken (eth_usd)
      Profit: 1.89%
      Buy: $3,200.00 → Sell: $3,250.00

📈 EXCHANGE PERFORMANCE:
   BINANCE:
      Total Trades: 301,970
      Avg Price: $114,250.00
      Symbols: btcusdt, ethusdt, solusdt
```

## 🔍 Key Features

- **Real-time Data**: Uses tardis.dev for historical data
- **Multiple Exchanges**: Binance, Coinbase, Kraken
- **Fee Integration**: Real exchange fees included
- **Latency Risk**: Configurable risk factor
- **Comprehensive Analysis**: Exchange performance metrics
- **Export Results**: CSV output for further analysis

## ⚠️ Important Notes

1. **Date Restriction**: Only 1st of each month has free data
2. **Data Volume**: Downloads 24 hours of data (full day)
3. **Processing Time**: May take several minutes
4. **Internet Required**: For data download

## 🛠️ Troubleshooting

### Common Issues:
- **No data found**: Check if date is 1st of month
- **Import errors**: Ensure virtual environment is activated
- **Network issues**: Check internet connection

### Solutions:
- Use valid dates (1st of month)
- Run `source venv/bin/activate` before execution
- Check tardis.dev service status

## 📞 Support

For issues or questions, check the logs in the terminal output.
