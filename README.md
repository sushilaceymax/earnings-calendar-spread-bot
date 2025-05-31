# Earnings Trade Automation

Automated trading bot for executing earnings calendar spread strategies using options in a workflow automation. Integrates with Google Sheets for trade tracking and Alpaca for order execution. 

## Features
- **Automated Earnings Calendar Spread Trading**: Opens and closes calendar spreads around earnings events based on strict screening criteria.
- **Kelly Criterion Position Sizing**: Uses a 10% Kelly fraction for optimal, risk-managed position sizing.
- **Google Sheets Integration**: Tracks trades and workflow status in a Google Sheet via Apps Script.
- **Alpaca API Integration**: Places and closes trades automatically using Alpaca brokerage API.
- **Configurable and Extensible**: Modular codebase for easy strategy tweaks and integration.

## Quick Start

### 1. Clone the Repository
```bash
git clone https://github.com/yourusername/earnings-calendar-spread-bot.git
cd earnings-calendar-spread-bot
```

### 2. Google Sheets Set Up (Optional)
Create a copy of https://docs.google.com/spreadsheets/d/1qOu4PJtcpYwLZgFFIpVr8FXD12dXoWZaKxSeg9FR7lU/ and add code.gs to App Script

### 3. Set Up Environment Variables
Create a `.env` file in the root directory with your credentials:
```
APCA_API_KEY_ID=your-alpaca-key
APCA_API_SECRET_KEY=your-alpaca-secret
GOOGLE_SCRIPT_URL=your-google-apps-script-url
ALPACA_PAPER=true  # Set to 'false' to use live trading (default is 'true' for paper trading)
```

### 4. Install Dependencies
```bash
pip install -r requirements.txt
```

### 5. Run the Bot
```bash
python automation.py
```
### 6. Automate
Add the variables to GitHub secrets and enable GitHub Actions 


## Example Workflow
- **Screen for Earnings**: Bot fetches tomorrow's earnings tickers.
- **Screening & Sizing**: For each ticker, applies IV/volume/slope criteria and calculates position size using Kelly.
- **Open Trades**: Places calendar spread trades at the correct time (BMO/AMC logic).
- **Track & Close**: Monitors open trades and closes them at the correct time, updating Google Sheets.



## Disclaimer
This software is provided solely for educational and research purposes. It is not intended to provide investment advice. The developers are not financial advisors and accept no responsibility for any financial decisions or losses resulting from the use of this software. Always consult a professional financial advisor before making any investment decisions.

---

*Happy trading, and trade responsibly!* 
