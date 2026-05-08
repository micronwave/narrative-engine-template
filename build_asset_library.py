"""
Offline script: build the asset embedding library from SEC 10-K filings.
Run this once before starting the daily pipeline cycle.

Usage:
    python build_asset_library.py [--download-dir /path/to/filings]

The output is a pickle file at ASSET_LIBRARY_PATH (from settings):
    {ticker: {'name': str, 'embedding': np.ndarray}}
"""

import argparse
import logging
import pickle
import re
import sys
from pathlib import Path

import numpy as np

from embedding_model import MiniLMEmbedder
from settings import settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# S&P 500 constituents — representative list (~200 major US tickers)
# Company names are used as a fallback when no 10-K business section is found.
# ---------------------------------------------------------------------------
TICKERS: dict[str, str] = {
    "AAPL": "Apple Inc.",
    "MSFT": "Microsoft Corporation",
    "AMZN": "Amazon.com Inc.",
    "NVDA": "NVIDIA Corporation",
    "GOOGL": "Alphabet Inc. Class A",
    "GOOG": "Alphabet Inc. Class C",
    "META": "Meta Platforms Inc.",
    "TSLA": "Tesla Inc.",
    "BRK.B": "Berkshire Hathaway Inc.",
    "UNH": "UnitedHealth Group Inc.",
    "LLY": "Eli Lilly and Company",
    "JPM": "JPMorgan Chase & Co.",
    "V": "Visa Inc.",
    "XOM": "Exxon Mobil Corporation",
    "AVGO": "Broadcom Inc.",
    "PG": "Procter & Gamble Co.",
    "MA": "Mastercard Inc.",
    "COST": "Costco Wholesale Corporation",
    "HD": "Home Depot Inc.",
    "CVX": "Chevron Corporation",
    "MRK": "Merck & Co. Inc.",
    "ABBV": "AbbVie Inc.",
    "KO": "Coca-Cola Company",
    "PEP": "PepsiCo Inc.",
    "ADBE": "Adobe Inc.",
    "CRM": "Salesforce Inc.",
    "WMT": "Walmart Inc.",
    "C": "Citigroup Inc.",
    "BLK": "BlackRock Inc.",
    "BAC": "Bank of America Corporation",
    "MCD": "McDonald's Corporation",
    "CSCO": "Cisco Systems Inc.",
    "ACN": "Accenture plc",
    "ABT": "Abbott Laboratories",
    "NFLX": "Netflix Inc.",
    "TMO": "Thermo Fisher Scientific Inc.",
    "LIN": "Linde plc",
    "ORCL": "Oracle Corporation",
    "DIS": "Walt Disney Company",
    "INTC": "Intel Corporation",
    "AMD": "Advanced Micro Devices Inc.",
    "WFC": "Wells Fargo & Company",
    "PM": "Philip Morris International Inc.",
    "TXN": "Texas Instruments Inc.",
    "INTU": "Intuit Inc.",
    "CAT": "Caterpillar Inc.",
    "MS": "Morgan Stanley",
    "GS": "Goldman Sachs Group Inc.",
    "RTX": "Raytheon Technologies Corporation",
    "HON": "Honeywell International Inc.",
    "UPS": "United Parcel Service Inc.",
    "BMY": "Bristol-Myers Squibb Company",
    "AMGN": "Amgen Inc.",
    "LOW": "Lowe's Companies Inc.",
    "SPGI": "S&P Global Inc.",
    "GE": "General Electric Company",
    "SBUX": "Starbucks Corporation",
    "PLD": "Prologis Inc.",
    "AMAT": "Applied Materials Inc.",
    "AXP": "American Express Company",
    "MDT": "Medtronic plc",
    "BKNG": "Booking Holdings Inc.",
    "TJX": "TJX Companies Inc.",
    "SYK": "Stryker Corporation",
    "ISRG": "Intuitive Surgical Inc.",
    "ELV": "Elevance Health Inc.",
    "CB": "Chubb Limited",
    "GILD": "Gilead Sciences Inc.",
    "MO": "Altria Group Inc.",
    "CI": "Cigna Group",
    "BDX": "Becton Dickinson and Company",
    "CME": "CME Group Inc.",
    "LRCX": "Lam Research Corporation",
    "EOG": "EOG Resources Inc.",
    "ZTS": "Zoetis Inc.",
    "SO": "Southern Company",
    "PGR": "Progressive Corporation",
    "AON": "Aon plc",
    "DUK": "Duke Energy Corporation",
    "ITW": "Illinois Tool Works Inc.",
    "BSX": "Boston Scientific Corporation",
    "NOC": "Northrop Grumman Corporation",
    "HUM": "Humana Inc.",
    "KLAC": "KLA Corporation",
    "ETN": "Eaton Corporation plc",
    "ICE": "Intercontinental Exchange Inc.",
    "MAR": "Marriott International Inc.",
    "GD": "General Dynamics Corporation",
    "SHW": "Sherwin-Williams Company",
    "HCA": "HCA Healthcare Inc.",
    "TGT": "Target Corporation",
    "NKE": "Nike Inc.",
    "LULU": "Lululemon Athletica Inc.",
    "REGN": "Regeneron Pharmaceuticals Inc.",
    "FDX": "FedEx Corporation",
    "MATX": "Matson Inc.",
    "ZIM": "ZIM Integrated Shipping Services Ltd.",
    "DAC": "Danaos Corporation",
    "GOGL": "Golden Ocean Group Limited",
    "SBLK": "Star Bulk Carriers Corp.",
    "EGLE": "Eagle Bulk Shipping Inc.",
    "MCK": "McKesson Corporation",
    "COP": "ConocoPhillips",
    "PSX": "Phillips 66",
    # PXD removed — acquired by ExxonMobil, delisted May 2024
    "OXY": "Occidental Petroleum Corporation",
    "SLB": "Schlumberger Limited",
    "HAL": "Halliburton Company",
    "BKR": "Baker Hughes Company",
    "DVN": "Devon Energy Corporation",
    "FANG": "Diamondback Energy Inc.",
    "MRO": "Marathon Oil Corporation",
    "APA": "APA Corporation",
    "OVV": "Ovintiv Inc.",
    "CTRA": "Coterra Energy Inc.",
    "PR": "Permian Resources Corporation",
    "VLO": "Valero Energy Corporation",
    "MPC": "Marathon Petroleum Corporation",
    "F": "Ford Motor Company",
    "GM": "General Motors Company",
    "DE": "Deere & Company",
    "EMR": "Emerson Electric Co.",
    "APD": "Air Products and Chemicals Inc.",
    "ECL": "Ecolab Inc.",
    "PPG": "PPG Industries Inc.",
    "NEM": "Newmont Corporation",
    "GOLD": "Barrick Gold Corporation",
    "AEM": "Agnico Eagle Mines Limited",
    "FNV": "Franco-Nevada Corporation",
    "WPM": "Wheaton Precious Metals Corp.",
    "RGLD": "Royal Gold Inc.",
    "FCX": "Freeport-McMoRan Inc.",
    "NUE": "Nucor Corporation",
    "STLD": "Steel Dynamics Inc.",
    "LMT": "Lockheed Martin Corporation",
    "BA": "Boeing Company",
    "HII": "Huntington Ingalls Industries Inc.",
    "LHX": "L3Harris Technologies Inc.",
    "TDG": "TransDigm Group Inc.",
    "LDOS": "Leidos Holdings Inc.",
    "SAIC": "Science Applications International Corporation",
    "TRV": "Travelers Companies Inc.",
    "ALL": "Allstate Corporation",
    "AIG": "American International Group Inc.",
    "PRU": "Prudential Financial Inc.",
    "MET": "MetLife Inc.",
    "PFG": "Principal Financial Group Inc.",
    "AFL": "Aflac Inc.",
    "USB": "U.S. Bancorp",
    "PNC": "PNC Financial Services Group Inc.",
    "TFC": "Truist Financial Corporation",
    "COF": "Capital One Financial Corporation",
    "DFS": "Discover Financial Services",
    "SYF": "Synchrony Financial",
    "BK": "Bank of New York Mellon Corporation",
    "STT": "State Street Corporation",
    "SCHW": "Charles Schwab Corporation",
    "MSCI": "MSCI Inc.",
    "MCO": "Moody's Corporation",
    "FIS": "Fidelity National Information Services Inc.",
    "FISV": "Fiserv Inc.",
    "GPN": "Global Payments Inc.",
    "PYPL": "PayPal Holdings Inc.",
    "SQ": "Block Inc.",
    "SHOP": "Shopify Inc.",
    "NOW": "ServiceNow Inc.",
    "SNOW": "Snowflake Inc.",
    "DDOG": "Datadog Inc.",
    "OKTA": "Okta Inc.",
    "ZS": "Zscaler Inc.",
    "CRWD": "CrowdStrike Holdings Inc.",
    "PANW": "Palo Alto Networks Inc.",
    "FTNT": "Fortinet Inc.",
    "NET": "Cloudflare Inc.",
    "MDB": "MongoDB Inc.",
    "TEAM": "Atlassian Corporation",
    "ZM": "Zoom Video Communications Inc.",
    "DOCU": "DocuSign Inc.",
    "TWLO": "Twilio Inc.",
    "UBER": "Uber Technologies Inc.",
    "LYFT": "Lyft Inc.",
    "ABNB": "Airbnb Inc.",
    "DASH": "DoorDash Inc.",
    "COIN": "Coinbase Global Inc.",
    "RBLX": "Roblox Corporation",
    "SNAP": "Snap Inc.",
    "PINS": "Pinterest Inc.",
    "MTCH": "Match Group Inc.",
    "TTD": "Trade Desk Inc.",
    "ROKU": "Roku Inc.",
    "SPOT": "Spotify Technology S.A.",
    "EA": "Electronic Arts Inc.",
    "TTWO": "Take-Two Interactive Software Inc.",
    # ATVI removed — acquired by Microsoft, delisted Oct 2023
    "NTES": "NetEase Inc.",
    "BIDU": "Baidu Inc.",
    "JD": "JD.com Inc.",
    "PDD": "PDD Holdings Inc.",
    "NIO": "NIO Inc.",
    "XPEV": "XPeng Inc.",
    "LI": "Li Auto Inc.",
    "BABA": "Alibaba Group Holding Limited",
    "TCEHY": "Tencent Holdings Limited",
    "TSM": "Taiwan Semiconductor Manufacturing Co.",
    "ASML": "ASML Holding N.V.",
    "SAP": "SAP SE",
    "SIEGY": "Siemens AG",
    "NSRGY": "Nestle S.A.",
    "NVO": "Novo Nordisk A/S",
    "AZN": "AstraZeneca plc",
    "GSK": "GSK plc",
    "NOVN": "Novartis AG",
    "ROG": "Roche Holding AG",
    "SNY": "Sanofi",
    "BAYRY": "Bayer AG",
    "RHHBY": "Roche Holding Ltd",
    "RY": "Royal Bank of Canada",
    "TD": "Toronto-Dominion Bank",
    "BNS": "Bank of Nova Scotia",
    "BMO": "Bank of Montreal",
    "CM": "Canadian Imperial Bank of Commerce",
    "CNI": "Canadian National Railway Company",
    "CP": "Canadian Pacific Kansas City Limited",
    "ENB": "Enbridge Inc.",
    "SU": "Suncor Energy Inc.",
    "CVS": "CVS Health Corporation",
    "WBA": "Walgreens Boots Alliance Inc.",
    "CAH": "Cardinal Health Inc.",
    "ABC": "AmerisourceBergen Corporation",
    "DHR": "Danaher Corporation",
    "A": "Agilent Technologies Inc.",
    "WAT": "Waters Corporation",
    "MTD": "Mettler-Toledo International Inc.",
    "PKI": "PerkinElmer Inc.",
    "IDXX": "IDEXX Laboratories Inc.",
    "IQV": "IQVIA Holdings Inc.",
    "CRL": "Charles River Laboratories International Inc.",
    "ILMN": "Illumina Inc.",
    "BIIB": "Biogen Inc.",
    "VRTX": "Vertex Pharmaceuticals Inc.",
    "ALNY": "Alnylam Pharmaceuticals Inc.",
    "MRNA": "Moderna Inc.",
    "BNTX": "BioNTech SE",
    "PFE": "Pfizer Inc.",
    "JNJ": "Johnson & Johnson",
    "HLT": "Hilton Worldwide Holdings Inc.",
    "H": "Hyatt Hotels Corporation",
    "IHG": "InterContinental Hotels Group plc",
    "CCL": "Carnival Corporation",
    "RCL": "Royal Caribbean Cruises Ltd.",
    "NCLH": "Norwegian Cruise Line Holdings Ltd.",
    "DAL": "Delta Air Lines Inc.",
    "UAL": "United Airlines Holdings Inc.",
    "AAL": "American Airlines Group Inc.",
    "LUV": "Southwest Airlines Co.",
    "ALK": "Alaska Air Group Inc.",
    "CSX": "CSX Corporation",
    "NSC": "Norfolk Southern Corporation",
    "UNP": "Union Pacific Corporation",
    "CHRW": "C.H. Robinson Worldwide Inc.",
    "EXPD": "Expeditors International of Washington Inc.",
    "XPO": "XPO Inc.",
    "JBHT": "J.B. Hunt Transport Services Inc.",
    "KNX": "Knight-Swift Transportation Holdings Inc.",
    "WM": "Waste Management Inc.",
    "RSG": "Republic Services Inc.",
    "AWK": "American Water Works Company Inc.",
    "AEE": "Ameren Corporation",
    "AEP": "American Electric Power Company Inc.",
    "EXC": "Exelon Corporation",
    "PCG": "PG&E Corporation",
    "ED": "Consolidated Edison Inc.",
    "D": "Dominion Energy Inc.",
    "NEE": "NextEra Energy Inc.",
    "AES": "AES Corporation",
    "ES": "Eversource Energy",
    "ETR": "Entergy Corporation",
    "PPL": "PPL Corporation",
    "XEL": "Xcel Energy Inc.",
    "WEC": "WEC Energy Group Inc.",
    "CNP": "CenterPoint Energy Inc.",
    "NI": "NiSource Inc.",
    "OGE": "OGE Energy Corp.",
    "PNW": "Pinnacle West Capital Corporation",
    "IDA": "IDACORP Inc.",
    "NWE": "NorthWestern Corporation",
    "AVA": "Avista Corporation",
    "POR": "Portland General Electric Company",
    "HE": "Hawaiian Electric Industries Inc.",
    "SPG": "Simon Property Group Inc.",
    "O": "Realty Income Corporation",
    "AMT": "American Tower Corporation",
    "CCI": "Crown Castle Inc.",
    "SBAC": "SBA Communications Corporation",
    "DLR": "Digital Realty Trust Inc.",
    "EQIX": "Equinix Inc.",
    "PSA": "Public Storage",
    "EXR": "Extra Space Storage Inc.",
    "AVB": "AvalonBay Communities Inc.",
    "EQR": "Equity Residential",
    "MAA": "Mid-America Apartment Communities Inc.",
    "CPT": "Camden Property Trust",
    "UDR": "UDR Inc.",
    "ESS": "Essex Property Trust Inc.",
    "INVH": "Invitation Homes Inc.",
    "AMH": "American Homes 4 Rent",
    # SFR removed — Tricon Residential, acquired and delisted
    # Utilities (additions)
    "SRE": "Sempra Energy",
    "PEG": "Public Service Enterprise Group Inc.",
    # Materials (additions)
    "VMC": "Vulcan Materials Company",
    "MLM": "Martin Marietta Materials Inc.",
    "DOW": "Dow Inc.",
    "VTR": "Ventas Inc.",
    "WELL": "Welltower Inc.",
    # PEAK removed — changed ticker to DOC (already listed)
    "OHI": "Omega Healthcare Investors Inc.",
    "SBRA": "Sabra Health Care REIT Inc.",
    "NHI": "National Health Investors Inc.",
    "HR": "Healthcare Realty Trust Inc.",
    "DOC": "Physicians Realty Trust",
    "CTRE": "CareTrust REIT Inc.",
    "MPW": "Medical Properties Trust Inc.",
    "LTC": "LTC Properties Inc.",
    # SNH removed — Senior Housing Properties Trust, delisted
}


# ---------------------------------------------------------------------------
# ETFs and crypto — no SEC 10-K filings; descriptive text embedded directly.
# ---------------------------------------------------------------------------
NON_FILING_ASSETS: dict[str, dict[str, str]] = {
    # Broad market ETFs
    "SPY": {
        "name": "SPDR S&P 500 ETF Trust",
        "text": "SPY tracks the S&P 500 index representing 500 large-cap US equities across all sectors. Broad market US equity exposure. Most liquid ETF globally.",
    },
    "QQQ": {
        "name": "Invesco QQQ Trust (Nasdaq-100)",
        "text": "QQQ tracks the Nasdaq-100 index of the 100 largest non-financial Nasdaq-listed companies. Heavy technology and growth stock exposure including mega-cap tech.",
    },
    "IWM": {
        "name": "iShares Russell 2000 ETF",
        "text": "IWM tracks the Russell 2000 index of US small-cap equities. Exposure to smaller domestic US companies sensitive to economic growth and interest rates.",
    },
    "DIA": {
        "name": "SPDR Dow Jones Industrial Average ETF",
        "text": "DIA tracks the Dow Jones Industrial Average, 30 large blue-chip US companies. Price-weighted index of established US industrial and consumer companies.",
    },
    # Sector ETFs
    "XLF": {
        "name": "Financial Select Sector SPDR Fund",
        "text": "XLF tracks financial sector stocks including banks, insurance companies, capital markets, and diversified financial services.",
    },
    "XLK": {
        "name": "Technology Select Sector SPDR Fund",
        "text": "XLK tracks technology sector stocks including semiconductors, software, hardware, and IT services companies.",
    },
    "XLE": {
        "name": "Energy Select Sector SPDR Fund",
        "text": "XLE tracks energy sector stocks including oil and gas exploration, production, refining, pipelines, and energy equipment services.",
    },
    "XLV": {
        "name": "Health Care Select Sector SPDR Fund",
        "text": "XLV tracks health care sector stocks including pharmaceuticals, biotechnology, medical devices, health insurance, and managed care.",
    },
    "XLI": {
        "name": "Industrial Select Sector SPDR Fund",
        "text": "XLI tracks industrial sector stocks including aerospace defense, machinery, transportation, construction, and industrial conglomerates.",
    },
    "XLP": {
        "name": "Consumer Staples Select Sector SPDR Fund",
        "text": "XLP tracks consumer staples stocks including food beverage, household products, tobacco, and retail companies with stable non-cyclical demand.",
    },
    "XLU": {
        "name": "Utilities Select Sector SPDR Fund",
        "text": "XLU tracks utilities sector stocks including electric utilities, gas utilities, water utilities, and multi-utilities. Rate-sensitive defensive sector.",
    },
    "XLY": {
        "name": "Consumer Discretionary Select Sector SPDR Fund",
        "text": "XLY tracks consumer discretionary stocks including retail, automobiles, hotels, restaurants, and leisure companies sensitive to consumer spending.",
    },
    "XLC": {
        "name": "Communication Services Select Sector SPDR Fund",
        "text": "XLC tracks communication services stocks including telecom, media, entertainment, and interactive internet companies.",
    },
    "XLRE": {
        "name": "Real Estate Select Sector SPDR Fund",
        "text": "XLRE tracks real estate sector stocks including REITs covering residential, commercial, industrial, healthcare, and specialty properties.",
    },
    "XLB": {
        "name": "Materials Select Sector SPDR Fund",
        "text": "XLB tracks materials sector stocks including chemicals, metals, mining, paper, and construction materials companies.",
    },
    # Commodity ETFs
    "GLD": {
        "name": "SPDR Gold Shares ETF",
        "text": "GLD tracks the price of gold bullion. Safe-haven asset used as inflation hedge and store of value. Inversely correlated with real interest rates.",
    },
    "SLV": {
        "name": "iShares Silver Trust ETF",
        "text": "SLV tracks the price of silver bullion. Industrial and precious metal with demand from electronics, solar panels, and as a monetary metal.",
    },
    "USO": {
        "name": "United States Oil Fund ETF",
        "text": "USO tracks West Texas Intermediate crude oil futures prices. Exposure to oil price movements driven by OPEC supply, global demand, and geopolitics.",
    },
    # Bond ETFs
    "TLT": {
        "name": "iShares 20+ Year Treasury Bond ETF",
        "text": "TLT tracks long-duration US Treasury bonds with maturities over 20 years. Highly sensitive to interest rate changes and Federal Reserve policy.",
    },
    "HYG": {
        "name": "iShares iBoxx High Yield Corporate Bond ETF",
        "text": "HYG tracks high-yield or junk corporate bonds. Credit risk sensitive to economic conditions, default rates, and corporate earnings outlook.",
    },
    "LQD": {
        "name": "iShares iBoxx Investment Grade Corporate Bond ETF",
        "text": "LQD tracks investment-grade corporate bonds. Sensitive to interest rate changes and credit spreads of large investment-grade US corporations.",
    },
    # Oil services ETF
    "OIH": {
        "name": "VanEck Oil Services ETF",
        "text": "OIH tracks oil field services and equipment companies including drilling, pressure pumping, and oilfield technology services for upstream oil and gas.",
    },
    # Defense ETFs
    "ITA": {
        "name": "iShares U.S. Aerospace & Defense ETF",
        "text": "ITA tracks US aerospace and defense companies including military aircraft, weapons systems, defense electronics, and government contractors.",
    },
    "XAR": {
        "name": "SPDR S&P Aerospace & Defense ETF",
        "text": "XAR tracks the S&P Aerospace and Defense Select Industry Index covering commercial aerospace, military defense, and space companies.",
    },
    # Shipping ETF
    "BDRY": {
        "name": "Breakwave Dry Bulk Shipping ETF",
        "text": "BDRY provides exposure to dry bulk shipping freight rates via futures. Tracks iron ore, coal, and grain shipping demand across global trade routes.",
    },
    # Gold ETFs
    "IAU": {
        "name": "iShares Gold Trust ETF",
        "text": "IAU tracks the price of gold bullion. Low-cost alternative to GLD for gold price exposure as an inflation hedge and safe-haven asset.",
    },
    "SGOL": {
        "name": "Aberdeen Standard Physical Gold Shares ETF",
        "text": "SGOL holds physical gold bullion stored in Swiss vaults. Provides direct gold price exposure outside the US banking system.",
    },
    "GDX": {
        "name": "VanEck Gold Miners ETF",
        "text": "GDX tracks large-cap gold and silver mining companies. Provides leveraged exposure to gold price movements via mining company equity.",
    },
    "GDXJ": {
        "name": "VanEck Junior Gold Miners ETF",
        "text": "GDXJ tracks small and mid-cap junior gold and silver mining companies. Higher volatility and leverage to gold prices than senior miners.",
    },
    # Bond ETFs (additional)
    "IEF": {
        "name": "iShares 7-10 Year Treasury Bond ETF",
        "text": "IEF tracks intermediate-duration US Treasury bonds maturing in 7-10 years. Sensitive to Federal Reserve rate decisions and inflation expectations.",
    },
    "SHY": {
        "name": "iShares 1-3 Year Treasury Bond ETF",
        "text": "SHY tracks short-duration US Treasury bonds maturing in 1-3 years. Low interest rate risk, near cash equivalent in safe-haven periods.",
    },
    "BND": {
        "name": "Vanguard Total Bond Market ETF",
        "text": "BND tracks the broad US investment-grade bond market including Treasuries, mortgage-backed securities, and corporate bonds.",
    },
    "AGG": {
        "name": "iShares Core U.S. Aggregate Bond ETF",
        "text": "AGG tracks the US aggregate bond market. Broad fixed income exposure across government, corporate, and securitized bonds.",
    },
    "TIPS": {
        "name": "iShares TIPS Bond ETF",
        "text": "TIPS tracks Treasury Inflation-Protected Securities. Principal adjusts with CPI inflation; used as inflation hedge and real yield gauge.",
    },
    "TMF": {
        "name": "Direxion Daily 20+ Year Treasury Bull 3X Shares",
        "text": "TMF provides 3x leveraged exposure to long-duration US Treasury bonds. Extreme sensitivity to interest rate changes and Fed policy.",
    },
    # Volatility products
    "VXX": {
        "name": "iPath Series B S&P 500 VIX Short-Term Futures ETN",
        "text": "VXX tracks short-term VIX futures providing exposure to near-term market volatility. Spikes during market stress and risk-off events.",
    },
    "UVXY": {
        "name": "ProShares Ultra VIX Short-Term Futures ETF",
        "text": "UVXY provides 1.5x leveraged exposure to short-term VIX futures. Extreme volatility instrument used for hedging during market crashes.",
    },
    "SVXY": {
        "name": "ProShares Short VIX Short-Term Futures ETF",
        "text": "SVXY provides inverse exposure to short-term VIX futures. Profits when market volatility declines during calm risk-on periods.",
    },
    # REIT ETF
    "VNQ": {
        "name": "Vanguard Real Estate ETF",
        "text": "VNQ tracks the MSCI US Investable Market Real Estate 25/50 Index. Broad REIT exposure across commercial, residential, and specialty real estate.",
    },
    # China & Emerging Markets ETFs
    "FXI": {
        "name": "iShares China Large-Cap ETF",
        "text": "FXI tracks the 50 largest Chinese equities listed on Hong Kong Stock Exchange. Concentrated exposure to Chinese financials, tech, and energy.",
    },
    "KWEB": {
        "name": "KraneShares CSI China Internet ETF",
        "text": "KWEB tracks Chinese internet and e-commerce companies including search, social media, online retail, and fintech. High geopolitical risk sensitivity.",
    },
    "EEM": {
        "name": "iShares MSCI Emerging Markets ETF",
        "text": "EEM tracks large and mid-cap emerging market equities across 24 countries. Sensitive to dollar strength, commodity prices, and geopolitical risk.",
    },
    "VWO": {
        "name": "Vanguard FTSE Emerging Markets ETF",
        "text": "VWO tracks emerging market equities including China, India, Brazil, Taiwan, and South Africa. Broad developing economy exposure.",
    },
    "MCHI": {
        "name": "iShares MSCI China ETF",
        "text": "MCHI tracks large and mid-cap Chinese equities across all exchanges including A-shares, H-shares, and US-listed ADRs.",
    },
    # Bank ETFs
    "KBE": {
        "name": "SPDR S&P Bank ETF",
        "text": "KBE tracks the S&P Banks Select Industry Index of large US commercial banks and thrifts. Rate-sensitive financial sector exposure.",
    },
    "KRE": {
        "name": "SPDR S&P Regional Banking ETF",
        "text": "KRE tracks regional US banks. More sensitive to domestic economic conditions, loan losses, and interest rate spreads than money-center banks.",
    },
    # Volatility
    "VIX": {
        "name": "CBOE Volatility Index (VIX)",
        "text": "VIX measures implied volatility of S&P 500 options, known as the fear gauge. Spikes during market stress, uncertainty, and risk-off events.",
    },
    # Crypto
    "BTC-USD": {
        "name": "Bitcoin",
        "text": "Bitcoin is the largest decentralized digital currency and cryptocurrency by market cap. Used as digital store of value, inflation hedge, and speculative asset.",
    },
    "ETH-USD": {
        "name": "Ethereum",
        "text": "Ethereum is a decentralized blockchain platform supporting smart contracts and DeFi applications. ETH is the native token powering the Ethereum network.",
    },
    "SOL-USD": {
        "name": "Solana",
        "text": "Solana is a high-performance blockchain for decentralized applications and crypto assets. Known for high throughput and low transaction costs.",
    },
    "AVAX-USD": {
        "name": "Avalanche",
        "text": "Avalanche is a layer-1 blockchain platform for DeFi, NFTs, and enterprise applications with fast finality and low fees.",
    },
    "MATIC-USD": {
        "name": "Polygon (MATIC)",
        "text": "Polygon is an Ethereum layer-2 scaling solution providing faster and cheaper transactions while leveraging Ethereum security.",
    },
    "LINK-USD": {
        "name": "Chainlink (LINK)",
        "text": "Chainlink is a decentralized oracle network connecting smart contracts with real-world data, enabling hybrid on-chain and off-chain computation.",
    },
    "UNI-USD": {
        "name": "Uniswap (UNI)",
        "text": "Uniswap is the largest decentralized exchange protocol on Ethereum using automated market makers for permissionless token swaps.",
    },
}

# ---------------------------------------------------------------------------
# High-impact macro and market event topic embeddings.
# Stored in the library as TOPIC:<topic_text> keys.
# ---------------------------------------------------------------------------
MACRO_TOPICS: dict[str, str] = {
    "Federal Reserve interest rate decision": (
        "Federal Reserve FOMC interest rate decision monetary policy tightening easing "
        "rate hike rate cut basis points federal funds rate central bank"
    ),
    "FOMC meeting minutes": (
        "FOMC Federal Open Market Committee meeting minutes monetary policy discussion "
        "inflation outlook economic projections dot plot hawkish dovish"
    ),
    "inflation CPI report": (
        "inflation consumer price index CPI report core inflation headline inflation "
        "price pressures cost of living purchasing power Fed target 2 percent"
    ),
    "unemployment jobs report": (
        "unemployment nonfarm payrolls jobs report labor market employment BLS "
        "job creation unemployment rate jobless claims labor force participation"
    ),
    "GDP growth quarterly": (
        "GDP gross domestic product quarterly growth economic expansion contraction "
        "recession growth rate annualized real GDP economic output"
    ),
    "earnings season guidance": (
        "earnings season quarterly results EPS revenue guidance outlook beat miss "
        "forward guidance analyst estimates earnings per share profit"
    ),
    "stock buyback announced": (
        "stock buyback share repurchase program announced return of capital shareholder "
        "value buyback authorization treasury stock reduction in shares outstanding"
    ),
    "dividend increase announced": (
        "dividend increase announced dividend raise quarterly dividend payout "
        "shareholder return yield income dividend growth policy"
    ),
    "merger acquisition announced": (
        "merger acquisition deal announced M&A takeover buyout strategic acquisition "
        "purchase price premium synergies regulatory approval antitrust"
    ),
    "SEC investigation subpoena": (
        "SEC investigation subpoena Securities Exchange Commission inquiry enforcement "
        "action regulatory probe securities fraud accounting irregularities"
    ),
    "short seller report": (
        "short seller report short selling activist short attack fraud allegations "
        "accounting manipulation overvalued stock short interest"
    ),
    "insider selling filing": (
        "insider selling Form 4 SEC filing executive director sell shares insider "
        "transaction stock sale 10b5-1 plan insider activity"
    ),
    "credit rating downgrade": (
        "credit rating downgrade Moody's S&P Fitch rating action outlook negative "
        "watch downgrade junk speculative grade investment grade debt"
    ),
    "bankruptcy filing Chapter 11": (
        "bankruptcy filing Chapter 11 restructuring insolvency debt restructuring "
        "creditor protection reorganization liquidation distressed company default"
    ),
    # Geopolitical
    "Iran sanctions oil embargo": (
        "Iran sanctions oil embargo crude oil exports banned restricted OFAC "
        "secondary sanctions energy sector geopolitical supply disruption"
    ),
    "Strait of Hormuz shipping blockade": (
        "Strait of Hormuz shipping blockade tanker oil transport disruption "
        "Persian Gulf maritime security choke point crude oil LNG passage"
    ),
    "OPEC production cut": (
        "OPEC production cut output reduction supply restriction cartel decision "
        "barrel per day quota oil price support Saudi Arabia UAE"
    ),
    "strategic petroleum reserve release": (
        "strategic petroleum reserve SPR release emergency oil stockpile IEA "
        "coordinated release crude supply shock price cap energy security"
    ),
    "military strike attack": (
        "military strike attack bombing airstrike missile offensive military "
        "action conflict escalation war geopolitical risk premium"
    ),
    "defense spending increase": (
        "defense spending increase military budget appropriations NATO two percent "
        "GDP defense contracts weapons procurement government defense"
    ),
    "NATO alliance response": (
        "NATO alliance response collective defense article five commitment "
        "military cooperation alliance solidarity European defense"
    ),
    "trade war tariffs imposed": (
        "trade war tariffs imposed import duties trade restrictions retaliatory "
        "tariffs protectionism WTO dispute supply chain cost inflation"
    ),
    "supply chain disruption": (
        "supply chain disruption shortage bottleneck logistics delay inventory "
        "semiconductor manufacturing reshoring nearshoring production halt"
    ),
    # Monetary policy
    "Fed rate hike pause cut": (
        "Federal Reserve interest rate hike pause cut monetary policy FOMC "
        "federal funds rate basis points tightening easing pivot"
    ),
    "quantitative tightening QT": (
        "quantitative tightening QT balance sheet reduction Fed reserve "
        "bond runoff liquidity drain Treasury MBS maturity"
    ),
    "yield curve inversion": (
        "yield curve inversion 2-10 spread inverted recession indicator "
        "Treasury bonds short term long term rates economic slowdown"
    ),
    "inflation expectations anchored": (
        "inflation expectations anchored breakeven rates TIPS CPI forecast "
        "price stability mandate credibility consumer expectations"
    ),
    "dollar strength DXY": (
        "dollar strength DXY US dollar index currency appreciation forex "
        "emerging market pressure commodity prices dollar rally"
    ),
    "treasury auction demand": (
        "treasury auction demand bid-to-cover foreign buyers primary dealers "
        "bond auction tail Treasury issuance fiscal deficit financing"
    ),
    "bank reserves liquidity": (
        "bank reserves liquidity federal reserve reserves repo market SOFR "
        "overnight rates money market funding stress banking system"
    ),
    # Market structure
    "margin call liquidation": (
        "margin call liquidation forced selling deleveraging fund reduction "
        "prime brokerage collateral market stress volatility spike"
    ),
    "options expiration OPEX": (
        "options expiration OPEX monthly quarterly max pain gamma exposure "
        "dealer hedging pinning market structure options market"
    ),
    "short squeeze gamma": (
        "short squeeze gamma squeeze retail meme stock short interest covering "
        "options delta hedging forced buying momentum volatility"
    ),
    "ETF rebalancing flows": (
        "ETF rebalancing flows index rebalance passive investing fund flows "
        "quarterly rebalancing buy sell pressure sector rotation"
    ),
    "index reconstitution": (
        "index reconstitution S&P 500 Russell 2000 MSCI addition removal "
        "index inclusion passive fund buying forced rebalance"
    ),
    "credit spread widening": (
        "credit spread widening investment grade high yield corporate bonds "
        "risk premium default risk economic slowdown recession fear"
    ),
    "high yield distressed": (
        "high yield distressed debt junk bonds CCC rated credit stress "
        "covenant breach default risk leveraged loans financial distress"
    ),
    # Corporate events
    "guidance lowered revised": (
        "guidance lowered revised earnings warning profit warning downside "
        "revenue miss below expectations forward guidance cut analyst"
    ),
    "layoffs workforce reduction": (
        "layoffs workforce reduction headcount cuts job cuts restructuring "
        "employees terminated downsizing cost cutting efficiency"
    ),
    "share dilution offering": (
        "share dilution equity offering secondary offering ATM program "
        "dilutive share count increase capital raise new shares"
    ),
    "debt covenant breach": (
        "debt covenant breach violation leverage ratio restriction lender "
        "waiver amendment default risk credit agreement breach"
    ),
    "activist investor stake": (
        "activist investor stake 13D filing hedge fund campaign board seat "
        "strategic alternatives spinoff buyback pressure shareholder"
    ),
    "proxy fight board": (
        "proxy fight board of directors shareholder vote contested election "
        "activist hedge fund governance change management replacement"
    ),
    "spinoff divestiture": (
        "spinoff divestiture separation business unit carve-out asset sale "
        "strategic review value unlock shareholder return independent"
    ),
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _extract_item1(text: str, max_words: int = 2000) -> str:
    """
    Extract the Business section (Item 1) from raw 10-K text.
    Searches for "ITEM 1" header and reads until "ITEM 1A" or "ITEM 2".
    Falls back to the first max_words words of the document.
    """
    # Look for Item 1 Business header
    match = re.search(r"item\s+1[\.\s]+business", text, re.IGNORECASE)
    if match:
        start = match.end()
        # Find the end of the section (Item 1A or Item 2)
        end_match = re.search(r"item\s+(?:1a|2)\b", text[start:], re.IGNORECASE)
        if end_match:
            section_text = text[start: start + end_match.start()]
        else:
            section_text = text[start: start + 100_000]
        words = section_text.split()
        return " ".join(words[:max_words])

    # Fallback: first max_words of full document
    words = text.split()
    return " ".join(words[:max_words])


def _find_filing_text(ticker_dir: Path) -> str:
    """
    Walk the sec-edgar-downloader directory tree for a ticker and return
    the text of the most recent 10-K primary document.
    Returns empty string if nothing is found.
    """
    if not ticker_dir.exists():
        return ""

    for submission_dir in sorted(ticker_dir.iterdir(), reverse=True):
        if not submission_dir.is_dir():
            continue
        # Prefer the primary-document file
        primary = submission_dir / "primary-document.txt"
        if primary.exists():
            return primary.read_text(encoding="utf-8", errors="replace")
        # Fallback: any .txt file in the submission directory
        txt_files = list(submission_dir.glob("*.txt"))
        if txt_files:
            return txt_files[0].read_text(encoding="utf-8", errors="replace")

    return ""


# ---------------------------------------------------------------------------
# Main builder
# ---------------------------------------------------------------------------

def build(download_dir: str | None = None) -> None:
    """
    Download 10-K filings for all tickers in TICKERS, embed the business
    description, and save the asset library to ASSET_LIBRARY_PATH.
    """
    import tempfile
    if download_dir is None:
        download_dir = str(Path(tempfile.gettempdir()) / "sec_filings")

    Path(download_dir).mkdir(parents=True, exist_ok=True)

    # Load embedding model
    embedder = MiniLMEmbedder(settings)
    expected_dim = embedder.dimension()
    logger.info(
        "Building asset library: %d tickers, embedding dim=%d, mode=%s",
        len(TICKERS), expected_dim, settings.EMBEDDING_MODE,
    )

    # Initialize sec-edgar-downloader
    try:
        from sec_edgar_downloader import Downloader
        try:
            dl = Downloader("NarrativeIntelligenceEngine", settings.SEC_EDGAR_EMAIL, download_dir)
        except TypeError:
            # sec-edgar-downloader v5+ does not accept a download path in constructor
            dl = Downloader("NarrativeIntelligenceEngine", settings.SEC_EDGAR_EMAIL)
    except ImportError:
        logger.error(
            "sec-edgar-downloader not installed. "
            "Run: pip install sec-edgar-downloader"
        )
        sys.exit(1)

    library: dict[str, dict] = {}
    failed: list[str] = []
    total = len(TICKERS)

    for i, (ticker, company_name) in enumerate(TICKERS.items(), start=1):
        try:
            logger.info("[%d/%d] Processing %s (%s)", i, total, ticker, company_name)

            # Download the most recent 10-K filing (skip if already cached)
            try:
                dl.get("10-K", ticker, limit=1)
            except Exception as dl_exc:
                logger.warning("Download failed for %s: %s — skipping", ticker, dl_exc)
                failed.append(ticker)
                continue

            # Locate the downloaded filing text
            filings_root = Path(download_dir) / "sec-edgar-filings" / ticker / "10-K"
            text_content = _find_filing_text(filings_root)

            if not text_content.strip():
                logger.warning("No 10-K text found for %s — skipping", ticker)
                failed.append(ticker)
                continue

            # Extract Item 1 Business section
            business_text = _extract_item1(text_content)
            if not business_text.strip():
                logger.warning("No business section extracted for %s — falling back to name", ticker)
                business_text = f"{company_name} is a publicly traded company."

            # Embed and L2-normalize
            try:
                embedding = embedder.embed_single(business_text).astype(np.float32)
            except Exception as emb_exc:
                logger.error("Embedding failed for %s: %s — skipping", ticker, emb_exc)
                failed.append(ticker)
                continue

            norm = np.linalg.norm(embedding)
            if norm > 0:
                embedding = embedding / norm

            # Dimension check
            if embedding.shape[0] != expected_dim:
                logger.error(
                    "Dimension mismatch for %s: got %d, expected %d — skipping",
                    ticker, embedding.shape[0], expected_dim,
                )
                failed.append(ticker)
                continue

            library[ticker] = {
                "name": company_name,
                "embedding": embedding,
            }
            logger.info("  OK: %s (dim=%d)", ticker, embedding.shape[0])

        except Exception as exc:
            logger.error("Unexpected error for %s: %s — skipping", ticker, exc)
            failed.append(ticker)

    # --- Embed ETFs, crypto, and other non-filing assets ---
    logger.info("Embedding %d non-filing assets (ETFs, crypto)...", len(NON_FILING_ASSETS))
    non_filing_added = 0
    for symbol, data in NON_FILING_ASSETS.items():
        try:
            embedding = embedder.embed_single(data["text"]).astype(np.float32)
            norm = np.linalg.norm(embedding)
            if norm > 0:
                embedding = embedding / norm
            library[symbol] = {"name": data["name"], "embedding": embedding}
            logger.info("  OK (non-filing): %s", symbol)
            non_filing_added += 1
        except Exception as exc:
            logger.warning("Non-filing embed failed for %s: %s — skipping", symbol, exc)
            failed.append(symbol)

    # --- Embed macro/event topics ---
    logger.info("Embedding %d macro topics...", len(MACRO_TOPICS))
    topics_added = 0
    for topic_name, topic_text in MACRO_TOPICS.items():
        key = f"TOPIC:{topic_name}"
        try:
            embedding = embedder.embed_single(topic_text).astype(np.float32)
            norm = np.linalg.norm(embedding)
            if norm > 0:
                embedding = embedding / norm
            library[key] = {"name": topic_name, "embedding": embedding}
            logger.info("  OK (topic): %s", topic_name)
            topics_added += 1
        except Exception as exc:
            logger.warning("Topic embed failed for '%s': %s — skipping", topic_name, exc)
            failed.append(key)

    # Save the library
    output_path = Path(settings.ASSET_LIBRARY_PATH)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "wb") as f:
        pickle.dump(library, f)

    logger.info(
        "Asset library saved: path=%s assets=%d failed=%d",
        output_path, len(library), len(failed),
    )
    logger.info(
        "Breakdown: SEC tickers=%d | ETFs/crypto=%d | topics=%d | failed=%d",
        len(library) - non_filing_added - topics_added,
        non_filing_added, topics_added, len(failed),
    )

    # Validation
    logger.info(
        "Validation: embedding_dim=%d (expected %d for EMBEDDING_MODE='%s')",
        expected_dim, expected_dim, settings.EMBEDDING_MODE,
    )
    if failed:
        logger.warning("Failed (%d): %s", len(failed), failed)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    parser = argparse.ArgumentParser(description="Build asset embedding library from SEC 10-K filings.")
    parser.add_argument(
        "--download-dir",
        default=None,
        help="Directory to download SEC filings into (default: system temp dir)",
    )
    args = parser.parse_args()
    build(download_dir=args.download_dir)
