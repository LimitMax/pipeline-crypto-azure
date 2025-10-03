import re

# Peta regex ke symbol
COIN_MAP = {
    r"\b(bitcoin|btc)\b": "BTC-USD",
    r"\b(ethereum|eth)\b": "ETH-USD",
    r"\b(solana|sol)\b": "SOL-USD",
    r"\bxrp\b": "XRP-USD",
    r"\bdoge|dogecoin\b": "DOGE-USD"
}

def detect_coin(text: str) -> str:
    """Deteksi coin dari teks berita (title + content)"""
    text = str(text).lower()
    for pattern, coin in COIN_MAP.items():
        if re.search(pattern, text):
            return coin
    return "ALL"
