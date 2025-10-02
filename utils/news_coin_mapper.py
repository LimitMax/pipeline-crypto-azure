def detect_coin(text: str) -> str:
    text = str(text).lower()
    if "bitcoin" in text or "btc" in text:
        return "BTC-USD"
    elif "ethereum" in text or "eth" in text:
        return "ETH-USD"
    elif "solana" in text or "sol" in text:
        return "SOL-USD"
    elif "xrp" in text:
        return "XRP-USD"
    elif "doge" in text:
        return "DOGE-USD"
    else:
        return "ALL"
