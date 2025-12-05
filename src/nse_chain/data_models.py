from dataclasses import dataclass

@dataclass
class OptionRow:
    timestamp: str
    underlying: str
    expiry: str
    strike: float
    option_type: str
    ltp: float
    iv: float
    oi: float
    oi_change: float
    volume: float
    ltp_prev: float

@dataclass
class DerivedStrikeMetrics:
    strike: float
    ce_oi: float
    pe_oi: float
    ce_oi_change: float
    pe_oi_change: float
    ce_price_change: float
    pe_price_change: float
    total_oi: float
    oi_diff: float
    oi_ratio: float
    ce_signal: str
    pe_signal: str
    oi_signal: str
  
