from enum import Enum


class Metrics(Enum):
    """Enumeration for the different metrics computed from resale prices data."""
    MIN_PRICE = 'Minimum Price'
    AVG_PRICE = 'Average Price'
    STDDEV = 'Standard Deviation of Price'
    MIN_PRICE_PER_SQM = 'Minimum Price per Square Meter'
