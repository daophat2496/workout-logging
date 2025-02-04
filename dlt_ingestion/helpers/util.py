from datetime import datetime, timedelta
import time

def get_sod_n_days_from_now(n_days: int) -> int:
    now = datetime.now()
    sod_n_days_ago = datetime(now.year, now.month, now.day) + timedelta(days=n_days)
    return int(sod_n_days_ago.timestamp())