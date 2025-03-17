import os
import pandas as pd
from datetime import datetime


class CacheManager:
    def __init__(self, cache_dir="cache", default_ttl=86400):
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)

    def get(self, key, ttl_hours=24):
        path = os.path.join(self.cache_dir, f"{key}.parquet")
        if os.path.exists(path):
            file_age = datetime.now() - datetime.fromtimestamp(os.path.getmtime(path))
            if file_age.total_seconds() < ttl_hours * 3600:
                return pd.read_parquet(path)
        return None

    def get_safe(self, key, ttl=None):
        """Get cached data or empty DataFrame if corrupt"""
        try:
            return self.get(key, ttl) or pd.DataFrame()
        except:
            return pd.DataFrame()

    def set(self, key, data):
        path = os.path.join(self.cache_dir, f"{key}.parquet")
        data.to_parquet(path)