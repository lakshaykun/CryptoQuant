# pipelines/transformations/raw/market.py

import json

class RawMarketTransformer:
    @staticmethod
    def transform(df: json) -> json:
        '''Transforms raw market data into a standardized format for kafka.'''
        
        if df is None:
            return None
        
        # Remove the ignore column
        if "ignore" in df:
            del df["ignore"]

        return df