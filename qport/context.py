# context is a storage class, ideally it will take in config 
# and input data for strategies
import pandas as pd
class Context:
    def __init__(self, config: dict = None, input_data: pd.DataFrame = None):
        # input dictionary
        self.config = config

        # input data
        self.input_data = input_data

        # intermediate storage
        self.signals_data = {}
        self.factors_data = {}
        self.multifactor_data = {}
        self.portfolio_data = {}