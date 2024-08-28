from .strategy import Strategy
from . import signal_functions
from . import factor_trade
from .portfolio_optimizer import MinActiveExposure
import pandas as pd
import numpy as np

# create strategy templates
# factor strategies
class FactorStrategy(Strategy):

    # define individual execution funcs
    def get_signal(self):   
        if self.context.input_data.empty:
            raise ValueError("Data must be loaded before signal calculations.")
        config_signals = self.context.config['signals']
        for signal_name, signal_params in config_signals.items():
            # retrieve function handlers and params
            signal_func_name = signal_params['function']
            signal_func_params = signal_params['params']
            
            signal_func = getattr(signal_functions, signal_func_name)
            self.context.signals_data[signal_name] = signal_func(self.context.input_data, 
                                                                 **signal_func_params)

    def get_factor(self):
        config_factors = self.context.config['factors']

        for factor_name, factor_params in config_factors.items():
            # retrieve function handlers and params
            signal_name = factor_params['signal']
            factor_func_name = factor_params['function']
            factor_func_params = factor_params['params']
            factor_func = getattr(factor_trade, factor_func_name)
            self.context.factors_data[factor_name] = factor_func(self.context.signals_data[signal_name], 
                                                                 self.context.input_data, 
                                                                 **factor_func_params)
        # print out analytics here?

    def get_multifactor(self):
        # self.context.multifactor_data['value']['allocations_data']
        config_mf = self.context.config['multifactor']
        factor_contribs_ls = []
        for factor_name, factor_weights in zip(config_mf['factors'], config_mf['factor_weights']):
            factor_contrib = self.context.factors_data[factor_name]['trade_data']
            factor_contrib['Weight_Contributions'] = factor_contrib['Weight'] * factor_weights
            factor_contribs_ls += [factor_contrib]
        factor_contribs = pd.concat(factor_contribs_ls, ignore_index=True)
        mf_trade_data = factor_contribs.groupby(['Date', 'Cusip', 'ISIN']).agg(Weight=('Weight_Contributions', 'sum')).reset_index()
        self.context.multifactor_data['trade_data'] = mf_trade_data

    def get_optimport(self):
        config_portfolio = self.context.config['portfolio']
        constraints_params = config_portfolio['constr_params']
        optim_input_data = pd.merge(self.context.multifactor_data['trade_data'], 
                                    self.context.input_data,
                                    on = ['Date', 'Cusip', 'ISIN'],
                                    how = 'left')
        optimizer = MinActiveExposure(data = optim_input_data, 
                                       constr_params = constraints_params)    
        optimizer.build_constraints()
        optimizer.optimize()
        self.context.portfolio_data['trade_data'] = optim_input_data[['Date', 'Cusip', 'ISIN', 'Weight_Optimized']]

    # process execution
    def process(self):
        self.get_signal()
        self.get_factor()
        self.get_multifactor()
        self.get_optimport()








# custom indexing strategies
class CustomIndexing(Strategy):
    
    def sampling(self):
        raise ValueError("Not yet implemented")

    def process(self):
        self.sampling()
    