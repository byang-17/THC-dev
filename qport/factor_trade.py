import pandas as pd
import numpy as np

def get_factor_trade(signal_data,
                     analytics_data,
                     sec_id = ['Cusip', 'ISIN'],
                     filters = {},
                     control_factors = [],
                     trade_dir = 'ls',
                     long_pct = [0.8, 1],
                     short_pct = [0, 0.2],
                     weighting_scheme = 'mv',
                     norm_factors = [],
                     rebalance_freq = 'monthly',
                     holding_period = 1,
                     trade_lag = 1):

    full_data = pd.merge(signal_data, analytics_data, on = ['Date'] + sec_id, how = 'left')

    # Apply filters if they exist
    filtered_list = []
    if filters:
        for filter_col, filter_bar in filters.items():
            full_data_filtered = full_data[full_data[filter_col] >= filter_bar]
            filtered_data_temp = full_data.loc[full_data[filter_col] <= filter_bar, ['Date'] + sec_id]
            filtered_data_temp['Filter'] = filter_col
            filtered_list.append(filtered_data_temp)
        filtered_data = pd.concat(filtered_list, ignore_index=True)
    else:
        full_data_filtered = full_data.copy()
        filtered_data = pd.DataFrame()

    # Create rank and bucket
    full_data_filtered['Rank'] = full_data_filtered.groupby(['Date'] + control_factors)['Signal'].transform(lambda x: x.rank(pct=True, method='dense'))

    # Trade rule
    full_data_filtered['Trade_Dir'] = 0
    if trade_dir == 'ls':
        full_data_filtered.loc[(full_data_filtered['Rank'] >= long_pct[0]) & (full_data_filtered['Rank'] <= long_pct[1]), 'Trade_Dir'] = 1
        full_data_filtered.loc[(full_data_filtered['Rank'] >= short_pct[0]) & (full_data_filtered['Rank'] <= short_pct[1]), 'Trade_Dir'] = -1
    elif trade_dir == 'lo':
        full_data_filtered.loc[(full_data_filtered['Rank'] >= long_pct[0]) & (full_data_filtered['Rank'] <= long_pct[1]), 'Trade_Dir'] = 1

    # Compute holdings weight for each leg
    trade_data = full_data_filtered.loc[full_data_filtered['Trade_Dir'] != 0]
    if weighting_scheme == 'equal':
        trade_data['Weight_Bucket'] = 1 / trade_data.groupby(['Date', 'Trade_Dir'])['Rank'].transform('count')
    elif weighting_scheme == 'mv':
        trade_data['Weight_Bucket'] = trade_data.groupby(['Date', 'Trade_Dir'])['Market_Value_Percent'].transform(lambda x: x / x.sum())
    elif weighting_scheme == 'linear':
        trade_data['Weight_Bucket'] = trade_data.groupby(['Date', 'Trade_Dir'])['Rank'].transform(lambda x: x / x.sum())
    else:
        raise ValueError('weighting_scheme has to be either "equal", "mv", or "linear"')

    # Normalize to benchmark if norm_factors are provided
    if norm_factors:
        weight_bucket_by_norm_factor = trade_data.groupby(['Date', 'Trade_Dir'] + norm_factors).agg(Weight_Bucket_by_Norm_Factor=('Weight_Bucket', sum)).reset_index()
        weight_benchmark_by_norm_factor = full_data.groupby(['Date'] + norm_factors).agg(Weight_Benchmark_by_Norm_Factor=('Market_Value_Percent', sum)).reset_index()
        weight_renorm = pd.merge(weight_bucket_by_norm_factor,
                                 weight_benchmark_by_norm_factor,
                                 on = ['Date'] + norm_factors,
                                 how = 'left')
        weight_renorm['Renorm_Constant'] = np.where(weight_renorm['Weight_Bucket_by_Norm_Factor'] != 0,
                                                    weight_renorm['Weight_Benchmark_by_Norm_Factor'] / weight_renorm['Weight_Bucket_by_Norm_Factor'],
                                                    0)
        trade_data = pd.merge(trade_data, 
                              weight_renorm, 
                              on = ['Date', 'Trade_Dir'] + norm_factors, 
                              how = 'left')
        trade_data['Weight_Bucket'] = trade_data['Weight_Bucket'] * trade_data['Renorm_Constant']

    # Compute final portfolio weight
    trade_data['Weight'] = trade_data['Weight_Bucket'] * trade_data['Trade_Dir']

    # Create trade open and close dates
    if rebalance_freq == 'monthly':
        trade_data['Trade_Open'] = trade_data['Date'] + pd.offsets.MonthEnd(trade_lag)
        trade_data['Trade_Close'] = trade_data['Trade_Open'] + pd.offsets.MonthEnd(holding_period)
    elif rebalance_freq == 'daily':
        trade_data['Trade_Open'] = trade_data['Date'] + pd.offsets.Day(trade_lag)
        trade_data['Trade_Close'] = trade_data['Date'] + pd.offsets.Day(holding_period)

    # Clean and output data
    trade_data_full = pd.merge(trade_data, full_data[['Date'] + sec_id], on = ['Date'] + sec_id, how = 'outer')
    trade_data_full['Weight'].fillna(0, inplace = True)
    trade_data_full = trade_data_full[['Date'] + sec_id + ['Rank', 'Trade_Dir', 'Weight', 'Trade_Open', 'Trade_Close']]

    output = {'trade_data': trade_data_full, 'filtered_data': filtered_data}
    return output

# this is the research function version 
# where we generate full spectrum quantile 
# factor portfolios based on the signal
def get_factor_quantiles(signal_data,
                         analytics_data,
                         sec_id = ['Cusip', 'ISIN'],
                         filters = {'key': 0.05},
                         control_factors = [],
                         n_quantiles = 5,
                         weighting_scheme = 'mv',
                         norm_factors = [],
                         rebalance_freq = 'monthly',
                         holding_period = 1,
                         trade_lag = 0):

    full_data = pd.merge(signal_data, analytics_data, on=['Date'] + sec_id, how='left')

    # Apply filters if they exist
    filtered_list = []
    if filters:
        for filter_col, filter_bar in filters.items():
            full_data_filtered = full_data[full_data[filter_col] >= filter_bar]
            filtered_data_temp = full_data.loc[full_data[filter_col] <= filter_bar, ['Date'] + sec_id]
            filtered_data_temp['Filter'] = filter_col
            filtered_list.append(filtered_data_temp)
        filtered_data = pd.concat(filtered_list, ignore_index=True)
    else:
        full_data_filtered = full_data.copy()
        filtered_data = pd.DataFrame()

    # Create rank and bucket
    full_data_filtered['Rank'] = full_data_filtered.groupby(['Date'] + control_factors)['Signal'].transform(lambda x: x.rank(pct=True, method='dense'))
    full_data_filtered['Bucket'] = pd.cut(full_data_filtered['Rank'], bins=n_quantiles, labels=range(1, n_quantiles + 1))

    # Compute holdings weight for each leg
    if weighting_scheme == 'equal':
        full_data_filtered['Weight_Bucket'] = 1 / full_data_filtered.groupby(['Date', 'Bucket'])['Rank'].transform('count')
    elif weighting_scheme == 'mv':
        full_data_filtered['Weight_Bucket'] = full_data_filtered.groupby(['Date', 'Bucket'])['Market_Value_Percent'].transform(lambda x: x / x.sum())
    elif weighting_scheme == 'linear':
        full_data_filtered['Weight_Bucket'] = full_data_filtered.groupby(['Date', 'Bucket'])['Rank'].transform(lambda x: x / x.sum())
    else:
        raise ValueError('weighting_scheme has to be either "equal", "mv", or "linear"')

    # Normalize to benchmark if norm_factors are provided
    if norm_factors:
        weight_bucket_by_norm_factor = full_data_filtered.groupby(['Date', 'Bucket'] + norm_factors).agg(Weight_Bucket_by_Norm_Factor=('Weight_Bucket', sum)).reset_index()
        weight_benchmark_by_norm_factor = full_data.groupby(['Date'] + norm_factors).agg(Weight_Benchmark_by_Norm_Factor=('Market_Value_Percent', sum)).reset_index()
        weight_renorm = pd.merge(weight_bucket_by_norm_factor,
                                 weight_benchmark_by_norm_factor,
                                 on = ['Date'] + norm_factors,
                                 how = 'left')
        weight_renorm['Renorm_Constant'] = np.where(weight_renorm['Weight_Bucket_by_Norm_Factor'] != 0,
                                                    weight_renorm['Weight_Benchmark_by_Norm_Factor'] / weight_renorm['Weight_Bucket_by_Norm_Factor'],
                                                    0)
        full_data_filtered = pd.merge(full_data_filtered, 
                                      weight_renorm, 
                                      on = ['Date', 'Bucket'] + norm_factors, 
                                      how = 'left')
        full_data_filtered['Weight_Bucket'] = full_data_filtered['Weight_Bucket'] * full_data_filtered['Renorm_Constant']

    # Create trade open and close dates
    if rebalance_freq == 'monthly':
        full_data_filtered['Trade_Open'] = full_data_filtered['Date'] + pd.offsets.MonthEnd(trade_lag)
        full_data_filtered['Trade_Close'] = full_data_filtered['Trade_Open'] + pd.offsets.MonthEnd(holding_period)
    elif rebalance_freq == 'daily':
        full_data_filtered['Trade_Open'] = full_data_filtered['Date'] + pd.offsets.Day(trade_lag)
        full_data_filtered['Trade_Close'] = full_data_filtered['Date'] + pd.offsets.Day(holding_period)

    # Prepare final output data
    trade_data = full_data_filtered[['Date'] + sec_id + ['Rank', 'Bucket', 'Weight_Bucket', 'Trade_Open', 'Trade_Close']]
    
    # Clean and finalize output
    trade_data_full = trade_data.copy()
    trade_data_full['Weight_Bucket'].fillna(0, inplace = True)

    output = {'trade_data': trade_data_full, 'filtered_data': filtered_data}
    return output