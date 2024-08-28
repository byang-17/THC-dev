import pandas as pd
import os
from datetime import datetime
import numpy as np

# value signals
def get_credit_value_signal(data,
                            sec_id = ['Cusip', 'ISIN'],
                            control_factors = ['Sector', 'Rating']):

  data_copy = data[['Date'] + sec_id + control_factors + ['OAS']]
  data_copy['Signal'] = data_copy.groupby(['Date'] + control_factors)['OAS'].transform(lambda x: (x - x.mean()) / x.std())
  data_copy.loc[data_copy['Signal'].isna(), 'Signal'] = 0

  signal_data = data_copy[['Date'] + sec_id + ['Signal']]
  return(signal_data)

def get_credit_lowvol_signal(data, 
                             sec_id = ['Cusip', 'ISIN'], 
                             control_factors = ['Sector']):
  
  data_copy = data[['Date'] + sec_id + control_factors + ['Dur']]
  data_copy['Signal'] = -1 * data_copy['Dur']
  signal_data = data_copy[['Date'] + sec_id + ['Signal']]
  return(signal_data)


