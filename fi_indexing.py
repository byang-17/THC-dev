import qport as qp
from utils.db_manager import *
from utils.data_loader import *
import yaml
import pandas as pd

# Load configuration and data
db_mana = db_manager(project_id = 'analytics-lakehouse-428212')
dt_loader = data_loader(db_mana, 
                        asset = 'credit', 
                        universe = 'CAN',
                        start_date = '2024-07-18',
                        end_date = '2024-07-18', 
                        freq = 'daily')
index_data = dt_loader.load_credit_data()
index_data.rename(columns = {'date' : 'Date', 
                             'ratings_sp' : 'Rating', 
                             'cusip' : 'Cusip'}, inplace = True)
index_data['DTS_Bucket'] = pd.cut(index_data['DTS'], bins = 10, labels = range(1, 11))
index_data['Market_Value_Percent'] = index_data['Weight _%_']
index_data['Market_Value_Percent'] = index_data['Market_Value_Percent'] / index_data['Market_Value_Percent'].sum()
index_data['Date'] = pd.to_datetime(index_data['Date'], format = '%Y%m%d')

# load config
file_path = 'config.yaml'
with open(file_path, 'r') as file:
    config = yaml.safe_load(file)

# Initialize context
context = qp.Context(config = config, input_data = index_data)

# Initialize and run the portfolio manager
port_manager = qp.PortfolioManager(context, qp.FactorStrategy)
port_manager.run()





















temp_trade = port_manager.context.portfolio_data['trade_data']
temp_trade_analytics = pd.merge(temp_trade, 
                                index_data[['Date', 'Cusip', 'ISIN', 'OAS', 'Dur', 'DTS', 'Sector', 'Rating', 'Market_Value_Percent']], 
                                on = ['Date', 'Cusip', 'ISIN'], 
                                how = 'left')
temp_trade_analytics['Weight_Active'] = temp_trade_analytics['Weight_Optimized'] - temp_trade_analytics['Market_Value_Percent']                               
temp_trade_analytics.groupby(['Sector'])['Weight_Active'].sum()
temp_trade_analytics.groupby(['Rating'])['Weight_Active'].sum()
sum(temp_trade_analytics['Weight_Active'] * temp_trade_analytics['OAS'])


print((temp_trade_analytics[temp_trade_analytics['Trade_Dir'] == 1]['OAS'] * temp_trade_analytics[temp_trade_analytics['Trade_Dir'] == 1]['Weight'] / 100).sum())
print((temp_trade_analytics[temp_trade_analytics['Trade_Dir'] == -1]['OAS'] * temp_trade_analytics[temp_trade_analytics['Trade_Dir'] == -1]['Market_Value_Percent'] / 100).sum())
print((temp_trade_analytics['OAS'] * temp_trade_analytics['Market_Value_Percent'] / 100).sum())

print((temp_trade_analytics[temp_trade_analytics['Trade_Dir'] == 1]['DTS'] * temp_trade_analytics[temp_trade_analytics['Trade_Dir'] == 1]['Weight'] / 100).sum())
print((temp_trade_analytics[temp_trade_analytics['Trade_Dir'] == -1]['DTS'] * temp_trade_analytics[temp_trade_analytics['Trade_Dir'] == -1]['Market_Value_Percent'] / 100).sum())
print((temp_trade_analytics['DTS'] * temp_trade_analytics['Market_Value_Percent'] / 100).sum())

print((temp_trade_analytics[temp_trade_analytics['Trade_Dir'] == 1]['Dur'] * temp_trade_analytics[temp_trade_analytics['Trade_Dir'] == 1]['Weight'] / 100).sum())
print((temp_trade_analytics[temp_trade_analytics['Trade_Dir'] == -1]['Dur'] * temp_trade_analytics[temp_trade_analytics['Trade_Dir'] == -1]['Market_Value_Percent'] / 100).sum())
print((temp_trade_analytics['Dur'] * temp_trade_analytics['Market_Value_Percent'] / 100).sum())

print((temp_trade_analytics[temp_trade_analytics['Trade_Dir'] == 1].groupby('Sector')['Weight'].sum()))
print((temp_trade_analytics[temp_trade_analytics['Trade_Dir'] == -1].groupby('Sector')['Weight'].sum()))
print(temp_trade_analytics.groupby('Sector')['Market_Value_Percent'].sum())

print((temp_trade_analytics[temp_trade_analytics['Trade_Dir'] == 1].groupby('Rating')['Weight'].sum()))
print((temp_trade_analytics[temp_trade_analytics['Trade_Dir'] == -1].groupby('Rating')['Weight'].sum()))
print(temp_trade_analytics.groupby('Rating')['Market_Value_Percent'].sum())

stop = 1
# from flask import Flask, request, jsonify

# def FI_indexing(request):
    
#     # need to write YAML files and get the associated parameters
#     # YAML file to context
#     # 
#     request_args = request.get_json(silent=True)
    
#     # 1. load data
#     db_mana = db_manager(project_id = 'analytics-lakehouse-428212')
#     dt_loader = data_loader(db_mana, 
#                             asset = 'credit',
#                             start_date = '2024-07-18',
#                             end_date = '2024-07-18',
#                             freq = 'daily')
#     index_data = dt_loader.load_credit_data()


#     # 2. compute signals
    


#     # 3. compute factors



#     # 4. 
    
    
#     index_data = jsonify(index_data)    
#     return(index_data)

