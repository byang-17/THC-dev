from utils.data_loader import * 
from flask import Flask, request, jsonify

def FI_indexing(request):
    
    # need to write YAML files and get the associated parameters
    # YAML file to context
    # 
    request_args = request.get_json(silent=True)
    db_mana = db_manager(project_id = "analytics-lakehouse-428212")
    dt_loader = data_loader(db_mana, 
                            asset = "eq",
                            start_date = '2024-07-01',
                            end_date = '2024-07-01',
                            freq = "daily")
    index_data = dt_loader.load_equity_data()
    index_data = jsonify(index_data)
    return(index_data)

"""
need to write - 
 1. data loader class for loading data based on config
 2. write yaml file, load to get the necessary configs
 3. context object class for storing config and output data
 4. compute signals, trades, and optimize. 
 

"""
