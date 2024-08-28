from utils.db_manager import *
class data_loader:
    
    def __init__(self, db_manager, asset: str, universe: str, start_date = None, end_date = None, freq: str = None):

        self.db_manager = db_manager
        self.asset = asset
        self.universe = universe
        self.start_date = start_date
        self.end_date = end_date
        self.freq = freq
        
    def load_equity_data(self, tickers = None):
        
        table_name = f"`{self.db_manager.project_id}.SHARADAR.SEP`"
        
        # Base query
        db_query = f"""
                    SELECT * FROM {table_name}
                    WHERE date BETWEEN @start_date AND @end_date
                   """
                   
        query_params = [
            bigquery.ScalarQueryParameter("start_date", "DATE", self.start_date),
            bigquery.ScalarQueryParameter("end_date", "DATE", self.end_date)
        ]        
        
        if tickers:
            db_query += " AND ticker IN UNNEST(@tickers)"
            query_params.append(bigquery.ArrayQueryParameter("tickers", "STRING", tickers))
        
        job_config = bigquery.QueryJobConfig(query_parameters = query_params)
        result = self.db_manager.query(db_query, job_config)
        return(result)
        
    def load_credit_data(self, tickers = None):

        if self.universe == 'USIG':
            table_name = f"`{self.db_manager.project_id}.Derived_Analytics.Temp_Bond_Analytics`"
        elif self.universe == 'CAN':
            table_name = f"`{self.db_manager.project_id}.ETFs.XCB_TEMP`"
        else:
            raise ValueError('Currently only supports universe "USIG" or "CAN".')
        
        # Base query
        db_query = f"""
                    SELECT * FROM {table_name}
                    
                   """
                   
        query_params = [
            #bigquery.ScalarQueryParameter("start_date", "DATE", self.start_date),
            #bigquery.ScalarQueryParameter("end_date", "DATE", self.end_date)
        ]        
        
        if tickers:
            db_query += " AND Ticker IN UNNEST(@tickers)"
            query_params.append(bigquery.ArrayQueryParameter("tickers", "STRING", tickers))
        
        job_config = bigquery.QueryJobConfig(query_parameters = query_params)
        result = self.db_manager.query(db_query, job_config)
        return(result)
        

