from db_manager import *
class data_loader:
    
    def __init__(self, db_manager, asset: str, start_date, end_date, freq: str):

        self.db_manager = db_manager
        self.asset = asset
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
        


        

