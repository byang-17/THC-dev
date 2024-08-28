from google.cloud import bigquery
class db_manager:
    """
    db_manager is the class for handling connections to BigQuery.
    """

    def __init__(self, project_id: str, credentials_path: str = None):
        """
        Initializes the BigQuery connection.

        Args:
            project_id (str): The Google Cloud Project ID.
            credentials_path (str, optional): The path to the service account credentials file.
                Defaults to None, which uses the default application credentials.
        """
        self.project_id = project_id
        self.client = bigquery.Client.from_service_account_json(credentials_path) if credentials_path else bigquery.Client(project=project_id)

    def query(self, query: str, job_config: None) -> list:
        """
        Executes a BigQuery query and returns the results.

        Args:
            query (str): The SQL query to execute.

        Returns:
            list: A list of dictionaries representing the query results.
        """
        if job_config:
            query_job = self.client.query(query, job_config)
        else: 
            query_job = self.client.query(query)
        results = query_job.result().to_dataframe()
        return results

    def insert_rows(self, table_id: str, rows: list) -> None:
        """
        Inserts rows into a BigQuery table.

        Args:
            table_id (str): The ID of the BigQuery table.
            rows (list): A list of dictionaries representing the rows to insert.
        """
        errors = self.client.insert_rows_json(table_id, rows)
        if errors:
            print(f"Encountered errors while inserting rows: {errors}")

    def create_table(self, table_id: str, schema: list) -> None:
        """
        Creates a new BigQuery table.

        Args:
            table_id (str): The ID of the BigQuery table.
            schema (list): A list of dictionaries representing the table schema.
                Each dictionary should have 'name' and 'field_type' keys.
        """
        table = bigquery.Table(table_id, schema = schema)
        table = self.client.create_table(table)
        print(f"Created table {table.table_id}")

    def delete_table(self, table_id: str) -> None:
        """
        Deletes a BigQuery table.

        Args:
            table_id (str): The ID of the BigQuery table.
        """
        self.client.delete_table(table_id)
        print(f"Deleted table {table_id}")

    def get_table(self, table_id: str) -> bigquery.Table:
        """
        Retrieves a BigQuery table.

        Args:
            table_id (str): The ID of the BigQuery table.

        Returns:
            bigquery.Table: The BigQuery table object.
        """
        return self.client.get_table(table_id)

    def close(self) -> None:
        """
        Closes the BigQuery connection.
        """
        self.client.close()