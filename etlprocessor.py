"""
    ETLProcessor module with class implementation with functions to read, transform, and model data. 
    Uses a Database class to manage database operations.

    @author: Defne Demirtuerk

"""

import yaml
import pandas as pd
from database import Database
import os

class ETLProcessor:
    def __init__(self):
        # store the config file path
        self.config_file = os.path.join(os.path.dirname(__file__), 'config.yaml')
        try:
            cfg = yaml.safe_load(open(self.config_file))
            self.raw_path = os.path.join(os.path.dirname(__file__), cfg['paths']['raw_data'])
        except Exception as e:
            print(f"Error loading config file '{self.config_file}': {e}")
            raise
        # create the database manager object with config details
        try:
            self.db = Database(self.config_file)
        except Exception as e:
            print(f"Error initializing Database: {e}")
            raise

    def read_raw_data(self):
        # Read raw JSON logs into a pandas DataFrame
        try:
            df = pd.read_json(self.raw_path)
            return df
        except Exception as e:
            print(f"Error reading raw data from '{self.raw_path}': {e}")
            raise

    def transform(self, df):
        """
        Clean up and transform the DataFrame
        """
        # Remove entries with missing user_id or action_type
        df = df[df['user_id'].notna() & df['action_type'].notna()]

        # change to ISO timestamps
        df['timestamp'] = pd.to_datetime(df['timestamp']).dt.strftime("%Y-%m-%dT%H:%M:%S")

        # read in and store metadata columns, device and location
        if 'metadata' in df.columns:
            metadata_df = pd.json_normalize(df['metadata'])
            metadata_df.index = df.index  # align 
            df = pd.concat([df.drop(columns=['metadata']), metadata_df], axis=1)
        
        # Drop duplicate rows based on all columns
        df = df.drop_duplicates()

        return df

    def model(self, df):
        """
        Create dimension and fact dataframes
        """
        # Users dimension
        df_users = df[['user_id']].drop_duplicates().reset_index(drop=True)
        # Actions dimension
        df_actions = df[['action_type']].drop_duplicates().reset_index(drop=True)
        # Fact user anctions table
        df_fact = df[['user_id', 'action_type', 'timestamp', 'device', 'location']]

        return df_users, df_actions, df_fact
    
    def run(self):
        """
        Run full ETL pipeline.
        """
        try:
            # read json data
            df_raw = self.read_raw_data()

            # clean up and transform data
            df_clean = self.transform(df_raw)
            df_users, df_actions, df_fact = self.model(df_clean)

            # Create tables using database manager object
            schema_file = os.path.join(os.path.dirname(__file__), 'create_tables.sql')
            self.db.create_tables(schema_file)
            cfg = yaml.safe_load(open(self.config_file))
            tables_cfg = cfg['tables']

            # load data to tables in postgresql database using database manager object 
            dim_users_pk = tables_cfg['dim_users'].get('primary_key')
            self.db.load_dataframe(df_users, tables_cfg['dim_users']['target_table'], pk_col=dim_users_pk)
            dim_actions_pk = tables_cfg['dim_actions'].get('primary_key')
            self.db.load_dataframe(df_actions, tables_cfg['dim_actions']['target_table'], pk_col=dim_actions_pk)
            fact_pk = tables_cfg['fact_user_actions'].get('primary_key')
            self.db.load_dataframe(df_fact, tables_cfg['fact_user_actions']['target_table'], pk_col=fact_pk)
            print("ETL pipeline completed successfully.")
        except Exception as e:
            print(f"ETL pipeline failed: {e}")
            raise

if __name__ == '__main__':
    ETLProcessor().run()
