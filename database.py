"""
    Database Manager module with class implementation with functions to connect to Postgresql database, create tables, load data. 

    @author: Defne Demirtuerk

"""
import yaml
import psycopg2
from psycopg2 import Error

class Database:
    def __init__(self, config_file):
        # Load database config from YAML file
        cfg = yaml.safe_load(open(config_file))['database']
        self.user = cfg['user']
        self.password = cfg['password']
        self.host = cfg['host']
        self.port = cfg['port']
        self.db = cfg['database']

        # Create psycopg2 connection with error handling
        try:
            self.conn = psycopg2.connect(
                dbname=self.db,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
        except Error as e:
            print(f"Error connecting to the PostgreSQL database: {e}")
            self.conn = None
            raise

    def create_tables(self, schema_file):
        """
        Create tables using statements from create_tables.sql schema file
        """
        try:
            with open(schema_file) as f:
                ddl = f.read()
            with self.conn.cursor() as cur:
                for statement in ddl.split(';'):
                    stmt = statement.strip()
                    if stmt:
                        cur.execute(stmt)
                self.conn.commit()
        except Exception as e:
            print(f"Error creating tables from schema file '{schema_file}': {e}")
            if self.conn:
                self.conn.rollback()
            raise

    def load_dataframe(self, df, table_name, pk_col=None):
        """
        Load a pandas DataFrame into the specified database table (includes upsert operation).
        Supports composite primary keys and only updates if the incoming timestamp is more recent.
        """
        # Build the column and value placeholders for SQL
        cols = ','.join(df.columns)
        vals = ','.join(['%s'] * len(df.columns))

        # Determine the primary key(s) for upsert logic
        if pk_col is None:
            pk_col = df.columns[0]  # Default to first column if not provided
        if isinstance(pk_col, list):
            pk_str = ','.join(pk_col)  # Composite key as comma-separated string
            update_cols = [col for col in df.columns if col not in pk_col]  # Columns to update
        else:
            pk_str = pk_col
            update_cols = [col for col in df.columns if col != pk_col]

        # Build the upsert SQL statement
        if update_cols:
            # Update only if the new timestamp is more recent
            update_stmt = ', '.join([f"{col}=EXCLUDED.{col}" for col in update_cols])
            insert_sql = (
                f"INSERT INTO {table_name} ({cols}) VALUES ({vals}) "
                f"ON CONFLICT ({pk_str}) DO UPDATE SET {update_stmt} "
                f"WHERE {table_name}.timestamp < EXCLUDED.timestamp"
            )
        else:
            # If only PK columns, just do nothing on conflict
            insert_sql = f"INSERT INTO {table_name} ({cols}) VALUES ({vals}) ON CONFLICT ({pk_str}) DO NOTHING"

        # Execute the insert/upsert for each row
        try:
            with self.conn.cursor() as cur:
                for row in df.itertuples(index=False, name=None):
                    cur.execute(insert_sql, row)
                self.conn.commit()
        except Exception as e:
            print(f"Error loading data into table '{table_name}': {e}")
            if self.conn:
                self.conn.rollback()
            raise
