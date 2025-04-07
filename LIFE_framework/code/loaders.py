import pandas as pd
import os
import json
import uuid
from datetime import datetime
from code.utils.db_utils import connect_postgres, run_query, insert_dataframe
from code.logger_config import get_logger

logger = get_logger()

class BaseLoader:
    def __init__(self, config_path):
        self.config = self.load_config(config_path)
        self.conn = connect_postgres(self.config["database"])
        self.schema_path = self.config["schema_path"]
        self.col_types = self.apply_custom_schema()
        self.df = pd.DataFrame()

    def load_config(self, path):
        with open(path, 'r') as f:
            return json.load(f)

    def apply_custom_schema(self):
        schema_df = pd.read_csv(self.schema_path)
        col_types = {row["column_name"]: row["dtype"] for _, row in schema_df.iterrows()}
        logger.info(f"Custom schema applied: {col_types}")
        return col_types

    def read_csv(self, file_path):
        logger.info(f"CSV file read with schema: {file_path}")
        return pd.read_csv(file_path, dtype=self.col_types)

    def read_json(self, file_path):
        logger.info(f"JSON file read with schema: {file_path}")
        return pd.read_json(file_path, dtype=self.col_types)

class StageLoader(BaseLoader):
    def run_pipeline(self):
        file_path = self.config["input_file"]
        file_type = self.config["file_type"]
        primary_keys = self.config.get("primary_keys", [])
        nulls_output_path = self.config.get("null_output_file", "data/nulls.csv")
        db_schema = self.config.get("target_db_schema", "stage")
        table = self.config["target_table"]

        if file_type == "csv":
            self.df = self.read_csv(file_path)
        elif file_type == "json":
            self.df = self.read_json(file_path)

        self.df.drop_duplicates(inplace=True)

        if primary_keys:
            null_df = self.df[self.df[primary_keys].isnull().any(axis=1)]
            if not null_df.empty:
                null_df.to_csv(nulls_output_path, index=False)
                logger.info(f"Loaded {len(null_df)} null records of file {file_path} into file {nulls_output_path}")
            self.df = self.df.dropna(subset=primary_keys)
        
        self.df["primary_key"] = [str(uuid.uuid4()) for _ in range(len(self.df))]

        self.df['load_timestamp'] = datetime.now()

        insert_dataframe(self.conn, self.df, table, db_schema)
        logger.info("Stage pipeline completed successfully.")

class ProcessedLoader(BaseLoader):
    def run_pipeline(self):
        db_schema = self.config["target_schema"]
        table = self.config["target_table"]
        query_path = self.config["sql_file"]

        with open(query_path, 'r') as f:
            query = f.read()

        self.df = run_query(self.conn, query)

        scd_type = self.config.get("scd_type")
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        if scd_type == 1:
            self.df['update_timestamp'] = timestamp
        elif scd_type == 2:
            self.df['effective_from'] = timestamp
            self.df['effective_to'] = pd.Timestamp("9999-12-31")

        self.df["surrogate_key"] = [str(uuid.uuid4()) for _ in range(len(self.df))]

        insert_dataframe(self.conn, self.df, table, db_schema)
        logger.info("Processed pipeline completed successfully.")