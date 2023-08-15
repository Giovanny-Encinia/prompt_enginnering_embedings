import pandas as pd
from openai.embeddings_utils import get_embedding, cosine_similarity
from openai_api_connection.api_conection import embedding_connection
import os
import logging
import numpy as np
import sys

# Set the logging configuration
logging.basicConfig(level=logging.INFO)
debug_mode = False

if "--debug" in sys.argv:
    debug_mode = True
    logging.getLogger().setLevel(logging.DEBUG)
else:
    logging.getLogger().setLevel(logging.INFO)


class VectorStore:
    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.count = 0

    def restruct_split(self):
        columns = {col: [] for col in self.df.columns}

        for i, content in self.df.iterrows():
            for text in content.content:
                columns["content"].append(text)
                columns["name_path"].append(content.name_path)
                columns["index_document"].append(content.index_document)
                columns["page"].append(content.page)

        self.df = pd.DataFrame(columns)

    def get_embedding_(self, x: str):
        self.count += 1
        logging.info(f"{self.count}/{self.df.shape[0]}") if self.count % 50 == 0 else 0
        x = get_embedding(x, engine="testCX_2")
        return x

    def create_vector_store(self):
        embedding_connection()
        self.restruct_split()
        logging.info("Generating embeddings...")
        self.df["ada_v2"] = self.df["content"].apply(lambda x: self.get_embedding_(x))

        if debug_mode:
            for i, content in self.df.iterrows():
                logging.debug(i)
                logging.debug(content.content)

        logging.info("Embeddings Generated!!!")
        logging.info("Vectorstore generated in memory!")
        return self.df

    def save_vector_store(self, file_name):
        root_path = os.path.abspath(os.path.join(os.getcwd(), "."))
        # Path to the "data" folder in the root path
        data_folder_path = os.path.join(root_path, "data")
        # CSV file name
        csv_filename = file_name
        # Full path to the CSV file
        csv_path = os.path.join(data_folder_path, csv_filename)
        self.df.to_parquet(csv_path)
        logging.info(f"Vectorstore saved in {csv_path}")
