import pandas as pd
from openai.embeddings_utils import get_embedding
from openai_api_connection.api_conection import embedding_connection
import os
import logging
import sys

debug_mode = False

if "--debug" in sys.argv:
    debug_mode = True
    logging.getLogger().setLevel(logging.DEBUG)
else:
    logging.getLogger().setLevel(logging.INFO)


class VectorStore:
    """
    A class for generating and managing embeddings-based vector store.

    This class is designed to create a vector store from a given pandas DataFrame.
    It provides methods to restructure and split the DataFrame, generate embeddings
    for the contents, and save the resulting vector store as a parquet file.

    Args:
        df (pd.DataFrame): The input DataFrame containing data to create the vector store.
    """

    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.count = 0

    def __restruct_split(self):
        """
        Restructure and split the DataFrame for embedding generation.
        """
        columns = {col: [] for col in self.df.columns}

        # The column 'content' in the df is a list object, the items in this list
        # will be transformed in new rows inheriting the other elements in the adjacent columns
        for i, content in self.df.iterrows():
            for text in content.content:
                # save the info in dictionaries for create a new expanded data frame
                columns["content"].append(text)
                columns["name_path"].append(content.name_path)
                columns["index_document"].append(content.index_document)
                columns["page"].append(content.page)

        self.df = pd.DataFrame(columns)

    def __get_embedding_(self, x: str):
        """
        Generate embedding for a given input text.

        Args:
            x (str): Input text for embedding generation.

        Returns:
            str: The generated embedding.
        """
        self.count += 1
        logging.info(f"{self.count}/{self.df.shape[0]}") if self.count % 50 == 0 else 0
        x = get_embedding(x, engine="testCX_2")
        return x

    def create_vector_store(self):
        """
        Create the vector store by generating embeddings.

        Returns:
            pd.DataFrame: The DataFrame containing generated embeddings.
        """
        embedding_connection()
        self.__restruct_split()
        logging.info("Generating embeddings...")
        self.df["ada_v2"] = self.df["content"].apply(lambda x: self.__get_embedding_(x))

        if debug_mode:
            for i, content in self.df.iterrows():
                logging.debug(i)
                logging.debug(content.content)

        logging.info("Embeddings Generated!!!")
        logging.info("Vectorstore generated in memory!")
        return self.df

    def save_vector_store(self, file_name):
        """
        Save the vector store to a parquet file.

        Args:
            file_name (str): The name of the output file.
        """
        root_path = os.path.abspath(os.path.join(os.getcwd(), "."))
        # Path to the "data" folder in the root path
        data_folder_path = os.path.join(root_path, "data")
        # CSV file name
        csv_filename = file_name
        # Full path to the CSV file
        csv_path = os.path.join(data_folder_path, csv_filename)
        self.df.to_parquet(csv_path)
        logging.info(f"Vectorstore saved in {csv_path}")
