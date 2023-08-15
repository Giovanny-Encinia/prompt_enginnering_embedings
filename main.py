# TODO
import logging
import pandas as pd

from load_transform_data.etl_blobstorage import (
    create_clean_pandas_document_dataframe_from_blob,
)
from load_transform_data.splitters import TextSplitter
from load_transform_data.vectorstore import VectorStore


def create_and_save_vectorstore():
    splitter = TextSplitter(tokens_per_document=600, overlap=40)
    df = create_clean_pandas_document_dataframe_from_blob()
    df = splitter.generate_documents(df)
    logging.debug(df.head())
    vectorstore = VectorStore(df)
    df = vectorstore.create_vector_store()
    logging.debug(df.head())
    vectorstore.save_vector_store("vectorstore.parquet")
    loaded_df = pd.read_parquet("./data/vectorstore.parquet")
    print(loaded_df)


create_and_save_vectorstore()
