# search through the reviews for a specific product
import pandas as pd
from openai.embeddings_utils import get_embedding, cosine_similarity
import os
import logging


def search_docs(df, user_query, top_n=3) -> pd.DataFrame:
    embedding = get_embedding(user_query, engine="testCX_2")
    logging.info("message embedding created")
    df["similarities"] = df.ada_v2.apply(lambda x: cosine_similarity(x, embedding))
    logging.info("cosine similarity applied")
    res = df.sort_values("similarities", ascending=False).head(top_n)

    return res


def search_vectorstore(filename: str) -> str:
    root_path = os.path.abspath(os.path.join(os.getcwd(), "."))
    # Path to the "data" folder in the root path
    data_folder_path = os.path.join(root_path, "data")
    filepath = os.path.join(data_folder_path, filename)
    return filepath


def retrieve_data(df: pd.DataFrame, threshold: float = 0.82) -> tuple[str, str]:
    df = df[df.similarities >= threshold]

    if df.empty:
        logging.info("there are no information in the vectorstore")
        return "null", "null"

    context = ""
    pages = {}
    file_name = []
    indexes = []

    for i, row in df.iterrows():
        file_name.append(row.name_path)
        pages[row.name_path] = []
        pages[row.name_path].append(row.page)
        indexes.append(i)

    context = ""
    for key in pages.keys():

        for val in pages[key]:
            df_path = df.query("name_path == @key")
            content_values = df_path.content[df_path.page == val].values
            content_values = " ".join(list(content_values))
            context += content_values
    pages = get_references_str(pages)
    logging.info("documentation and references created")

    return context, pages


def get_references_str(dict_references: dict) -> str:
    references = ""
    ref = []

    # concat the path and the page
    for key, value in dict_references.items():
        for val in value:
            ref.append(key + "§" + str(value))

    # get the unique values
    ref = set(ref)
    dict_references = {}

    # again transform in dictionary
    for item in ref:
        item_list = item.split("§")
        dict_references[item_list[0]] = item_list[1]

    # transform in str
    for key in dict_references:
        references += f"`File name: {key} | Pages: {str(dict_references[key])}`\n"

    return references
