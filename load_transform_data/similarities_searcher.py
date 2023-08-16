# search through the reviews for a specific product
import pandas as pd
from openai.embeddings_utils import get_embedding, cosine_similarity
import logging


class SimilaritiesContextSearcher:
    """
    A class for searching similar contexts and references within a DataFrame.
    Usage:
    searcher = SimilaritiesContextSearcher()
    top_similarities = searcher.get_dataframe_top_similarities(df, user_query)
    context, references = searcher.get_context_and_references(threshold)
    """

    def __init__(self):
        self.df = pd.DataFrame({})
        self.pages = {}

    def get_dataframe_top_similarities(
        self, df: pd.DataFrame, user_query: str, top_n: int = 3
    ) -> pd.DataFrame:
        """
        Get the top similar rows from the DataFrame based on a user query.

        Args:
            df (pd.DataFrame): The DataFrame containing data to search.
            user_query (str): The user's query for similarity comparison.
            top_n (int, optional): The number of top similar rows to retrieve. Default is 3.

        Returns:
            pd.DataFrame: A DataFrame containing the top similar rows.
        """
        self.df = df
        embedding = get_embedding(user_query, engine="testCX_2")
        logging.info("message embedding created")
        self.df["similarities"] = self.df.ada_v2.apply(
            lambda x: cosine_similarity(x, embedding)
        )
        logging.info("cosine similarity applied")
        similarities = self.df.sort_values("similarities", ascending=False).head(top_n)

        return similarities

    def get_context_and_references(self, threshold: float = 0.8) -> tuple[str, str]:
        """
        Get context and references for rows with similarities above the threshold.

        Args:
            threshold (float, optional): The similarity threshold. Default is 0.8.

        Returns:
            tuple[str, str]: A tuple containing context and references information.
        """
        if self.df.empty:
            raise NameError(
                "First use get_dataframe_top_similarities and use a DataFrame"
            )

        logging.info("maximum")
        logging.info(self.df.similarities.max())
        df_similarities = self.df[self.df.similarities >= threshold]

        if df_similarities.empty:
            logging.info("there are no information in the vectorstore")
            return "null", "null"

        file_name = []
        indexes = []

        for i, row in df_similarities.iterrows():
            file_name.append(row.name_path)
            self.pages[row.name_path] = []
            self.pages[row.name_path].append(row.page)
            indexes.append(i)

        context = ""
        for key in self.pages.keys():

            for val in self.pages[key]:
                df_path = df_similarities.query("name_path == @key")
                content_values = df_path.content[df_path.page == val].values
                content_values = " ".join(list(content_values))
                context += content_values

        pages = self.__transform_dict_to_str_references()
        logging.info("documentation and references created")
        logging.info(pages)

        return context, pages

    def __transform_dict_to_str_references(self) -> str:
        """
        Transform the pages dictionary into formatted string references.
        """
        references = ""
        ref = []

        # concat the path and the page
        for key, value in self.pages.items():
            for val in value:
                ref.append(key + "§" + str(value))

        # get the unique values
        ref = set(ref)
        dict_references = {}

        # again transform in dictionary
        for item in ref:
            # use the symbol § because is rarely used in texts
            item_list = item.split("§")
            dict_references[item_list[0]] = item_list[1]

        # transform in str
        for key in dict_references:
            references += f"`File name: {key} | Pages: {str(dict_references[key])}`\n"

        return references
