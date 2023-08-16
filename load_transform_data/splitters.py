import pandas as pd
import logging


class TextSplitter:
    """
    A class for splitting text content into smaller documents.

    Args:
        max_tokens (int, optional): The maximum number of tokens in the entire text.
            Defaults to 2048.
        tokens_per_document (int, optional): The desired number of tokens per document.
            Defaults to 500.
        overlap (int, optional): The number of tokens overlapping between consecutive documents.
            Defaults to 50.
    """

    def __init__(
        self, max_tokens: int = 4096, tokens_per_document: int = 600, overlap: int = 150
    ):
        self.max_tokens = max_tokens
        self.tokens_per_document = tokens_per_document
        self.overlap = overlap
        self.list_indexes = []

        if self.overlap >= self.tokens_per_document:
            raise ValueError("The tokens_per_document has to be > overlap")

        self.__generate_indexes()

    def __generate_indexes(self):
        """
        Generates the list of start and end indexes for each document based on
        `tokens_per_document` and `overlap` parameters.
        """

        list_indexes = []
        i = 0

        while i < self.max_tokens:
            next_i = i + self.tokens_per_document - 1

            if i == 0:
                list_indexes.append((0, self.tokens_per_document - 1))
            else:
                list_indexes.append((i, next_i))

            i = next_i - self.overlap + 1

        self.list_indexes = list_indexes[:]
        logging.info("General structure for indexes split generated")

    def __split_text(self, text: str) -> list:
        """
        Splits the given text into a list of smaller documents based on the
        pre-generated list of indexes.

        Args:
            text (str): The input text to be split.

        Returns:
            list: List of split documents.
        """
        list_text = text.split(" ")
        list_documents = []

        for i, j in self.list_indexes:
            new_item = " ".join(list_text[i : j + 1])

            if new_item != "":
                list_documents.append(new_item)

            if j > len(list_text):
                break

        return list_documents

    def generate_documents(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Generates smaller documents from the 'content' column of the input DataFrame.

        Args:
            df (pd.DataFrame): Input DataFrame with a 'content' column containing text.

        Returns:
            pd.DataFrame: DataFrame with added 'content' column containing split documents.
        """
        print(self.list_indexes)
        df["content"] = df["content"].apply(lambda x: self.__split_text(x))
        logging.info("Documents split generated")
        return df
