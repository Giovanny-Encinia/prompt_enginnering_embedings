import pandas as pd


class TextSplitter:
    def __init__(
        self, max_tokens: int = 2048, tokens_per_document: int = 25, overlap: int = 3
    ):
        self.max_tokens = max_tokens
        self.tokens_per_document = tokens_per_document
        self.overlap = overlap
        self.list_indexes = []

        if self.overlap >= self.tokens_per_document:
            raise ValueError("The tokens_per_document has to be > overlap")

        self.generate_indexes()

    def generate_indexes(self):
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

    def split_text(self, text: str) -> list:
        list_text = text.split(" ")
        list_documents = []

        for i, j in self.list_indexes:
            new_item = " ".join(list_text[i : j + 1])

            if new_item != "":
                list_documents.append(new_item)

            if j > len(list_text):
                break

        return list_documents

    def generate_documents(self, df: pd.DataFrame):
        print(self.list_indexes)
        df["content"] = df["content"].apply(lambda x: self.split_text(x))
        return df
