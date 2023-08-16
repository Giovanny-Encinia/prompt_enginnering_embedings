import pandas as pd
from azure.storage.blob import (
    BlobServiceClient,
    ContainerClient,
    StorageStreamDownloader,
)
import os
import io
import pypdf
import re
import logging


class AzureBlobStorageDocumentLoader:
    """
    Class that load files from blob storage, then you can transform this files
    in a pd Dataframe.

    Parameters:
        account_key (str): this is the key, usually is charge in the environment
            Defaults to 'westdraid001'
        account_name (str, optional): the name of the azure account
            Defaults to 'tests-gpt-neoris'
        container_name (str, optional): the name of the azure blob container
        prefix (str, optional): the beginning of the path where you want to search and load the files
            Defaults to 'EMBEDDINGS_TEST/ai_papers_segmented'
    """

    def __init__(
        self,
        account_key: str,
        account_name: str = "westdraid001",
        container_name: str = "tests-gpt-neoris",
        prefix: str = "EMBEDDINGS_TEST/langchain",
    ):
        self.account_name = account_name
        self.account_key = account_key
        self.container_name = container_name
        self.prefix = prefix

    def __connect_blob_storage(self) -> ContainerClient:
        """
        Connects to an Azure Blob Storage container using the provided account credentials.

        This function creates a connection to an Azure Blob Storage container using the
        specified account name, account key, and container name. It returns a ContainerClient
        object that can be used to interact with the container.

        Parameters
        ------------
            self

        Returns
        -------
            ContainerClient:
                A client object for interacting with the specified container.
        """
        string_connection = self.__string_connection_blob_storage()
        service_client = BlobServiceClient.from_connection_string(string_connection)
        container_client = service_client.get_container_client(self.container_name)
        logging.info("Connection to Container succesful")

        return container_client

    def __string_connection_blob_storage(self) -> str:
        """
        Generate a connection string for Azure Blob Storage.

        This private method constructs a connection string for Azure Blob Storage using the provided account name and account key.

        Returns:
            str: A connection string for Azure Blob Storage.

        Example:
            instance = BlobStorageConnection(account_name='myaccount', account_key='myaccountkey')
            connection_string = instance._BlobStorageConnection__string_connection_blob_storage()
            print(connection_string)
            # Output: 'DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=myaccountkey;EndpointSuffix=core.windows.net;'
        """
        string_connection = "DefaultEndpointsProtocol=https;"
        string_connection += f"AccountName={self.account_name};"
        string_connection += f"AccountKey={self.account_key};"
        string_connection += "EndpointSuffix=core.windows.net;"
        return string_connection

    @staticmethod
    def __retrieve_file_from_blob_storage(
        container_client: ContainerClient, blob_name: str
    ) -> StorageStreamDownloader:
        """
        Retrieve the content of a file from Azure Blob Storage.

        This function retrieves the content of a file stored in Azure Blob Storage
        using the provided `container_client` and `blob_name`.

        Parameters
        ----------
        None

        Returns
        -------
        azure.storage.blob.BlobDownloadStream
            A BlobDownloadStream object representing the downloaded file content.
        """
        blob_client = container_client.get_blob_client(blob_name)
        blob_content = blob_client.download_blob()
        return blob_content

    def __create_list_path_blobs(
        self, blob_container: ContainerClient, ext=None
    ) -> list:
        """
        Create a list of file paths for blobs in Azure Blob Storage.

        This function generates a list of file paths for blobs located in the specified
        `blob_container` and starting with the given `init_path`. You can optionally
        provide a list of file extensions to filter the results.

        Parameters
        ----------
        self
        blob_container : azure.storage.blob.ContainerClient
            A container client instance pointing to the Azure Blob Storage container.
        ext : list, optional
            A list of file extensions to filter the results. If not provided, the default
            extension list [".pdf"] will be used.

        Returns
        -------
        list
            A list of file paths for blobs matching the given criteria.
        """

        if ext is None:
            ext = [".pdf"]

        blob_path_files = blob_container.list_blobs(name_starts_with=self.prefix)
        path_files = [
            blob.name
            for blob in blob_path_files
            if os.path.splitext(blob.name)[-1] in ext
        ]
        return path_files

    def __generate_pandas_dataframe_from_blob(
        self, list_path_blobs: list, blob_container_: ContainerClient
    ) -> pd.DataFrame:
        """
        Generate a pandas DataFrame from PDFs stored as blobs in Azure Blob Storage.

        This function reads PDFs stored as blobs in the specified `blob_container_` and
        extracts information such as the file path, page number, and content. It then
        creates a pandas DataFrame containing this information.

        Parameters
        ----------
        self
        list_path_blobs : list
            A list of file paths for the PDF blobs to be processed.
        blob_container_ : azure.storage.blob.ContainerClient
            A container client instance pointing to the Azure Blob Storage container.

        Returns
        -------
        pd.DataFrame
            A pandas DataFrame containing information about PDFs, including their file
            paths, page numbers, and extracted content.
        """
        list_pdfs = []
        list_name_path = []
        list_page = []
        list_content = []
        list_index_document = []

        for i, path in enumerate(list_path_blobs):
            pdf_binary = self.__retrieve_file_from_blob_storage(blob_container_, path)
            stream = io.BytesIO()
            pdf_binary.readinto(stream)
            pdf = pypdf.PdfReader(stream, strict=True)
            list_pdfs.append((i, pdf))

            # enumerate each page of each file
            for j, page in enumerate(pdf.pages):
                list_name_path.append(path)
                list_page.append(j)
                list_content.append(page.extract_text())
                list_index_document.append(i)

        columns_name = ["name_path", "index_document", "page", "content"]
        columns_data = [list_name_path, list_index_document, list_page, list_content]
        dict_pdf_info = dict(zip(columns_name, columns_data))
        return pd.DataFrame(dict_pdf_info)

    @staticmethod
    def clean_content(x: str) -> str:
        """
        Clean the input text by removing line breaks, strange symbols, and double spaces.

        This function applies regular expressions to the input text in order to remove line
        breaks, non-alphanumeric characters, and consecutive spaces. It returns the cleaned text.

        Parameters
        ----------
        x : str
            The input text to be cleaned.

        Returns
        -------
        str
            The cleaned text with line breaks, strange symbols, and double spaces removed.
        """
        x = re.sub(r"[\r\n\t]+|[^a-zA-Z0-9\s.,!?_(){}\[\]+=\-/*]+| {2,}", "", x)
        x = re.sub(r"\s+", " ", x).strip()
        x = re.sub(r". ,", "", x)
        # remove all instances of multiple spaces
        x = x.replace("..", ".")
        x = x.replace(". .", ".")
        x = x.replace("\n", "")
        x = x.strip()
        return x

    def load_document(self) -> pd.DataFrame:
        """
        Create a pandas DataFrame with cleaned content from documents stored as blobs.

        This function connects to Azure Blob Storage using the provided credentials and
        retrieves PDF documents stored as blobs. It generates a pandas DataFrame with
        information about the documents, including cleaned content.

        Parameters
        ----------
        self

        Returns
        -------
        pd.DataFrame
            A pandas DataFrame containing information about documents, including their
            file paths, page numbers, and cleaned content."""
        blob_container = self.__connect_blob_storage()
        path_blobs = self.__create_list_path_blobs(blob_container)
        df = self.__generate_pandas_dataframe_from_blob(path_blobs, blob_container)
        df["content"] = df.content.apply(lambda x: self.clean_content(x))
        return df
