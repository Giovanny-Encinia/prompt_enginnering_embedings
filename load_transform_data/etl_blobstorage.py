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


def connect_blob_storage(
    account_name: str, account_key: str, container_name: str
) -> ContainerClient:
    """
    Connects to an Azure Blob Storage container using the provided account credentials.

    This function creates a connection to an Azure Blob Storage container using the
    specified account name, account key, and container name. It returns a ContainerClient
    object that can be used to interact with the container.

    Parameters
    ------------
        ccount_name (str):
            The name of the Azure Blob Storage account.
        account_key (str):
            The account key or shared access signature (SAS) token for authentication.
        container_name (str):
            The name of the container within the storage account.

    Returns
    -------
        ContainerClient:
            A client object for interacting with the specified container.
    """

    string_connection = "DefaultEndpointsProtocol=https;"
    string_connection += f"AccountName={account_name};"
    string_connection += f"AccountKey={account_key};"
    string_connection += "EndpointSuffix=core.windows.net;"
    service_client = BlobServiceClient.from_connection_string(string_connection)
    container_client = service_client.get_container_client(container_name)

    return container_client


def get_azure_primary_key(name_env_variable: str = "AZURE_KEY") -> str:
    """
    Retrieve the Azure primary key from an environment variable.

    This function retrieves the Azure primary key from the specified environment
    variable. If the environment variable is not set or has no value, an exception
    is raised with a corresponding error message.

    Parameters
    ----------
    name_env_variable : str, optional
        The name of the environment variable containing the Azure primary key.
        The default is "AZURE_KEY".

    Returns
    -------
    str
        The retrieved Azure primary key.

    Raises
    ------
    Exception
        If the specified environment variable is not set or has no value, an
        exception is raised with an error message suggesting to reload the
        environment or create the variable.
    """
    azure_key = os.getenv(name_env_variable, False)

    if not azure_key:
        raise Exception(
            "There is no variable, reload the environment or create the variable"
        )

    return azure_key


def retrieve_file_from_blob_storage(
    container_client: ContainerClient, blob_name: str
) -> StorageStreamDownloader:
    """
    Retrieve the content of a file from Azure Blob Storage.

    This function retrieves the content of a file stored in Azure Blob Storage
    using the provided `container_client` and `blob_name`.

    Parameters
    ----------
    container_client : azure.storage.blob.ContainerClient
        A container client instance pointing to the Azure Blob Storage container.
    blob_name : str
        The name of the blob (file) to retrieve from the container.

    Returns
    -------
    azure.storage.blob.BlobDownloadStream
        A BlobDownloadStream object representing the downloaded file content.
    """
    blob_client = container_client.get_blob_client(blob_name)
    blob_content = blob_client.download_blob()
    return blob_content


def create_list_path_blobs(
    init_path: str, blob_container: ContainerClient, ext=None
) -> list:
    """
    Create a list of file paths for blobs in Azure Blob Storage.

    This function generates a list of file paths for blobs located in the specified
    `blob_container` and starting with the given `init_path`. You can optionally
    provide a list of file extensions to filter the results.

    Parameters
    ----------
    init_path : str
        The initial path or prefix for blobs to be listed.
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

    blob_path_files = blob_container.list_blobs(name_starts_with=init_path)
    path_files = [
        blob.name for blob in blob_path_files if os.path.splitext(blob.name)[-1] in ext
    ]
    return path_files


def generate_pandas_dataframe_from_blob(
    list_path_blobs: list, blob_container_: ContainerClient
) -> pd.DataFrame:
    """
    Generate a pandas DataFrame from PDFs stored as blobs in Azure Blob Storage.

    This function reads PDFs stored as blobs in the specified `blob_container_` and
    extracts information such as the file path, page number, and content. It then
    creates a pandas DataFrame containing this information.

    Parameters
    ----------
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

    for i, path in enumerate(list_path_blobs):
        pdf_binary = retrieve_file_from_blob_storage(blob_container_, path)
        stream = io.BytesIO()
        pdf_binary.readinto(stream)
        pdf = pypdf.PdfReader(stream, strict=True)
        list_pdfs.append((i, pdf))

        for j, page in enumerate(pdf.pages):
            list_name_path.append(path)
            list_page.append(j)
            list_content.append(page.extract_text())

    columns_name = ["name_path", "page", "content"]
    columns_data = [list_name_path, list_page, list_content]
    dict_pdf_info = dict(zip(columns_name, columns_data))
    return pd.DataFrame(dict_pdf_info)


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
    cleaned_text = re.sub(r"[\r\n\t]+|[^a-zA-Z0-9\s.,!?]+| {2,}", " ", x)
    return cleaned_text


def create_clean_pandas_document_dataframe_from_blob(
    account_name: str = "westdraid001",
    container_name: str = "tests-gpt-neoris",
    begin_path: str = "EMBEDDINGS_TEST/ai_papers_segmented",
) -> pd.DataFrame:
    """
    Create a pandas DataFrame with cleaned content from documents stored as blobs.

    This function connects to Azure Blob Storage using the provided credentials and
    retrieves PDF documents stored as blobs. It generates a pandas DataFrame with
    information about the documents, including cleaned content.

    Parameters
    ----------
    account_name : str, optional
        Azure Storage account name. Default is "westdraid001".
    container_name : str, optional
        Azure Blob Storage container name. Default is "tests-gpt-neoris".
    begin_path : str, optional
        The initial path or prefix for the PDF blobs to be processed.
        Default is "EMBEDDINGS_TEST/ai_papers_segmented".

    Returns
    -------
    pd.DataFrame
        A pandas DataFrame containing information about documents, including their
        file paths, page numbers, and cleaned content."""
    azure_key = get_azure_primary_key()
    blob_container = connect_blob_storage(
        account_name=account_name, account_key=azure_key, container_name=container_name
    )
    path_blobs = create_list_path_blobs(begin_path, blob_container)
    df = generate_pandas_dataframe_from_blob(path_blobs, blob_container)
    df["content"] = df.content.apply(lambda x: clean_content(x))
    return df


if __name__ == "__main__":
    print(create_clean_pandas_document_dataframe_from_blob().head())
