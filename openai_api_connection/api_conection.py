import os
import openai
import requests
import logging


def embedding_connection():
    api_key = os.getenv("OPENAI_API_KEY")
    openai.api_type = "azure"
    openai.api_base = os.getenv("OPENAI_API_BASE")
    openai.api_key = api_key
    openai.api_version = "2022-12-01"
    url = openai.api_base + "/openai/deployments?api-version=2022-12-01"

    r = requests.get(url, headers={"api-key": api_key})


def connect_api(
    api_type: str = "azure", api_version: str = "2023-03-15-preview"
) -> None:
    """
    Connects to the OpenAI API using the provided API type and version.

    Parameters
    ----------
        api_type (str): The type of API to connect to. Defaults to "azure".
        api_version (str): The version of the API to use. Defaults to "2023-03-15-preview".

    Returns
    -------
        None
    """
    openai.api_base = os.getenv("OPENAI_API_BASE")
    openai.api_key = os.getenv("OPENAI_API_KEY")
    logging.info("Set credentials correctly")
    openai.api_type = api_type
    openai.api_version = api_version


def get_completion_from_messages(
    messages: list,
    model: str = "gpt-3.5-turbo",
    engine: str = "cx_gpt4",
    temperature: float = 0,
) -> str:
    """
    Chat models take a list of messages as input and
    return a model-generated message as output.
    Although the chat format is designed to make multi-turn
    conversations easy, it’s just as useful for single-turn
    tasks without any conversation.

    Parameters
    ----------
    messages : list
        messages for which the model will generate a response.

    model : str, optional
        The name of the chat model to use for generating the response.
        Default is "gpt-3.5-turbo".
    engine: str, optional
        The name of the deployment model in azure
        Default is "cx_gpt4"
    temperature: float, optional
        The randomness of the output response, the value is between 0 and 1
        Default is 0, for more predictable response

    Returns
    -------
    str
        The model-generated message in response to the user prompt.
    """
    response = openai.ChatCompletion.create(
        engine=engine, model=model, messages=messages, temperature=temperature
    )

    return response.choices[0].message.get("content")
