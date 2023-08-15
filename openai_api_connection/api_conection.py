import os
import openai
import requests


def embedding_connection():
    api_key = os.getenv("OPENAI_API_KEY")
    openai.api_type = "azure"
    openai.api_base = os.getenv("OPENAI_API_BASE")
    openai.api_key = api_key
    openai.api_version = "2022-12-01"
    url = openai.api_base + "/openai/deployments?api-version=2022-12-01"

    r = requests.get(url, headers={"api-key": api_key})
