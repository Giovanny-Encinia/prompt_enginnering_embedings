import logging
import pandas as pd

from load_transform_data.loader_blob_storage import (
    AzureBlobStorageDocumentLoader,
)
from load_transform_data.splitters import TextSplitter
from load_transform_data.vectorstore import VectorStore
from load_transform_data.similarities_searcher import SimilaritiesContextSearcher
from openai_api_connection.api_conection import (
    connect_api,
    get_completion_from_messages,
)
from prompts.paper_assistent import CONTEXT, CHECK_IF_ONLY_HELLO
import gradio as gr
import time
import signal
from openai_api_connection.api_conection import get_completion_from_messages
from load_transform_data.utils import get_azure_primary_key, get_file_full_path
import sys

# Set the logging configuration
logging.basicConfig(level=logging.INFO)


def create_and_save_vectorstore():
    splitter = TextSplitter(tokens_per_document=600, overlap=40)
    azure_key = get_azure_primary_key()
    loader = AzureBlobStorageDocumentLoader(azure_key)
    df = loader.load_document()
    df = splitter.generate_documents(df)
    logging.debug(df.head())
    vectorstore = VectorStore(df)
    df = vectorstore.create_vector_store()
    logging.debug(df.head())
    vectorstore.save_vector_store("vectorstore.parquet")
    loaded_df = pd.read_parquet("./data/vectorstore.parquet")
    print(loaded_df)


def check_user_time_between_questions(delay_time: float):
    if delay_time > USER_DELAY_TIME:
        logging.info("User has abandoned the chat")
        chat.close()
        exit()


def chatbot(message, history):
    global time_response
    time_request = time.time()
    delay_user = time_request - time_response

    # this is for avoid hallucination in references
    if history:
        split_last_assistant_message = history[-1][1].split("\n\n`References:`")
        history[-1][1] = "".join(split_last_assistant_message[:-1])

    if len(history) < 1:
        delay_user = 0

    check_user_time_between_questions(delay_user)
    logging.info(delay_user)
    similarity_searcher.get_dataframe_top_similarities(
        df, message, top_n=5
    )
    text, pages = similarity_searcher.get_context_and_references()
    # delete the previous request
    history_openai = [{"role": "system", "content": CONTEXT.format(information=text)}]
    logging.info("system context created correctly")

    for user, assistant in history:
        history_openai.append({"role": "user", "content": user})
        history_openai.append({"role": "assistant", "content": assistant})

    history_openai.append({"role": "user", "content": message})
    logging.info("History loaded")
    response = get_completion_from_messages(history_openai)
    logging.info("response generated")

    if pages != "null":
        response += f"\n\n`References:`\n{pages}"

    logging.info("writing in the chat...")

    for i in range(len(response)):
        time.sleep(0.005)
        time_response = time.time()
        yield response[: i + 1]


def run_chatbot():
    my_theme = gr.Theme.from_hub("JohnSmith9982/small_and_pretty")
    chat_interface = gr.ChatInterface(
        chatbot,
        chatbot=gr.Chatbot(height=500),
        textbox=gr.Textbox(
            placeholder="Ask me about Lang Chain", container=False, scale=9
        ),
        title="Lang Chain Professor",
        description="AI",
        theme=my_theme,
        examples=[""],
        submit_btn="Submit",
        stop_btn="Stop",
        retry_btn=None,
        undo_btn=None,
        clear_btn=None,
    )
    return chat_interface


def handler(signum, frame):
    logging.info("Time's up! Terminating the process.")
    gr.Info("Session finished")
    exit()


# create_and_save_vectorstore()
# Set the alarm to 1 minute (60 seconds)
signal.signal(signal.SIGALRM, handler)
filename = "vectorstore.parquet"
filepath = get_file_full_path(filename)
df = pd.read_parquet(filepath)
SESSION_TOTAL_TIME = 3600
USER_DELAY_TIME = 600
signal.alarm(SESSION_TOTAL_TIME)
connect_api()
similarity_searcher = SimilaritiesContextSearcher()
time_response = time.time()
chat = run_chatbot()
chat.queue().launch(share=True)
