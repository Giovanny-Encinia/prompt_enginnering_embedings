# TODO
import logging
import pandas as pd

from load_transform_data.etl_blobstorage import (
    create_clean_pandas_document_dataframe_from_blob,
)
from load_transform_data.splitters import TextSplitter
from load_transform_data.vectorstore import VectorStore
from openai_api_connection.api_conection import (
    connect_api,
    get_completion_from_messages,
)
from prompts.paper_assistent import CONTEXT
import gradio as gr
import time
import signal
from openai_api_connection.api_conection import get_completion_from_messages
import os


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


def check_user_time_between_questions(delay_time: float):
    if delay_time > USER_DELAY_TIME:
        logging.info("User has abandoned the chat")
        chat.close()
        exit()


def chatbot(message, history):
    global time_response
    time_request = time.time()
    delay_user = time_request - time_response

    if len(history) < 1:
        delay_user = 0

    check_user_time_between_questions(delay_user)
    logging.info(delay_user)
    history_openai = [{"role": "system", "content": context}]

    for user, assistant in history:
        history_openai.append({"role": "user", "content": user})
        history_openai.append({"role": "assistant", "content": assistant})

    history_openai.append({"role": "user", "content": message})
    response = get_completion_from_messages(history_openai)

    for i in range(len(response)):
        time.sleep(0.005)
        time_response = time.time()
        yield response[: i + 1]


def run_chatbot():
    my_theme = gr.Theme.from_hub("HaleyCH/HaleyCH_Theme")
    chat_interface = gr.ChatInterface(
        chatbot,
        chatbot=gr.Chatbot(height=300),
        textbox=gr.Textbox(
            placeholder="Ask me about artificial intelligence", container=False, scale=7
        ),
        title="Artificial Intelligence Professor",
        description="AI",
        theme=my_theme,
        examples=[""],
        cache_examples=True,
        retry_btn=None,
        undo_btn="Delete Previous",
        clear_btn="Clear",
    )
    return chat_interface


def handler(signum, frame):
    logging.info("Time's up! Terminating the process.")
    gr.Info("Session finished")
    exit()


# Set the alarm to 1 minute (60 seconds)
signal.signal(signal.SIGALRM, handler)
SESSION_TOTAL_TIME = 600
USER_DELAY_TIME = 10
signal.alarm(SESSION_TOTAL_TIME)
# create_and_save_vectorstore()
connect_api()
context = CONTEXT.format(information="HOLA QUE HACE")
time_response = time.time()
chat = run_chatbot()
chat.queue().launch()
