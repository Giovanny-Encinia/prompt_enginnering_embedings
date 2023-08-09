# TODO
from langchain.chat_models import AzureChatOpenAI
from langchain.prompts import ChatPromptTemplate
import os

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_API_BASE = os.getenv("OPENAI_API_BASE")
DEPLOYMENT_NAME = "cx_gpt4"
version = "2023-03-15-preview"

chat = AzureChatOpenAI(
    openai_api_key=OPENAI_API_KEY,
    openai_api_base=OPENAI_API_BASE,
    openai_api_version=version,
    deployment_name=DEPLOYMENT_NAME,
)
template_string = """Translate the text between <<>> in {style}: <<{text}>>"""
prompt_template = ChatPromptTemplate.from_template(template_string)
style = "spanish informal"
text = (
    "I am very tired, but I have to finish the work because many people depends on me"
)
customer_messages = prompt_template.format_messages(style=style, text=text)
customer_response = chat(customer_messages)
print(customer_response)
