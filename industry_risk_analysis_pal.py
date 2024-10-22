"""
Pulls data from industry news, regulatory updates, competitor press releases, and technology trend reports.
Utilizes web scraping, API integrations, and subscription-based data feeds.
Data Processing Agents:
Filters and normalizes data, transforming it into a consistent format.
Uses natural language processing (NLP) to extract key information and trends.
Data Analysis Agents:
Applies machine learning models to identify patterns, predict potential risks, and assess the impact on the industry.
Output:
Generates a report highlighting industry risks, competitor movements, and technological disruptions.
"""

# import requests

# # replace the "demo" apikey below with your own key from https://www.alphavantage.co/support/#api-key
# api_key = 'VHQE417KRI9RXHRZ'
# symbol = 'CNY'  # SLX is the VanEck Vectors Steel ETF
# url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}'
# r = requests.get(url)
# data = r.json()

# print(data)

import autogen
from utils import get_openai_api_key

OPENAI_API_KEY = get_openai_api_key()

llm_config = {
    "model": "gpt-4o",
    "config_list": [{"model": "gpt-4o", "api_key": OPENAI_API_KEY}],
}

assistant = autogen.AssistantAgent(
    name="assistant",
    llm_config=llm_config,
)

user_proxy = autogen.UserProxyAgent(
    name="user_proxy",
    code_execution_config={"work_dir": "./work", "use_docker": False},
    is_termination_msg=lambda x: x.get("content", "").rstrip().endswith("TERMINATE"),
    max_consecutive_auto_reply=10,
    human_input_mode="NEVER",
)

financial_tasks = [
    """What are the current prices of SLX and MT, and how is the performance of SLX over the past 6 months in terms of percentage change""",
    """Investigate the recent regulatory updates and their potential impact on the steel industry""",
]

presentation_tasks = [
    """Develop a presentation on the recent regulatory updates and their potential impact on the steel industry""",
]
code_tasks = [
    """Create a frontend for the presentation on the recent regulatory updates and their potential impact on the steel industry""",
]
financial_agent = autogen.AssistantAgent(
    name="financial_agent",
    llm_config=llm_config,
)
research_agent = autogen.AssistantAgent(
    name="research_agent",
    llm_config=llm_config,
)
presentation_agent = autogen.AssistantAgent(
    name="presentation_agent",
    llm_config=llm_config,
    system_message="""
        You are a professional presenter, you are known for your ability to create presentations. 
        You are known for your ability to understand the data and research and create corporate presentations.
        Reply "TERMINATE" in the end when everything is done.
        """,
)

developer_agent = autogen.AssistantAgent(
    name="developer_agent",
    llm_config=llm_config,
    system_message="""
        You are a professional developer, you are known for your ability to write code. 
        in NextJS 14, TailwindCSS, with shadcn/ui and next-themes.
        you can understand corporate presentations and create frontend code for them.
        Reply "TERMINATE" in the end when everything is done.
        """,
)

user_proxy_auto = autogen.UserProxyAgent(
    name="User_Proxy_Auto",
    human_input_mode="NEVER",
    is_termination_msg=lambda x: x.get("content", "") and x.get("content", "").rstrip().endswith("TERMINATE"),
    code_execution_config={
        "last_n_messages": 1,
        "work_dir": "tasks",
        "use_docker": False,
    },  # Please set use_docker=True if docker is available to run the generated code. Using docker is safer than running the generated code directly.
)


user_proxy = autogen.UserProxyAgent(
    name="User_Proxy",
    human_input_mode="ALWAYS",  # ask human for input at each step
    is_termination_msg=lambda x: x.get("content", "") and x.get("content", "").rstrip().endswith("TERMINATE"),
    code_execution_config={
        "last_n_messages": 1,
        "work_dir": "tasks",
        "use_docker": False,
    },  # Please set use_docker=True if docker is available to run the generated code. Using docker is safer than running the generated code directly.
)
chat_results = autogen.initiate_chats(
    [
        {
            "sender": user_proxy_auto,
            "recipient": financial_agent,
            "message": financial_tasks[0],
            "clear_history": True,
            "silent": False,
            "summary_method": "last_msg",
        },
        {
            "sender": user_proxy_auto,
            "recipient": research_agent,
            "message": financial_tasks[1],
            "max_turns": 2,  # max number of turns for the conversation (added for demo purposes, generally not necessarily needed)
            "summary_method": "reflection_with_llm",
        },
        {
            "sender": user_proxy,
            "recipient": presentation_agent,
            "message": presentation_tasks[0],
            "carryover": "I want to include a figure or a table, and  graphs of data.",  # additional carryover to include to the conversation (added for demo purposes, generally not necessarily needed)
        },
        {
            "sender": user_proxy,
            "recipient": developer_agent,
            "message": code_tasks[0],
            "message": "create a frontend for the presentation",
        },
    ]
)