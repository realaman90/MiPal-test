import autogen
from utils import get_openai_api_key, get_serpapi_key, get_fc_api_key, print_all_env_variables

OPENAI_API_KEY = get_openai_api_key()

llm_config = {
    "model": "gpt-4o",
    "config_list": [{"model": "gpt-4o", "api_key": OPENAI_API_KEY}],
}
# create an AssistantAgent instance named "assistant"
assistant = autogen.AssistantAgent(
    name="assistant",
    llm_config=llm_config,
)
# create a UserProxyAgent instance named "user_proxy"
user_proxy = autogen.UserProxyAgent(
    name="user_proxy",
    human_input_mode="TERMINATE",
    max_consecutive_auto_reply=10,
    is_termination_msg=lambda x: x.get("content", "").rstrip().endswith("TERMINATE"),
    code_execution_config={
        "work_dir": "web",
        "use_docker": False,
    },  # Please set use_docker=True if docker is available to run the generated code. Using docker is safer than running the generated code directly.
    llm_config=llm_config,
    system_message="""Reply TERMINATE if the task has been solved at full satisfaction.
Otherwise, reply CONTINUE, or the reason why the task is not solved yet.""",
)

user_proxy.initiate_chat(
    assistant,
    message="""
Show me the YTD gain of 10 largest technology companies as of today.
""",
)






