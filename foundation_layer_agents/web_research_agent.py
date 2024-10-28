#input: json
input = {
    "task": "EVs in Sweden",
    "result_structure": {
        
        "description": "string",
        "revenue_data": "string",
        "market_size": "string",
        "competition": {
            "companies": ["string"]
        },
        "technologies": ["string"],
        "regulations": ["string"],
        "trends": ["string"],
        "environmental_impact": ["string"],
       
    },
    "final_result_satisfaction_level": {
        "title": "boolean",
        "description": "boolean",
        "revenue_data": "boolean",
        "market_size": "boolean",
        "competition": "boolean",
        "technologies": "boolean",
        "regulations": "boolean",
        "trends": "boolean",
        "environmental_impact": "boolean",
        "chart_specific_data": "boolean"
    }
}


from dotenv import load_dotenv
load_dotenv()

import autogen


import asyncio
import sys
import os

# Add the parent directory to sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from typing import Dict

from utils import get_openai_api_key
from openai import AsyncOpenAI
from tools.serp_api import search_serpapi

# API Keys
OPENAI_API_KEY = get_openai_api_key()



print("API Keys:")
print(f"OPENAI_API_KEY: {OPENAI_API_KEY}")


# LLM config change it
llm_config = {
    "model": "gpt-4o",
    "config_list": [{"model": "gpt-4o", "api_key": OPENAI_API_KEY}],
}

client = AsyncOpenAI(api_key=OPENAI_API_KEY)



# Agents

#lets create agents depnding upon the input result structure

# Transport industruy outlook in Sweden
# for each of the keys in the result structure, create an agent that will research the task + key and return the result
initializer = autogen.UserProxyAgent(
    name='init',
    code_execution_config={
        "use_docker": False,
    },
    llm_config=None,
    human_input_mode="NEVER",
    is_termination_msg=lambda x: x.get("content", "").rstrip().endswith("Terminate."),
)
query_generator = autogen.AssistantAgent(
    name='Query_Generator',
    llm_config=llm_config,
    system_message="""
    You are a helpful assistant that generates queries based on the user's request for searching the web.
    """
)

data_collector = autogen.AssistantAgent(
    name='Web_Data_Collector',
    llm_config=llm_config,
    system_message=f"""
   You are a helpful assistant that collects data from the using search_serpapi function.
   Carefully read through the entire text.
   if there is revenue data, make sure to extract the year and the value and unit, it should be a time series data.
   Identify distinct categories of information that can be organized into separate required result structure.
   result: {input["result_structure"]}
"""
)
data_analyst = autogen.AssistantAgent(
    name='Data_Analyst',
    llm_config=llm_config,
    system_message=f"""You are a helpful assistant that analyzes the data collected by the data_collector.
    and create an output based on the required result structure below:
    result : {input["result_structure"]} \n and also include the final_result_satisfaction_level, telling if the result is satisfactory or not in the format below:\n
    satisfaction_level: {input["final_result_satisfaction_level"]}
    once you have provided the report, say Terminate.""",
    is_termination_msg=lambda x: x.get("content", "").rstrip().endswith("Terminate.")
)


def state_transition(last_speaker, group_chat):
    messages = group_chat.messages
    if last_speaker is initializer:
        #init -> retrieve
    #     return query_generator
    # elif last_speaker is query_generator:
        return data_collector
    elif last_speaker is data_collector:
        #retrieve -> action1 -> action2
        #if message contains ```markdown```:
        if "```markdown```" in messages[-1].get("content", ""):
            return data_analyst
        else:
            return data_collector
    
    elif last_speaker is data_analyst:
        return None
    

group_chat = autogen.GroupChat(
    agents=[initializer,data_collector, data_analyst],
    messages=[],
    max_round=5,
    speaker_selection_method="auto",
)
manager = autogen.GroupChatManager(
    groupchat=group_chat,
    llm_config=llm_config,
    
)
#register function for data_collector
@data_collector.register_for_execution()
@data_collector.register_for_llm(description="Fetch industry revenue data for the past 2-3 years.")
# @query_generator.register_for_llm(description="Generate a query based on the user's request.")
async def research_industry_growth_prospects(query: str,) -> str:
    data = await search_serpapi(query, num_results=5, geography="se")
    summary = await client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": "You are a helpful assistant. Summarize the content in a concise manner, focusing on revenue data and growth trends."},
            {"role": "user", "content": f"""
            You are tasked with converting the following raw text data into structured tables for analysis. Do not summarize or interpret the data; your role is solely to organize the information.
             
             *** Instructions ***
             - Carefully read through the entire text.
             - Identify distinct categories of information that can be organized into separate required result structure {input["result_structure"]}.
             - Create tables using Markdown syntax.
             - Ensure all relevant data from the text is included in the tables.
             - Use appropriate column headers for each table.
             - Maintain the original data values and units as presented in the text.
            {data}
            """}
        ],
    )
    return summary.choices[0].message.content
    
async def main():
    result = await initializer.a_initiate_chat(manager, message="Ev market size and growth in Sweden ") 
       
    

if __name__ == "__main__":
    asyncio.run(main())



    



# if __name__ == "__main__":
#     asyncio.run(test_search_serpapi())








