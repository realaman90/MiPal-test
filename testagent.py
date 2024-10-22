from dotenv import load_dotenv
load_dotenv()

import autogen
from firecrawl import FirecrawlApp
import aiohttp
import asyncio
import json
import logging
import traceback
from typing import Dict
from urllib.parse import urlparse
from utils import get_openai_api_key, get_serpapi_key
from openai import AsyncOpenAI
from main import scrape_website, search_serpapi
from scraping import scrape_url
import logging
from concurrent.futures import ThreadPoolExecutor

# API Keys
OPENAI_API_KEY = get_openai_api_key()
SERPAPI_KEY = get_serpapi_key()


print("API Keys:")
print(f"OPENAI_API_KEY: {OPENAI_API_KEY}")
print(f"SERPAPI_KEY: {SERPAPI_KEY}")


# LLM config
llm_config = {
    "model": "gpt-4o",
    "config_list": [{"model": "gpt-4o", "api_key": OPENAI_API_KEY}],
}

client = AsyncOpenAI(api_key=OPENAI_API_KEY)

#validate url
def is_valid_url(url: str) -> bool:
    """Validate the URL format."""
    parsed = urlparse(url)
    return all([parsed.scheme, parsed.netloc])

#logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


#async await serp

async def search_serpapi(query: str,  num_results: int = 10, geography: str = "se") -> Dict[str, str]:
    """
    Searches SerpAPI for the given query and scrapes the resulting URLs.

    Args:
        query (str): The search query.
        geography (str): The geography/region for the search.
        num_results (int): Number of search results to retrieve (default is 10).

    Returns:
        Dict[str, str]: A dictionary mapping URLs to their scraped content.
    """
    logger.info(f"Searching SerpAPI for: {query}, num_results={num_results}")

    if not isinstance(num_results, int):
        logger.error(f"Error: num_results is expected to be an integer, but got {type(num_results).__name__}")
        raise TypeError("num_results must be an integer")

    url = "https://serpapi.com/search"
    params = {
        "q": query,
        "api_key": SERPAPI_KEY,
        "num": min(num_results, 10),  # Ensure it's capped at 100
        "engine": "google",
        "gl": geography,

       
    }

    logger.info(f"SerpAPI request params: {params}")

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, params=params) as response:
                logger.info(f"SerpAPI response status: {response.status}")
                if response.status == 200:
                    json_response = await response.json()
                    logger.info(f"SerpAPI response data: {json_response}")
                    results = json_response.get("organic_results", [])
                    relevant_urls = [result.get("link") for result in results if
                                     "link" in result and is_valid_url(result.get("link"))]

                    # Use a ThreadPoolExecutor to run scrape_url concurrently
                    with ThreadPoolExecutor(max_workers=5) as executor:
                        loop = asyncio.get_event_loop()
                        scrape_tasks = [loop.run_in_executor(executor, scrape_url, url) for url in relevant_urls]
                        scraped_data = await asyncio.gather(*scrape_tasks)

                    data = {}
                    for url, content in zip(relevant_urls, scraped_data):
                        # print('*****************',url,'************')
                        # print('*****************',content,'************')
                        data[url] = content
                    return data
                else:
                    logger.error(f"SerpAPI request failed with status: {response.status}")
                    return {"error": f"SerpAPI request failed with status: {response.status}"}
        except Exception as e:
            logger.error(f"Exception during SerpAPI request: {e}\n{traceback.format_exc()}")
            return {"error": str(e)}

#test
async def test_search_serpapi():
    query = "EVs revenue in last 2 years in Sweden"
    num_results = 5
    geography = "se"
    result = await search_serpapi(query, num_results, geography)
    # print(result)

# Agents
initializer = autogen.UserProxyAgent(
    name='init',
    code_execution_config={
        "use_docker": False,
    },
    llm_config=None,
    human_input_mode="NEVER",
    is_termination_msg=lambda x: x.get("content", "").rstrip().endswith("Terminate"),
)
query_generator = autogen.AssistantAgent(
    name='Query_Generator',
    llm_config=llm_config,
    system_message="""
    You are a helpful assistant that generates queries based on the user's request for searching the web.
    """
)


data_collector = autogen.AssistantAgent(
    name='Retrieve_Action_1',
    llm_config=llm_config,
    system_message="""
    You are a helpful assistant that collects data from the using search_serpapi function.
    Carefully read through the entire text.
    Identify distinct categories of information that can be organized into separate tables.
    Create tables using Markdown syntax.
    Ensure all relevant data from the text is included in the tables.
    Use appropriate column headers for each table.
    Maintain the original data values and units as presented in the text.
    If exact numbers are given, use them. If ranges or approximations are provided, include them as such.
    For time-series data, organize chronologically if possible.
    Include any relevant metadata (e.g., dates, sources) as notes below each table.
    take current date into account when collecting data.
    Use consistent Currency for all the data and should be based on the geography.

    Potential Tables to Create:

    Market Share Data only if it is available
    Market Revenue Data
    Sales Volumes
    Top Selling Models
    Economic Indicators
    Year-to-Date Performance
    """
)

data_analyst = autogen.AssistantAgent(
    name='Research_Action_1',
    llm_config=llm_config,
    system_message="""
    You are a helpful assistant that analyzes the data collected by the data_collector.
    and creates a report based on the data.
    once you have provided the report, say Terminate.
    """
)

def state_transition(last_speaker, group_chat):
    messages = group_chat.messages
    if last_speaker is initializer:
        #init -> retrieve
        return query_generator
    elif last_speaker is query_generator:
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
    agents=[initializer, query_generator, data_collector, data_analyst],
    messages=[],
    max_round=10,
    speaker_selection_method=state_transition,
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
            You are tasked with converting the following raw text data into structured tables for analysis. Do not summarize or interpret the data; your role is solely to organize the information into clear, readable tables.

            ## Instructions:

            1. Carefully read through the entire text.
            2. Identify distinct categories of information that can be organized into separate tables.
            3. Create tables using Markdown syntax.
            4. Ensure all relevant data from the text is included in the tables.
            5. Use appropriate column headers for each table.
            6. Maintain the original data values and units as presented in the text.
            7. If exact numbers are given, use them. If ranges or approximations are provided, include them as such.
            8. For time-series data, organize chronologically if possible.
            9. Include any relevant metadata (e.g., dates, sources) as notes below each table.

            ## Potential Tables to Create:

            1. Market Share Data
            2. Sales Volumes
            3. Top Selling Models
            4. Economic Indicators
            5. Year-to-Date Performance

            ## Example Table Format:

            ```markdown
            | Column 1 | Column 2 | Column 3 |
            |----------|----------|----------|
            | Data 1   | Data 2   | Data 3   |
            | Data 4   | Data 5   | Data 6   |
            ```

            Note: Adjust the number and names of columns as needed for each table.

            After creating the tables, list any data that couldn't be easily tabulated, maintaining its original format.

            Begin the conversion process now, using the text provided below:
            {data}
            """}
        ],
    )
    return summary.choices[0].message.content
async def main():
    result = await initializer.a_initiate_chat(manager, message="Väderstad Sweden last 3 years Revenue and future predictions") 
    

if __name__ == "__main__":
    asyncio.run(main())




    



# if __name__ == "__main__":
#     asyncio.run(test_search_serpapi())






