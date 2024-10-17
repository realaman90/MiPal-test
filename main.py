from dotenv import load_dotenv
load_dotenv()

from autogen import UserProxyAgent, AssistantAgent, ConversableAgent, GroupChat, GroupChatManager
from firecrawl import FirecrawlApp
import aiohttp
import asyncio
import json
import logging
import traceback
from typing import Dict, Any
from urllib.parse import urlparse
from utils import get_openai_api_key, get_serpapi_key, get_fc_api_key, print_all_env_variables
from openai import AsyncOpenAI
from scraping import scrape_url, save_json
import os

# API Keys
OPENAI_API_KEY = get_openai_api_key()
SERPAPI_KEY = get_serpapi_key()
FIRECRAWL_API_KEY = get_fc_api_key()

print("API Keys:")
print(f"OPENAI_API_KEY: {OPENAI_API_KEY}")
print(f"SERPAPI_KEY: {SERPAPI_KEY}")
print(f"FIRECRAWL_API_KEY: {FIRECRAWL_API_KEY}")



# Initialize the AsyncOpenAI client
client = AsyncOpenAI(api_key=OPENAI_API_KEY)
# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# functions


def is_valid_url(url: str) -> bool:
    """Validate the URL format."""
    parsed = urlparse(url)
    return all([parsed.scheme, parsed.netloc])

# Define a semaphore to limit concurrency
SEM = asyncio.Semaphore(5)

async def scrape_website(url: str) -> Dict[str, Any]:
    """
    Scrapes the given URL using the improved scrape_url function with Selenium.
    """
    async with SEM:
        logger.info("Scraping website: %s", url)
        try:
            scraped_content_json = await asyncio.to_thread(scrape_url, url)
            scraped_content = json.loads(scraped_content_json)
            logger.info("Scraped content from %s: %s...", url, str(scraped_content)[:500])
            
            # Save the scraped content as JSON
            output_dir = "scraped_data"
            os.makedirs(output_dir, exist_ok=True)
            filename = os.path.join(output_dir, f"scraped_{url.split('//')[1].replace('/', '_')}.json")
            save_json(scraped_content, filename)
            
            return scraped_content
        except Exception as e:
            logger.error("Failed to scrape %s: %s\n%s", url, e, traceback.format_exc())
            return {"error": str(e)}

async def search_serpapi(query: str,  num_results: int = 10) -> Dict[str, str]:
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
        "engine": "bing",

       
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

                    scrape_tasks = [scrape_website(url) for url in relevant_urls]
                    scraped_data = await asyncio.gather(*scrape_tasks, return_exceptions=True)
                    

                    data = {}
                    for url, content in zip(relevant_urls, scraped_data):
                        print('*****************',url,'************')
                        print('*****************',content,'************')
                        data[url] = content
                    return data
                else:
                    logger.error(f"SerpAPI request failed with status: {response.status}")
                    return {"error": f"SerpAPI request failed with status: {response.status}"}
        except Exception as e:
            logger.error(f"Exception during SerpAPI request: {e}\n{traceback.format_exc()}")
            return {"error": str(e)}






llm_config = {
    "model": "gpt-4o",
    "config_list": [{"model": "gpt-4o", "api_key": OPENAI_API_KEY}],

}

user_proxy = UserProxyAgent(
    name="user_proxy",
    human_input_mode="NEVER",
    max_consecutive_auto_reply=10,
    is_termination_msg=lambda msg: msg.get("content") is not None and "TERMINATE" in msg["content"],
    code_execution_config={
      
        "use_docker": False,
    },
      # Please set use_docker=True if docker is available to run the generated code. Using docker is safer than running the generated code directly.
    llm_config=llm_config,
    system_message="""Reply TERMINATE if the task has been solved at full satisfaction.
                    Otherwise, reply CONTINUE, or the reason why the task is not solved yet. Don't ask for feedback.""",
)


""" Industry Growth Prospects Agent
    This agent evaluates the industry's growth prospects by analyzing historical and forecasted revenue growth, cyclical trends, and seasonal factors.

    Key Tasks:
    Fetch industry revenue data for the past 2-3 years.
    Forecast future revenue growth using machine learning models.
    Compare the growth of this industry with others in a similar category.
"""
coder = AssistantAgent(
    name="Coder",  # the default assistant agent is capable of solving problems with code
    llm_config=llm_config,
)
additional_info = """
    - Fetch industry revenue data for the past 2-3 years and next 5 years.
    - Provide key data in a table for other agent to process this info.
    You can call research_industry_growth_data('query') with appropriate query for the specified geography to gather relevant data from credible sources like statista, IBISworld etc...
    """
industry_growth_data_agent = ConversableAgent(
    name='IndustryGrowthdataAgent',
    llm_config=llm_config,
    system_message="""
    IndustryGrowthdataAgent. You are a helpful assistant highly skilled in presenting data. Your key tasks include:
    
    - Fetch industry revenue data for the past 2-3 years and next 5 years.
    - Provide key data in a markdown table format
    - You can call research_industry_growth_data('query') with appropriate query for the specified geography to gather relevant data from credible sources like statista, IBISworld etc...
    - Output the data in a structured format for further analysis.
    
    """
)



#Register the fuction to the agent
@user_proxy.register_for_execution()
@industry_growth_data_agent.register_for_llm(description="Fetch industry revenue data for the past 2-3 years.")
async def research_industry_growth_prospects(query: str) -> str:
    data = await search_serpapi(query, num_results=5)
    summary = await client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": "You are a helpful assistant. Summarize the content in a concise manner, focusing on revenue data and growth trends."},
            {"role": "user", "content": f"Summarize the content from the webscrape results, focusing on {query}, include all important hard data for every year mentioned in the given data, facts and relevant url, facts and figures:\n{data}"}
        ],
    )
    return summary.choices[0].message.content

#critic agent


async def main():
    try:
        chat_res = await user_proxy.a_initiate_chat(
                industry_growth_data_agent,
                message=f"Evs in Sweden",
                summary_method="reflection_with_llm",
                
            ),
        print('*****************CHAT RESULT*****************')
        # print(chat_res)
    except asyncio.TimeoutError:
        print("The conversation timed out after 60 seconds.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    print('*****************STARTING*****************')
    
    asyncio.run(main())
