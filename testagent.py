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
from utils import get_openai_api_key, get_serpapi_key, get_fc_api_key, print_all_env_variables
from openai import AsyncOpenAI
from main import scrape_website, search_serpapi
from scraping import scrape_url
import logging
from concurrent.futures import ThreadPoolExecutor

# API Keys
OPENAI_API_KEY = get_openai_api_key()
SERPAPI_KEY = get_serpapi_key()
FIRECRAWL_API_KEY = get_fc_api_key()

print("API Keys:")
print(f"OPENAI_API_KEY: {OPENAI_API_KEY}")
print(f"SERPAPI_KEY: {SERPAPI_KEY}")
print(f"FIRECRAWL_API_KEY: {FIRECRAWL_API_KEY}")


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
        "engine": "bing",
        "cc": geography,

       
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

#test
async def test_search_serpapi():
    query = "EVs revenue in last 2 years in Sweden"
    num_results = 5
    geography = "se"
    result = await search_serpapi(query, num_results, geography)
    print(result)

if __name__ == "__main__":
    asyncio.run(test_search_serpapi())





