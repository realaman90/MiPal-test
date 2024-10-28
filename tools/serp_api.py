from typing import Dict
import aiohttp
import os
import logging
import traceback
from urllib.parse import urlparse
import asyncio
from concurrent.futures import ThreadPoolExecutor
from tools.web_scrap import scrape_url


def get_env_variable(var_name: str) -> str:
    """
    Get an environment variable and provide helpful error messages.
    """
    value = os.getenv(var_name)
    if value is None:
        print(f"Warning: {var_name} is not set in the environment.")
        return ""
    return value
def get_serpapi_key() -> str:
    """Get the SerpApi key from the environment."""
    return get_env_variable('SERP_API_KEY')

SERPAPI_KEY = get_serpapi_key()
#validate url
def is_valid_url(url: str) -> bool:
    """Validate the URL format."""
    parsed = urlparse(url)
    return all([parsed.scheme, parsed.netloc])

#logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


async def search_serpapi(query: str, num_results: int = 10, geography: str = "se") -> Dict[str, str]:
    """
    Searches SerpAPI for the given query and scrapes the resulting URLs.

    Args:
        query (str): The search query.
        num_results (int): Number of search results to retrieve (default is 10).
        geography (str): The geography/region for the search.

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
        "num": min(num_results, 10),  # Ensure it's capped at 10
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
                    for scraped_item in scraped_data:
                        url = scraped_item['url']
                        if 'error' in scraped_item:
                            logger.warning(f"Error scraping {url}: {scraped_item['error']}")
                            data[url] = f"Error: {scraped_item['error']}"
                        else:
                            data[url] = f"Title: {scraped_item['title']}\nDescription: {scraped_item['description']}\nContent: {scraped_item['main_content'][:500]}..."  # Truncate content for brevity

                    return data
                else:
                    logger.error(f"SerpAPI request failed with status: {response.status}")
                    return {"error": f"SerpAPI request failed with status: {response.status}"}
        except Exception as e:
            logger.error(f"Exception during SerpAPI request: {e}\n{traceback.format_exc()}")
            return {"error": str(e)}

#test

if __name__ == "__main__":
    query = "EVs revenue in last 2 years in Sweden"
    num_results = 5
    geography = "se"
    result = asyncio.run(search_serpapi(query, num_results, geography))
    print(result)
