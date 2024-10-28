from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import time
import random
import requests
import json
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def save_json(data, filename):
    """
    Save the given data as a JSON file.
    
    Args:
    data: The data to be saved (should be JSON serializable)
    filename: The name of the file to save the data to
    """
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    print(f"Data saved to {filename}")

def scrape_url(url: str) -> dict:
    """
    Scrape the given URL and return relevant information.
    """
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        # Extract title
        title = soup.title.string if soup.title else ""

        # Extract description
        description_meta = soup.find("meta", attrs={"name": "description"})
        description = description_meta.get("content", "") if description_meta else ""

        # Extract main content (this is a simple example, might need refinement)
        main_content = ' '.join([p.text for p in soup.find_all('p')])

        return {
            "url": url,
            "title": title,
            "description": description,
            "main_content": main_content
        }
    except requests.RequestException as e:
        logger.error(f"Error scraping {url}: {str(e)}")
        return {
            "url": url,
            "error": str(e)
        }
    except Exception as e:
        logger.error(f"Unexpected error scraping {url}: {str(e)}")
        return {
            "url": url,
            "error": f"Unexpected error: {str(e)}"
        }

# Example usage
if __name__ == "__main__":
    url = "https://analyticpartners.com/solutions/commercial-analytics/#:~:text=The%20term%20refers%20to%20the,decisions%20in%20a%20commercial%20setting"
    result = scrape_url(url)
    # print(result)
    
    # Save the result as JSON
    output_dir = "scraped_data"
    os.makedirs(output_dir, exist_ok=True)
    filename = os.path.join(output_dir, "scraped_result.json")
    save_json(result, filename)
