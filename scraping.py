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

def scrape_url(url, max_retries=3, delay=5):
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-infobars")
    options.add_argument("--start-maximized")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)

    for attempt in range(max_retries):
        try:
            # First, try with requests
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            
            # If successful, parse the content
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # If requests method fails, try with Selenium
            if "403 Forbidden" in soup.text or "Access Denied" in soup.text:
                raise Exception("Access denied, trying with Selenium")
            
        except Exception as e:
            print(f"Requests method failed: {e}. Trying with Selenium.")
            
            time.sleep(delay + random.uniform(1, 3))
            
            driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            driver.get(url)
            
            # Wait for the body to be present
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            
            # Simulate human-like scrolling
            total_height = int(driver.execute_script("return document.body.scrollHeight"))
            for i in range(1, total_height, 200):
                driver.execute_script(f"window.scrollTo(0, {i});")
                time.sleep(random.uniform(0.1, 0.3))
            
            # Wait for any dynamic content to load
            time.sleep(random.uniform(2, 4))
            
            # Get the page source
            page_source = driver.page_source
            
            driver.quit()
            
            # Parse the HTML content
            soup = BeautifulSoup(page_source, 'html.parser')
        
        # Extract meta data
        meta_data = {
            "title": soup.title.string if soup.title else "",
            "description": soup.find("meta", attrs={"name": "description"})["content"] if soup.find("meta", attrs={"name": "description"}) else "",
            "keywords": soup.find("meta", attrs={"name": "keywords"})["content"] if soup.find("meta", attrs={"name": "keywords"}) else "",
        }
        
        # Find the main content
        main_content = (
            soup.find('main') or 
            soup.find('article') or 
            soup.find('div', class_='content') or
            soup.find('div', class_='post-content') or
            soup.find('div', class_='entry-content') or
            soup.find('div', id='content') or
            soup.find('div', class_='main-content') or
            soup.find('body')  # Fallback to body if no specific content found
        )
        
        if not main_content:
            return json.dumps({"error": "Could not find main content"})
        
        # Extract text content
        text_content = main_content.get_text(separator='\n', strip=True)
        
        # Extract image URLs
        image_urls = [img['src'] for img in main_content.find_all('img') if 'src' in img.attrs]
        
        # Prepare the JSON output
        output = {
            "meta_data": meta_data,
            "main_content": text_content,
            "imagesUrl": image_urls
        }
        
        return json.dumps(output, ensure_ascii=False, indent=2)
    
    return json.dumps({"error": "Max retries reached, unable to scrape the content"})

# Example usage
if __name__ == "__main__":
    url = "https://cleantechnica.com/2023/12/02/evs-take-60-6-share-in-sweden/"
    result = scrape_url(url)
    # print(result)
    
    # Save the result as JSON
    output_dir = "scraped_data"
    os.makedirs(output_dir, exist_ok=True)
    filename = os.path.join(output_dir, "scraped_result.json")
    save_json(json.loads(result), filename)
