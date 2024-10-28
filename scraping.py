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
import base64

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

def extract_chart_data(driver, soup):
    charts = []

    print("Searching for charts...")

    # Look for SVG charts
    svg_charts = soup.find_all('svg')
    print(f"Found {len(svg_charts)} SVG elements")
    for svg in svg_charts:
        chart_title = ""
        data = []

        # Try to find the chart title
        svg_title = svg.find('title')
        if svg_title:
            chart_title = svg_title.string.strip()
        else:
            nearby_header = svg.find_previous(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'figcaption'])
            if nearby_header:
                chart_title = nearby_header.get_text(strip=True)

        # Try to extract data from SVG
        text_elements = svg.find_all('text')
        for i in range(0, len(text_elements) - 1, 2):
            label = text_elements[i].get_text(strip=True)
            value_text = text_elements[i+1].get_text(strip=True)
            try:
                value = float(value_text.replace(',', ''))
                data.append({"label": label, "value": value})
            except ValueError:
                print(f"Could not convert '{value_text}' to a number")

        if chart_title and data:
            charts.append({
                "chart_title": chart_title,
                "data": data
            })

    print(f"Total charts found: {len(charts)}")
    return charts

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

    charts = []  # Initialize charts list outside the try-except block

    for attempt in range(max_retries):
        try:
            print(f"Attempt {attempt + 1} to scrape {url}")
            
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
            
            # Parse the HTML content
            soup = BeautifulSoup(page_source, 'html.parser')
            
            # Extract chart data
            charts = extract_chart_data(driver, soup)
            
            break  # If successful, break the loop
            
        except Exception as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            if attempt == max_retries - 1:
                return json.dumps({"error": f"Max retries reached, unable to scrape the content: {e}"})
            time.sleep(delay)
        finally:
            if 'driver' in locals():
                driver.quit()

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
        "imagesUrl": image_urls,
        "charts": charts
    }
    
    return json.dumps(output, ensure_ascii=False, indent=2)
    
# Example usage
if __name__ == "__main__":
    url = "https://www.statista.com/outlook/mmo/electric-vehicles/sweden#units"
    result = scrape_url(url)
    # print(result)
    
    # Save the result as JSON
    output_dir = "scraped_data"
    os.makedirs(output_dir, exist_ok=True)
    filename = os.path.join(output_dir, "scraped_result.json")
    save_json(json.loads(result), filename)
