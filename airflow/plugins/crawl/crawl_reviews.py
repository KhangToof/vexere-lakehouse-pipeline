from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
from datetime import datetime, timedelta
import pandas as pd
import csv
import json
import traceback
import os  # Added import for directory operations
from selenium.common.exceptions import NoSuchElementException, TimeoutException

def initialize_driver():
    """Initialize and configure Chrome WebDriver"""
    options = webdriver.ChromeOptions()
    #options.add_argument("--start-maximized")
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-browser-side-navigation")
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-notifications")
    options.add_argument("--disable-popup-blocking")
    driver = webdriver.Chrome(options=options)
    return driver

def scroll_and_click_see_more(driver):
    """Scroll and click 'see more' to load all buses"""
    previous_count = 0  # Previous bus count
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1)
        
        bus_elements = driver.find_elements(By.CLASS_NAME, "bus-name")
        current_count = len(bus_elements)

        if current_count == previous_count:
            print(f"All data loaded ({current_count} buses).")
            break
        
        previous_count = current_count
        
        try:
            button_xpath = "//button[contains(@class, 'load-more')]"
            button = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, button_xpath))
            )
            driver.execute_script("arguments[0].click();", button)
            print(f"Clicking 'See more'... Total buses: {current_count}")
            time.sleep(1)
        except:
            print("'See more' button not found, may be all data loaded.")
            break

def get_bus_names_and_buttons(driver):
    """Get all unique bus names and their detail buttons"""
    wait = WebDriverWait(driver, 10)
    bus_data = []
    
    try:
        # Wait for bus elements to load
        wait.until(EC.presence_of_all_elements_located((By.CLASS_NAME, "bus-name")))
        
        # Find all ticket containers
        ticket_containers = driver.find_elements(By.XPATH, "//div[contains(@class, 'ticket')]")
        
        for container in ticket_containers:
            try:
                # Get bus name
                bus_name_element = container.find_element(By.CLASS_NAME, "bus-name")
                bus_name = bus_name_element.text.strip()
                
                # Get detail button
                detail_button = container.find_element(By.XPATH, ".//button[contains(@class, 'btn-detail')]")
                
                # Only add if we have both elements
                if bus_name and detail_button:
                    bus_data.append({
                        "name": bus_name,
                        "button": detail_button
                    })
            except NoSuchElementException:
                continue
        
        print(f"Found {len(bus_data)} bus entries")
        return bus_data
        
    except Exception as e:
        print(f"Error getting bus names: {e}")
        return []

def extract_reviews_from_page(driver):
    """Extract reviews from the current page, avoiding duplicates"""
    reviews = []
    processed_reviews = set()

    try:
        # Wait for review containers to be present
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'review-item')]"))
        )
        
        # Get all review containers
        review_containers = driver.find_elements(By.XPATH, "//div[contains(@class, 'review-item')]")
        print(f"Found {len(review_containers)} review containers on current page")
        
        for container in review_containers:
            try:
                # Extract customer name
                customer_name = "Unknown"
                try:
                    customer_name_elem = container.find_element(By.XPATH, ".//p[contains(@class, 'name')]")
                    customer_name = customer_name_elem.text.strip()
                except NoSuchElementException:
                    pass
                
                # Extract stars/rating
                stars = 0
                try:
                    star_elements = container.find_elements(By.XPATH, ".//i[contains(@class, 'color--critical')]")
                    stars = len(star_elements)
                except:
                    pass
                
                # Extract comment - fixed selector to properly locate comments
                comment = "No comment"
                try:
                    comment_elem = container.find_element(By.XPATH, ".//p[contains(@class, 'comment')]")
                    comment = comment_elem.text.strip()
                except NoSuchElementException:
                    pass
                
                # Extract date - fixed selector to properly locate dates
                date = "Unknown Date"
                try:
                    date_elem = container.find_element(By.XPATH, ".//p[contains(@class, 'rated-date')]")
                    date = date_elem.text.strip()
                except NoSuchElementException:
                    pass
                
                # Create a unique key for the review
                review_key = f"{customer_name}|{stars}|{comment}|{date}"
                
                # Add review only if not processed before
                if review_key not in processed_reviews:
                    reviews.append({
                        "customer_name": customer_name,
                        "stars": stars,
                        "comment": comment,
                        "date": date
                    })
                    processed_reviews.add(review_key)
                    print(f"Extracted review: {customer_name}, {stars} stars, comment: {comment[:30]}..., date: {date}")
                
            except Exception as detail_error:
                print(f"Error extracting individual review: {detail_error}")
                traceback.print_exc()
        
        return reviews
    
    except Exception as e:
        print(f"Error extracting reviews from page: {e}")
        traceback.print_exc()
        return []

def extract_reviews_for_bus(driver, bus_entry):
    """Extract reviews for a specific bus"""
    bus_name = bus_entry["name"]
    all_reviews = []
    wait = WebDriverWait(driver, 10)
    page_number = 1
    
    try:
        # Click on detail button for this bus
        bus_entry["button"].click()
        time.sleep(2)  # Wait longer for detail panel to load
        
        # Click review tab
        try:
            review_tab = wait.until(EC.element_to_be_clickable((By.XPATH, 
                "//div[@role='tab' and @id='REVIEW']")))
            review_tab.click()
            time.sleep(2)  # Wait longer after click
        except Exception as e:
            print(f"Error clicking review tab: {e}")
            # Try alternative method to find the tab
            try:
                tabs = driver.find_elements(By.XPATH, "//div[@role='tab']")
                for tab in tabs:
                    if "REVIEW" in tab.get_attribute('id') or "ĐÁNH GIÁ" in tab.text:
                        tab.click()
                        time.sleep(2)
                        break
            except:
                print("Could not find review tab using alternative method")
        
        print(f"Extracting reviews for '{bus_name}'")
        
        # Check if reviews section exists
        try:
            wait.until(EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'review-item')]")))
        except TimeoutException:
            print(f"  No reviews found for '{bus_name}'")
            
            # Close the detail panel and return to the listing
            try:
                close_button = driver.find_element(By.XPATH, "//button[contains(@class, 'close-btn')]")
                close_button.click()
            except:
                try:
                    bus_entry["button"].click()  # Try clicking the button again to close
                except:
                    pass
            time.sleep(1)
            return []
        
        while True:
            # Scroll to bottom of page to ensure all reviews are loaded
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1)
            
            # Extract reviews from current page
            page_reviews = extract_reviews_from_page(driver)
            
            # Add bus name to each review
            for review in page_reviews:
                review["bus_name"] = bus_name
                
            all_reviews.extend(page_reviews)
            print(f"  Page {page_number}: Extracted {len(page_reviews)} reviews")
            
            try:
                # Try to find and click next page button
                next_button_xpath = "//li[contains(@class, 'ant-pagination-next')]"
                next_button = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, next_button_xpath))
                )
                
                # Check if next button is disabled
                if next_button.get_attribute('aria-disabled') == 'true':
                    print("  Reached the last page. Ending pagination.")
                    break
                
                # Click next page button
                driver.execute_script("arguments[0].click();", next_button)
                page_number += 1
                time.sleep(2)
                
            except Exception as pagination_error:
                print("  No more pages to navigate or pagination error.")
                break
                
        # Close the detail panel
        try:
            close_button = driver.find_element(By.XPATH, "//button[contains(@class, 'close-btn')]")
            close_button.click()
        except:
            try:
                bus_entry["button"].click()  # Try clicking the button again to close
            except:
                pass
        time.sleep(1)
        
        print(f"  Total reviews collected for '{bus_name}': {len(all_reviews)}")
        return all_reviews
        
    except Exception as e:
        print(f"Error extracting reviews for {bus_name}: {e}")
        traceback.print_exc()
        
        # Try to go back to listing page if there was an error
        try:
            close_button = driver.find_element(By.XPATH, "//button[contains(@class, 'close-btn')]")
            close_button.click()
        except:
            try:
                bus_entry["button"].click()  # Try clicking the button again to close
            except:
                pass
        time.sleep(1)
            
        return []

def get_company_id(province, key, driver, date):
    url = f"https://vexere.com/vi-VN/ve-xe-khach-tu-sai-gon-di-{province}-{key}.html?date={date}"
    driver.get(url)

    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "bus-name"))
        )
        scroll_and_click_see_more(driver)
    except:
        print(f"Couldn't load data from {url}")
        return []

    bus_list = []
    
    # Locate ticket container elements to extract company IDs
    ticket_containers = driver.find_elements(By.CSS_SELECTOR, "[data-company-id]")
    company_ids = []
    
    for container in ticket_containers:
        company_id = container.get_attribute("data-company-id")
        if company_id:
            company_ids.append(company_id)
        else:
            company_ids.append("Unknown")
    
    bus_names = [b.text.strip() for b in driver.find_elements(By.CLASS_NAME, "bus-name")]
    
    # Adjust to include company_ids in the length check
    data_lengths = [len(bus_names), len(company_ids)]
    num_buses = min(data_lengths)
    
    for i in range(num_buses):
        bus_list.append([
            bus_names[i],
            company_ids[i],
        ])

    return bus_list

def crwl_reviews():
    provinces_keys = {
        "can-tho": "129t1131",
        "an-giang": "129t111",
        "tien-giang": "129t1581",
        "kien-giang": "129t1331",
        "hau-giang": "129t1281",
        "long-an": "129t1391",
        "tra-vinh": "129t1591",
        "soc-trang": "129t1511",
        "dong-thap": "129t1201",
        "vinh-long": "129t1611",
        "bac-lieu": "129t151",
        "ca-mau": "129t1121",
        "ben-tre": "129t171",
    }

    _date = (datetime.now() + timedelta(days=1)).strftime("%d-%m-%Y")
    all_reviews = []
    driver = initialize_driver()
    
    # Track processed company IDs across all provinces
    processed_company_ids = set()
    
    try:
        for province, key in provinces_keys.items():
            print(f"\n--- Processing province: {province} ---")
            
            # Get company IDs for this province
            _ids = get_company_id(province, key, driver, _date)
            list_id = [i[1] for i in _ids]
            unique_ids = list(set(list_id))
            
            for company_id in unique_ids:
                # Skip if we've already processed this company ID
                if company_id in processed_company_ids:
                    print(f"Skipping already processed company ID: {company_id}")
                    continue
                
                # Mark this company ID as processed
                processed_company_ids.add(company_id)
                
                print(f"\nProcessing company ID: {company_id}")
                company_url = f"https://vexere.com/vi-VN/ve-xe-khach-tu-sai-gon-di-{province}-{key}.html?date={_date}&companies={company_id}&sort=time%3Aasc"    
                
                try:
                    driver.get(company_url)
                    time.sleep(3)  # Wait longer for page to load completely
                    
                    # Get all bus entries (name + button)
                    bus_entries = get_bus_names_and_buttons(driver)
                    
                    # Dictionary to track processed buttons to avoid reprocessing the exact same DOM element
                    processed_buttons = {}
                    
                    # Set to track processed bus names within this company ID
                    processed_bus_names = set()
                    
                    # Process each bus
                    for index, bus_entry in enumerate(bus_entries):
                        bus_name = bus_entry["name"]
                        button_id = id(bus_entry["button"])  # Use the object ID as a unique identifier
                        
                        # Skip if we've already processed this exact button element
                        if button_id in processed_buttons:
                            print(f"  Skipping entry with duplicate button: {bus_name}")
                            continue
                        
                        # Skip if we've already processed a bus with this name for the current company
                        if bus_name in processed_bus_names:
                            print(f"  Skipping duplicate bus name: {bus_name}")
                            continue
                        
                        # Mark this bus name as processed
                        processed_bus_names.add(bus_name)
                        processed_buttons[button_id] = True
                        
                        # Extract reviews for this bus
                        bus_reviews = extract_reviews_for_bus(driver, bus_entry)
                        
                        # Add to our collection
                        all_reviews.extend(bus_reviews)
                    
                except Exception as e:
                    print(f"Error processing company {company_id}: {e}")
                    traceback.print_exc()
    
    except Exception as e:
        print(f"Error during execution: {e}")
        traceback.print_exc()
    
    finally:
        driver.quit()
    
    # Create DataFrame and save data
    if all_reviews:
        # Create formatted reviews for output
        formatted_reviews = []
        for review in all_reviews:
            formatted_reviews.append({
                "Bus_Name": review["bus_name"],
                "Customer_Name": review["customer_name"],
                "Stars": review["stars"],
                "Comment": review["comment"],
                "Date": review["date"]
            })
        
        # Create DataFrame for CSV
        df = pd.DataFrame(formatted_reviews)
        
        # Create raw/review directory if it doesn't exist
        os.makedirs('raw/review', exist_ok=True)
        
        # Save to JSON in raw/review directory
        json_path = os.path.join('raw', 'review', 'bus_reviews.json')
        with open(json_path, 'w', encoding='utf-8') as json_file:
            json.dump(formatted_reviews, json_file, ensure_ascii=False, indent=4)
        print(f"Data saved to {json_path}")
    else:
        print("\nNo reviews were collected.")

if __name__ == "__main__":
    crwl_reviews()
