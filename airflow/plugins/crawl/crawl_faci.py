import os
import time
import json
import shutil
from datetime import datetime, timedelta
from typing import Set

import pandas as pd
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, row_number, monotonically_increasing_id
from spark_session.spark_config import get_spark_session

def initialize_driver():
    options = webdriver.ChromeOptions()
    #options.add_argument("--headless")
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
    options.add_argument("--window-size=1920,1080")
    driver = webdriver.Chrome(options=options)
    return driver

def scroll_and_click_see_more(driver):
    previous_count = 0  # Số lượng chuyến xe trước đó
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1)
        
        bus_elements = driver.find_elements(By.CLASS_NAME, "bus-name")
        current_count = len(bus_elements)

        if current_count == previous_count:
            # print(f"Đã tải hết dữ liệu ({current_count} chuyến).")
            break
        
        previous_count = current_count
        
        try:
            button_xpath = "//button[contains(@class, 'load-more')]"
            button = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, button_xpath))
            )
            driver.execute_script("arguments[0].click();", button)
            # print(f"Nhấn 'See more'... Tổng số chuyến: {current_count}")
            time.sleep(1)
        except:
            # print("Không tìm thấy nút 'See more', có thể đã hết dữ liệu.")
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

def extract_facilities_for_bus(driver, bus_entry):
    """Extract facilities for a specific bus"""
    facilities = []
    wait = WebDriverWait(driver, 10)
    
    try:
        # Click on detail button for this bus
        bus_entry["button"].click()
        time.sleep(1)  # Wait for detail panel to load
        
        # Click next tab button
        next_tab_button = wait.until(EC.element_to_be_clickable((By.XPATH, 
            "//div[@class='ant-tabs ant-tabs-top DetailInfo__TabsStyled-sc-2a71o4-1 bPfTor ant-tabs-line']//span[contains(@class, 'ant-tabs-tab-next')]")))
        next_tab_button.click()
        time.sleep(1)  # Wait after click
        
        # Click facility tab
        facility_tab = wait.until(EC.element_to_be_clickable((By.XPATH, 
            "//div[@role='tab' and @id='FACILITY']")))
        facility_tab.click()
        time.sleep(1)  # Wait after click
        
        # Extract facilities
        facility_container = wait.until(EC.presence_of_element_located((By.XPATH, 
            "//div[contains(@class, 'ant-row') and contains(@class, 'Facilities__FacilityRow')]")))
        facility_elements = facility_container.find_elements(By.CLASS_NAME, "name")
        
        for facility in facility_elements:
            facility_text = facility.text.strip()
            if facility_text:
                facilities.append(facility_text)
        
        # print(f"  - {bus_entry['name']}: Found {len(facilities)} facilities: {facilities}")
        
        # Go back to listing page
        # driver.back()
        bus_entry["button"].click()
        time.sleep(1)  # Wait after navigation
        
        # Wait for listing page to reload
        wait.until(EC.presence_of_all_elements_located((By.CLASS_NAME, "bus-name")))
        
        return facilities
        
    except Exception as e:
        print(f"  - Error extracting facilities for {bus_entry['name']}: {e}")
        # Try to go back to listing page if there was an error
        try:
            # driver.back()
            time.sleep(1)
            wait.until(EC.presence_of_all_elements_located((By.CLASS_NAME, "bus-name")))
        except:
            pass
        return []
    
def get_company_id(province, key, driver, date):
    # date = datetime.now().strftime("%d-%m-%Y")
    # url = f"https://vexere.com/en-US/bus-ticket-booking-from-sai-gon-to-{province}-{key}.html?date={date}"
    url = f"https://vexere.com/vi-VN/ve-xe-khach-tu-sai-gon-di-{province}-{key}.html?date={date}"
    driver.get(url)

    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "bus-name"))
        )
        scroll_and_click_see_more(driver)
    except:
        print(f"Không thể tải dữ liệu từ {url}")
        return []

    bus_list = []
    bus_id = 1
    
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
            bus_names[i],  # Fixed: use the i-th bus name
            company_ids[i],
        ])
        bus_id += 1

    return bus_list


def crwl_facility():
    spark = get_spark_session("crwl_faci")
    
    # Read Delta Lake table
    delta_table_path = "s3a://bronze/facility"
    processed_buses_df = spark.read.format("delta").load(delta_table_path)
    # Extract processed bus names
    processed_bus_names = set(processed_buses_df.select('bus_name').rdd.flatMap(lambda x: x).collect())
    
    
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
    df = pd.DataFrame(columns=["Id", "Bus_Name", "Facilities"])
    driver = initialize_driver()
    id_counter = 1  # Initialize ID counter
    
    for province, key in provinces_keys.items():
        _ids = get_company_id(province, key, driver, _date)
        list_id = [i[1] for i in _ids]
        unique_ids = list(set(list_id))
        for company_id in unique_ids:
            company_url = f"https://vexere.com/vi-VN/ve-xe-khach-tu-sai-gon-di-{province}-{key}.html?date={_date}&companies={company_id}&sort=time%3Aasc"    
            try:
                # Load the page
                driver.get(company_url)
                time.sleep(1)
                
                # Get all bus entries (name + button)
                bus_entries = get_bus_names_and_buttons(driver)
                
                # Use a list to store all bus names and their facilities
                all_buses = []
                
                # Dictionary to track processed buttons to avoid reprocessing the exact same DOM element
                processed_buttons = {}
                
                # Set to track processed bus names within this company ID
                # processed_bus_names = set()
                
                print(f"\nProcessing {len(bus_entries)} bus entries for company ID {company_id}:")
                
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
                    
                    # Log every bus we're processing with a unique ID for debugging
                    print(f"  [{index+1}] Processing bus: '{bus_name}'")
                    processed_buttons[button_id] = True
                    
                    # Extract facilities for this bus
                    facilities = extract_facilities_for_bus(driver, bus_entry)
                    
                    # Add to our list - each bus gets its own entry
                    all_buses.append({
                        "bus_name": bus_name,
                        "facilities": facilities,
                        "company_id": company_id
                    })
                
                if all_buses:
                    # Generate IDs for this batch of buses
                    ids = list(range(id_counter, id_counter + len(all_buses)))
                    id_counter += len(all_buses)  # Update the counter for the next batch
                    
                    bus_names = [bus["bus_name"] for bus in all_buses]
                    bus_facilities = [bus["facilities"] for bus in all_buses]
                    
                    temp_df = pd.DataFrame({
                        "Id": ids,
                        "Bus_Name": pd.Series(bus_names, dtype="string"),
                        "Facilities": bus_facilities
                    })
                    df = pd.concat([df, temp_df], ignore_index=True)
                    
                    print(f"\nAdded {len(all_buses)} buses for company ID {company_id}:")
                    for i, name in enumerate(bus_names):
                        print(f"  {ids[i]}: '{name}'")
                
            except Exception as e:
                print(f"Error during execution: {e}")

    driver.quit()
    os.makedirs('raw/facility', exist_ok=True)
    json_path = os.path.join('raw/facility', 'bus_facilities_temp.json')
    df.to_json(json_path, orient='records', force_ascii=False)

    # --- append to old bus_faci json ---
    data_a = spark.read.json(f"file://{os.environ['BUS_FACILITY_PATH']}/bus_facilities.json")
    data_b = spark.read.json(f"file://{os.environ['BUS_FACILITY_PATH']}/bus_facilities_temp.json")

    if data_b.count() > 0:
        max_id = data_a.agg({"Id": "max"}).collect()[0][0] or 0
        data_b_indexed = data_b.withColumn("new_id", monotonically_increasing_id())
    
        window_spec = Window.orderBy("new_id")
        data_b_updated = data_b_indexed.withColumn("row_num", row_number().over(window_spec)) \
            .withColumn("Id", col("row_num") + max_id) \
            .drop("row_num", "new_id")
    
        data_all = data_a.unionByName(data_b_updated)
    else:
        print("bus_facilities_temp.json is empty. Skipping append.")
        data_all = data_a
    
    data_all = data_all.coalesce(1)
    output_dir = os.path.join(os.environ["BUS_FACILITY_PATH"], "bus_facilities")
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    data_all.write.json("file://{os.environ['BUS_FACILITY_PATH']}/bus_facilities")
    
    final_path = os.path.join(os.environ["BUS_FACILITY_PATH"], "bus_facilities.json")

    for file in os.listdir(output_dir):
        if file.startswith("part-") and file.endswith(".json"):
            full_file_path = os.path.join(output_dir, file)
            shutil.move(full_file_path, final_path)
            break

    spark.stop()
    print("Data saved to bus_facilities.json")