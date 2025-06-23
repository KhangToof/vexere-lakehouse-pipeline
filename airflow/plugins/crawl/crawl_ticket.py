from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import csv
from datetime import datetime
import time
import concurrent.futures
import traceback
import sys
import os
import queue
import threading

def scroll_and_click_see_more(driver):
    previous_count = 0  # Số lượng chuyến xe trước đó
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        
        bus_elements = driver.find_elements(By.CLASS_NAME, "bus-name")
        current_count = len(bus_elements)

        if current_count == previous_count:
            #print(f"Đã tải hết dữ liệu ({current_count} chuyến).")
            break
        
        previous_count = current_count
        
        try:
            button_xpath = "//button[contains(@class, 'load-more')]"
            button = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, button_xpath))
            )
            driver.execute_script("arguments[0].click();", button)
            #print(f"Nhấn 'See more'... Tổng số chuyến: {current_count}")
            time.sleep(2)
        except:
            #print("Không tìm thấy nút 'See more', có thể đã hết dữ liệu.")
            break

def get_all_bus_data(province, key, driver):
    date = datetime.now().strftime("%d-%m-%Y")
    # url = f"https://vexere.com/en-US/bus-ticket-booking-from-sai-gon-to-{province}-{key}.html?date={date}"
    url = f"https://vexere.com/vi-VN/ve-xe-khach-tu-sai-gon-di-{province}-{key}.html?date={date}"
    driver.get(url)

    try:
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CLASS_NAME, "bus-name"))
        )
        scroll_and_click_see_more(driver)
    except:
        #print(f"Không thể tải dữ liệu từ {url}")
        return []

    bus_list = []
    bus_id = 1
    
    bus_names = [b.text.strip() for b in driver.find_elements(By.CLASS_NAME, "bus-name")]
    fares = [f.text.strip() if f.text else "Không rõ" for f in driver.find_elements(By.CLASS_NAME, "fare")]
    departures_time = [d.text.strip() if d.text else "Không rõ" for d in driver.find_elements(By.CSS_SELECTOR, ".from .hour")]
    departures_place = [d.text.strip() if d.text else "Không rõ" for d in driver.find_elements(By.CSS_SELECTOR, ".from .place")]
    arrivals_time = [a.text.strip() if a.text else "Không rõ" for a in driver.find_elements(By.CSS_SELECTOR, ".content-to-info .hour")]
    arrivals_place = [a.text.strip() if a.text else "Không rõ" for a in driver.find_elements(By.CSS_SELECTOR, ".content-to-info .place")]
    durations = [d.text.strip() if d.text else "Không rõ" for d in driver.find_elements(By.CLASS_NAME, "duration")]
    seat_types = [s.text.strip() if s.text else "Không rõ" for s in driver.find_elements(By.CLASS_NAME, "seat-type")]
    
    num_buses = min(len(bus_names), len(fares), len(departures_time), len(departures_place), len(arrivals_time), len(arrivals_place), len(durations), len(seat_types))
    
    for i in range(num_buses):
        route = f"TP.HCM - {province.replace('-', ' ').title()}"
        bus_list.append([
            bus_id,
            bus_names[i],
            date,
            route,
            departures_time[i],
            arrivals_time[i],
            departures_place[i],
            arrivals_place[i],
            durations[i],
            seat_types[i],
            fares[i]
        ])
        bus_id += 1

    return bus_list

def save_to_csv(data, filename=None):
    _date = datetime.now().strftime("%d-%m-%Y")
    if filename is None:
        filename = f"bus_data_{_date}.csv"
    
    # Extract month-year from current date
    month_year = datetime.now().strftime("%m-%Y")
    
    # Create directory structure: raw/ticket/MM-YYYY/
    folder_path = os.path.join("raw", "ticket", month_year)
    os.makedirs(folder_path, exist_ok=True)
    
    filepath = os.path.join(folder_path, filename)
    
    with open(filepath, mode="w", newline="", encoding="utf-8-sig") as file:
        writer = csv.writer(file)
        writer.writerow(["Bus_Key", "Bus_Name", "Start_Date", "Route", "Departure_Time", "Arrival_Time", "Departure_Place", "Arrival_Place", "Duration", "Type_Bus", "Price"])
        writer.writerows(data)
    
    print(f"Dữ liệu đã được lưu vào {filepath}")

def preload_province(province, key):
    """Just load the webpage for a province without scraping data"""
    try:
        options = webdriver.ChromeOptions()
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
        
        date = datetime.now().strftime("%d-%m-%Y")
        url = f"https://vexere.com/vi-VN/ve-xe-khach-tu-sai-gon-di-{province}-{key}.html?date={date}"
        
        print(f"Đang tải trước trang web cho {province}...")
        driver.get(url)
        
        try:
            # Wait until the page is loaded with bus data
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "bus-name"))
            )
            print(f"Đã tải trước trang web thành công cho {province}")
            return driver  # Return the driver instance for later processing
        except Exception as e:
            print(f"Lỗi khi tải trước trang web cho {province}: {str(e)}")
            driver.quit()
            return None
            
    except Exception as e:
        print(f"Lỗi khởi tạo driver cho {province}: {str(e)}")
        traceback.print_exc()
        return None

def process_preloaded_province(province, driver):
    """Process a province with an already loaded driver"""
    if driver is None:
        print(f"Không có driver đã tải trước cho {province}")
        return []
    
    try:
        print(f"Đang xử lý dữ liệu cho {province} từ trang đã tải trước...")
        
        # The page is already loaded, just need to expand all results
        scroll_and_click_see_more(driver)
        
        # Extract data
        bus_list = []
        bus_id = 1
        
        bus_names = [b.text.strip() for b in driver.find_elements(By.CLASS_NAME, "bus-name")]
        fares = [f.text.strip() if f.text else "Không rõ" for f in driver.find_elements(By.CLASS_NAME, "fare")]
        departures_time = [d.text.strip() if d.text else "Không rõ" for d in driver.find_elements(By.CSS_SELECTOR, ".from .hour")]
        departures_place = [d.text.strip() if d.text else "Không rõ" for d in driver.find_elements(By.CSS_SELECTOR, ".from .place")]
        arrivals_time = [a.text.strip() if a.text else "Không rõ" for a in driver.find_elements(By.CSS_SELECTOR, ".content-to-info .hour")]
        arrivals_place = [a.text.strip() if a.text else "Không rõ" for a in driver.find_elements(By.CSS_SELECTOR, ".content-to-info .place")]
        durations = [d.text.strip() if d.text else "Không rõ" for d in driver.find_elements(By.CLASS_NAME, "duration")]
        seat_types = [s.text.strip() if s.text else "Không rõ" for s in driver.find_elements(By.CLASS_NAME, "seat-type")]
        
        num_buses = min(len(bus_names), len(fares), len(departures_time), len(departures_place), 
                        len(arrivals_time), len(arrivals_place), len(durations), len(seat_types))
        
        date = datetime.now().strftime("%d-%m-%Y")
        for i in range(num_buses):
            route = f"TP.HCM - {province.replace('-', ' ').title()}"
            bus_list.append([
                bus_id,
                bus_names[i],
                date,
                route,
                departures_time[i],
                arrivals_time[i],
                departures_place[i],
                arrivals_place[i],
                durations[i],
                seat_types[i],
                fares[i]
            ])
            bus_id += 1
        
        driver.quit()
        
        # Save data for this province
        #province_filename = f"bus_data_{province}_{date}.csv"
        #save_to_csv(bus_list, province_filename)
        
        return bus_list
    
    except Exception as e:
        print(f"Lỗi khi xử lý {province}: {str(e)}")
        traceback.print_exc()
        try:
            driver.quit()
        except:
            pass
        return []

def crwl_ticket():
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
    
    # Convert to list for easier batch processing
    province_items = list(provinces_keys.items())
    total_provinces = len(province_items)
    
    print(f"Working...")
    
    # Create queues for tasks
    province_queue = queue.Queue()
    for province, key in province_items:
        province_queue.put((province, key))
    
    # Queue for preloaded drivers
    preloaded_queue = queue.Queue()
    
    # Task completion synchronization
    task_lock = threading.Lock()
    all_data_lock = threading.Lock()
    all_bus_data = []
    
    # Function for worker group 1 - preloaders
    def preloader_worker():
        while not province_queue.empty():
            try:
                with task_lock:
                    if province_queue.empty():
                        break
                    province, key = province_queue.get()
                
                driver = preload_province(province, key)
                if driver:
                    preloaded_queue.put((province, driver))
            except Exception as e:
                print(f"Lỗi ở preloader_worker: {str(e)}")
                traceback.print_exc()
    
    # Function for worker group 2 - processors
    def processor_worker():
        while True:
            try:
                # Try to get a preloaded task
                try:
                    province, driver = preloaded_queue.get(timeout=1)
                except queue.Empty:
                    # If both queues are empty, we're done
                    if province_queue.empty() and preloaded_queue.empty():
                        break
                    time.sleep(0.5)  # Short sleep to prevent CPU spinning
                    continue
                
                # Process the province with preloaded driver
                data = process_preloaded_province(province, driver)
                
                # Add data to the combined dataset
                with all_data_lock:
                    all_bus_data.extend(data)
                    
            except Exception as e:
                print(f"Lỗi ở processor_worker: {str(e)}")
                traceback.print_exc()
    
    # Start with 5 processors and 1 preloader
    preloader_threads = []
    processor_threads = []
    
    # Start 1 preloader
    for i in range(1):
        thread = threading.Thread(target=preloader_worker)
        thread.start()
        preloader_threads.append(thread)
    
    # Start 5 processors
    for i in range(3):
        thread = threading.Thread(target=processor_worker)
        thread.start()
        processor_threads.append(thread)
    
    # Wait for all threads to complete
    for thread in preloader_threads:
        thread.join()
    
    for thread in processor_threads:
        thread.join()
    
    # Save the combined data
    _date = datetime.now().strftime("%d-%m-%Y")
    save_to_csv(all_bus_data, f"bus_data_{_date}.csv")
    
    print("Quá trình thu thập dữ liệu đã hoàn tất.")
    return all_bus_data

