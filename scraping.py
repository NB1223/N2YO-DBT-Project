from selenium import webdriver
from selenium.webdriver.safari.service import Service as SafariService
from selenium.webdriver.common.by import By
from selenium.webdriver.safari.options import Options
import time

# Set up Safari driver
options = Options()
service = SafariService()
driver = webdriver.Safari(service=service)

# Load the ISS Mimic page
url = "https://iss-mimic.github.io/Mimic/"
driver.get(url)

print("📡 Starting live telemetry stream...\n")

try:
    while True:
        time.sleep(1)  # Wait for the DOM to update
        print(f"🔄 Updated @ {time.strftime('%H:%M:%S')}:\n")
        print("-" * 60)

        rows = driver.find_elements(By.XPATH, "//table/tbody/tr")

        for row in rows:
            cols = row.find_elements(By.TAG_NAME, "td")
            if len(cols) >= 6:
                category = cols[1].text
                telemetry_id = cols[2].text
                info = cols[3].text
                value = cols[4].text
                timestamp = cols[5].text

                print(f"[{category}] {telemetry_id} | {info}")
                print(f"     ➤ Value: {value} at {timestamp}\n")

except KeyboardInterrupt:
    print("\n❌ Stopped by user.")
    driver.quit()
