import requests
import mercantile
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
import os
import random
import time

TOKEN = ""
IMAGES_PER_CITY = 10
MASTER_DIR = "canada_cities"

CITIES = {
    "toronto":       [-79.65, 43.58, -79.10, 43.86],
    "montreal":      [-73.98, 45.40, -73.45, 45.70],
    "vancouver":     [-123.30, 49.18, -122.95, 49.35],
    "calgary":       [-114.35, 50.84, -113.85, 51.20],
    "edmonton":      [-113.75, 53.38, -113.25, 53.70],
    "ottawa":        [-76.05, 45.20, -75.40, 45.55],
    "winnipeg":      [-97.35, 49.75, -96.90, 50.05],
    "quebec_city":   [-71.45, 46.70, -71.10, 46.95],
    "hamilton":      [-80.10, 43.15, -79.65, 43.45],
    "kitchener":     [-80.70, 43.35, -80.30, 43.60],
    "london_on":     [-81.40, 42.90, -81.10, 43.10],
    "halifax":       [-63.75, 44.58, -63.45, 44.80],
    "victoria":      [-123.55, 48.35, -123.25, 48.55]
}


#Collect all images using tiling
all_images = []

print(f"Collecting ALL images for each city...")

start_all = time.time()
for city, bbox in CITIES.items():
    start_city = time.time()
    print(f"\nTiling {city.title()}...")

    tiles = list(mercantile.tiles(*bbox, zooms=[14]))
    print(f"  Total tiles to check: {len(tiles)}")
    city_images = []

    for tile in tqdm(tiles, desc=f"{city} tiles", leave=False):
        west, south, east, north = mercantile.bounds(tile)
        bbox_str = f"{west:.6f},{south:.6f},{east:.6f},{north:.6f}"

        try:
            resp = requests.get(
                "https://graph.mapillary.com/images",
                params={
                    "access_token": TOKEN,
                    "bbox": bbox_str,
                    "fields": "id,thumb_2048_url,geometry,captured_at",
                    "limit": 200
                },
                timeout=10
            )
            data = resp.json()
            for img in data.get("data", []):
                img["city"] = city
                city_images.append(img)
        except Exception as e:
            pass

    end_city = time.time()
    print(f"  {city.title():12} → {len(city_images)} total images found in {end_city - start_city:.2f} sec")
    all_images.extend(city_images)

end_all = time.time()
print(f"\nCollected {len(all_images)} total images across all cities in {end_all - start_all:.2f} sec")

#Only select specific number of images to download
images_to_download = []

for city in CITIES.keys():
    city_imgs = [img for img in all_images if img["city"] == city]
    if len(city_imgs) <= IMAGES_PER_CITY:
        selected = city_imgs
    else:
        selected = random.sample(city_imgs, IMAGES_PER_CITY)
    images_to_download.extend(selected)

print(f"\nSelected {len(images_to_download)} images to download ({IMAGES_PER_CITY} per city)")

#Download images
os.makedirs(MASTER_DIR, exist_ok=True)
total_size = 0

def download(img):
    global total_size
    city, img_id = img["city"], img["id"]
    url = img["thumb_2048_url"]

    city_dir = os.path.join(MASTER_DIR, city)
    os.makedirs(city_dir, exist_ok=True)
    path = os.path.join(city_dir, f"{img_id}.jpg")

    if os.path.exists(path):
        size = os.path.getsize(path)
        total_size += size
        return f"EXISTS: {city}/{img_id}"

    try:
        r = requests.get(url, timeout=15)
        size = len(r.content)
        with open(path, "wb") as f:
            f.write(r.content)
        total_size += size
        return f"DOWNLOADED: {city}/{img_id}: {size/1024/1024:.2f} MB"
    except Exception:
        return f"FAILED: {city}/{img_id}"

print("\nDownloading selected 2048×1024 panoramas (sorted by city)...")
start_download = time.time()
with ThreadPoolExecutor(32) as pool:
    for line in pool.map(download, images_to_download):
        print("  " + line)
end_download = time.time()

total_gb = total_size / (1024 ** 3)
print(f"\nDONE! {len(images_to_download)} images → {total_gb:.2f} GB saved in '{MASTER_DIR}/'")
print(f"Download completed in {end_download - start_download:.2f} sec")
