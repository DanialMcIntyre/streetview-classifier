import requests
import mercantile
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import random
import time
from typing import List, Dict

#Config
TOKEN = ""
IMAGES_PER_CITY = 10
MASTER_DIR = "canada_full_city"
MAX_WORKERS = 30
REQUESTS_PER_SECOND = 10

CITIES = {
    "ottawa/gatineau":      [-75.938347,45.244301,-75.45704,45.502476],
    "toronto":              [-79.587052,43.60741,-79.113219,43.825279],
    "montreal":             [-73.840956,45.413688,-73.487333,45.652261],
    "vancouver":            [-123.265566,49.198931,-123.023242,49.316171],
    "calgary":              [-114.279455,50.848748,-113.909353,51.188257],
    "edmonton":             [-113.713841,53.394321,-113.344344,53.651086],
    "winnipeg":             [-97.325875,49.763144,-96.956529,49.972925],
    "saskatoon":            [-106.765098,52.071159,-106.536187,52.202335],
    "quebec_city":          [-71.371256,46.762893,-71.190277,46.879747],
    "hamilton":             [-80.045308,43.19374,-79.740724,43.301648],
    "kitchener/waterloo":   [-80.573458,43.326851,-80.257256,43.506828],
    "halifax":              [-63.823841,44.53971,-63.205,44.851819],
    "victoria":             [-123.391097,48.405878,-123.273155,48.472826],
    "st_johns":             [-52.919474,47.449785,-52.611841,47.659795],
    "charlottetown":        [-63.7443,46.1149,-62.506,46.4502]
}

# Rate limiter
class RateLimiter:
    def __init__(self, calls_per_second):
        self.calls_per_second = calls_per_second
        self.min_interval = 1.0 / calls_per_second
        self.last_called = 0

    def wait(self):
        now = time.time()
        elapsed = now - self.last_called
        sleep_time = max(0, self.min_interval - elapsed)
        if sleep_time > 0:
            time.sleep(sleep_time)
        self.last_called = time.time()
rate_limiter = RateLimiter(REQUESTS_PER_SECOND)

def fetch_images_from_tile(tile: mercantile.Tile, city: str, idx: int, total: int, retries: int = 3) -> list[Dict]:
    rate_limiter.wait()
    west, south, east, north = mercantile.bounds(tile)
    bbox_str = f"{west:.6f},{south:.6f},{east:.6f},{north:.6f}"

    for attempt in range(retries):
        try:
            resp = requests.get(
                "https://graph.mapillary.com/images",
                params={
                    "access_token": TOKEN,
                    "bbox": bbox_str,
                    "fields": "id,thumb_2048_url,geometry,captured_at",
                    "limit": 2000
                },
                timeout=60
            )
            resp.raise_for_status()
            data = resp.json().get("data", [])
            filtered = []
            for img in data:
                cam_type = img.get("camera_type", "").lower()
                if any(bad in cam_type for bad in ["helmet", "head", "body", "spherical"]):
                    continue
                img["city"] = city
                filtered.append(img)
            return filtered

        except requests.exceptions.HTTPError as e:
            code = resp.status_code if "resp" in locals() else "N/A"
            if code >= 500 and attempt < retries - 1:
                wait = 2 ** attempt + random.random()
                print(f"[RETRY] {city}: tile {idx + 1}/{total} ({tile.x}/{tile.y}/{tile.z}) "
                      f"(HTTP {code}) → retrying in {wait:.1f}s")
                time.sleep(wait)
                continue
            print(f"[FAIL]  {city}: tile {idx + 1}/{total} ({tile.x}/{tile.y}/{tile.z}) "
                  f"(HTTP {code}) → {e}")
            return []

        except Exception as e:
            print(f"[FAIL]  {city}: tile {idx + 1}/{total} ({tile.x}/{tile.y}/{tile.z}) "
                  f"→ {type(e).__name__}: {e}")
            return []


#Fetch images
all_images: List[Dict] = []
seen_ids: set[str] = set() 

print("Collecting images with parallel tile fetching (unique per id)...")
start_all = time.time()

for city, bbox in CITIES.items():
    start_city = time.time()
    print(f"\nTiling {city.title()} @ zoom 14...")

    tiles = list(mercantile.tiles(*bbox, zooms=[14]))
    print(f"  Total tiles: {len(tiles)} → using {MAX_WORKERS} workers")

    city_images: List[Dict] = []
    city_seen: set[str] = set()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_tile = {
            executor.submit(fetch_images_from_tile, tile, city, idx, len(tiles)): tile
            for idx, tile in enumerate(tiles)
        }

        for future in tqdm(as_completed(future_to_tile),
                           total=len(tiles),
                           desc=f"{city} tiles",
                           leave=False):
            imgs = future.result()

            for img in imgs:
                img_id = img["id"]
                if img_id not in seen_ids and img_id not in city_seen:
                    city_images.append(img)
                    city_seen.add(img_id)
                    seen_ids.add(img_id)

    end_city = time.time()
    print(f"  {city.title():12} → {len(city_images):,} unique images "
          f"({len(city_seen):,} total fetched) in {end_city - start_city:.1f}s")
    all_images.extend(city_images)

end_all = time.time()
print(f"\nCollected {len(all_images):,} unique images across all cities "
      f"in {end_all - start_all:.1f}s")

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

#Download images to computer
os.makedirs(MASTER_DIR, exist_ok=True)
total_size = 0

def download(img):
    global total_size
    city, img_id = img["city"], img["id"]
    url = img["thumb_2048_url"]
    path = os.path.join(MASTER_DIR, city, f"{img_id}.jpg")
    os.makedirs(os.path.dirname(path), exist_ok=True)

    if os.path.exists(path):
        size = os.path.getsize(path)
        total_size += size
        return f"EXISTS: {city}/{img_id}"

    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        size = len(r.content)
        with open(path, "wb") as f:
            f.write(r.content)
        total_size += size
        return f"DOWNLOADED: {city}/{img_id}: {size/1024:.2f}KB"
    except Exception as e:
        return f"FAILED: {city}/{img_id} ({e})"

print("\nDownloading images...")
start_dl = time.time()
with ThreadPoolExecutor(50) as pool:
    for line in pool.map(download, images_to_download):
        print("  " + line)
end_dl = time.time()

print(f"\nDONE! {len(images_to_download)} images → {total_size / (1024**3):.2f} GB")
print(f"Download time: {end_dl - start_dl:.1f}s")
