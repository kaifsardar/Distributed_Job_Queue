import random
import time

def resize_image_handler(payload):
    print(f"  [handler] resizing {payload.get('path')}...")
    time.sleep(10)  # simulate work
    if random.random() < 0.3:  # 30% chance of "failure", so we can watch retries happen
        raise RuntimeError("simulated resize failure")
    print(f"  [handler] done")

HANDLERS = {
    "resize_image": resize_image_handler,
}