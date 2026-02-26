import pandas as pd
import asyncio
from pathlib import Path

BUFFER_SIZE = 20_000  # number of rows to hold before writing
OUTPUT_DIR = Path("logs")  # folder to store CSVs
OUTPUT_DIR.mkdir(exist_ok=True)
FILE_PREFIX = "log"

# Internal state
buffer = []
file_index = 0
current_file = OUTPUT_DIR / f"{FILE_PREFIX}_{file_index}.csv"

# Initialize first CSV with header
pd.DataFrame(columns=["tag", "value", "timestamp"]).to_csv(current_file, index=False)
lock = asyncio.Lock()  # async-safe lock for concurrent writes


async def save_to_csv(tag_name: str, value: any, timestamp: float):
    """Append log entry to buffer; flush to CSV if buffer full."""
    global buffer, file_index, current_file

    async with lock:
        buffer.append({"tag": tag_name, "value": value, "timestamp": timestamp})

        if len(buffer) >= BUFFER_SIZE:
            df = pd.DataFrame(buffer)
            df.to_csv(current_file, mode="a", header=False, index=False)
            buffer = []

            # Rotate file
            file_index += 1
            current_file = OUTPUT_DIR / f"{FILE_PREFIX}_{file_index}.csv"
            pd.DataFrame(columns=["tag", "value", "timestamp"]).to_csv(current_file, index=False)


async def flush_buffer():
    """Flush remaining rows in buffer to disk."""
    global buffer, current_file
    async with lock:
        if buffer:
            df = pd.DataFrame(buffer)
            df.to_csv(current_file, mode="a", header=False, index=False)
            buffer = []