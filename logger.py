import pandas as pd
import asyncio
from pathlib import Path
from asyncua import ua
from datetime import datetime
import re
import signal

# ---------------- Configuration ----------------
BUFFER_SIZE = 20_000  # number of rows to hold before writing
OUTPUT_DIR = Path("logs")  # folder to store CSVs
OUTPUT_DIR.mkdir(exist_ok=True)
FILE_PREFIX = "log"
PRINT_INTERVAL = 5  # seconds between live value prints
FLUSH_INTERVAL = 5  # seconds between automatic flushes

# ---------------- Internal State ----------------
buffer = []
lock = asyncio.Lock()


def get_latest_file_index():
    """Return the next file_index based on existing files."""
    existing_files = list(OUTPUT_DIR.glob(f"{FILE_PREFIX}_*.csv"))
    indices = []

    for f in existing_files:
        match = re.search(rf"{FILE_PREFIX}_(\d+)\.csv", f.name)
        if match:
            indices.append(int(match.group(1)))

    return max(indices) + 1 if indices else 0


def create_new_file():
    """Create a new CSV file with header and return path."""
    global file_index
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = OUTPUT_DIR / f"{FILE_PREFIX}_{file_index}_{timestamp}.csv"
    pd.DataFrame(columns=["tag", "value", "status_code", "source_timestamp", "server_timestamp"]).to_csv(file_name, index=False)
    return file_name


# Initialize file
file_index = get_latest_file_index()
current_file = create_new_file()

# ---------------- CSV Functions ----------------
async def save_to_csv(tag_name: str, data_value: ua.DataValue):
    """Append log entry to buffer; flush to CSV if buffer full."""
    global buffer, file_index, current_file

    async with lock:
        buffer.append({"tag": tag_name, "value": data_value.Value.Value, "status_code": data_value.StatusCode.name, "source_timestamp": data_value.SourceTimestamp.strftime("%Y-%m-%d %H:%M:%S"), "server_timestamp": data_value.ServerTimestamp.strftime("%Y-%m-%d %H:%M:%S")})

        if len(buffer) >= BUFFER_SIZE:
            df = pd.DataFrame(buffer)
            df.to_csv(current_file, mode="a", header=False, index=False)
            buffer.clear()

            file_index += 1
            current_file = create_new_file()


async def flush_buffer():
    """Flush remaining rows in buffer to disk."""
    global buffer, current_file
    async with lock:
        if buffer:
            df = pd.DataFrame(buffer)
            df.to_csv(current_file, mode="a", header=False, index=False)
            buffer.clear()


async def periodic_flush(interval=FLUSH_INTERVAL):
    """Periodically flush buffer to disk."""
    while True:
        await asyncio.sleep(interval)
        await flush_buffer()


# ---------------- Live Printer ----------------
async def print_values(nodes, values, prefix=""):
    """Print tags and values in a readable format."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("================================================")
    print(f"Timestamp: {timestamp}")

    # Limit rows printed to first 10 for readability
    for node, value in list(zip(nodes, values))[:10]:
        print(f"{node.nodeid.to_string().removeprefix(prefix)} | Value: {value}")

    if len(values) > 10:
        print(f"... and {len(values)-10} more tags.")


# ---------------- Shutdown Handler ----------------
def setup_exit_handler(loop):
    """Flush buffer on application exit."""
    def exit_gracefully(*args):
        print("Exiting... flushing buffer.")
        loop.create_task(flush_buffer())
        # Give a small delay to flush buffer
        loop.call_later(0.5, loop.stop)

    signal.signal(signal.SIGINT, exit_gracefully)
    signal.signal(signal.SIGTERM, exit_gracefully)