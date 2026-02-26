from asyncua import Client, ua
from asyncua.crypto.security_policies import SecurityPolicyBasic256Sha256
from logger import save_to_csv, flush_buffer
from dotenv import load_dotenv
from tag_extractor import extract_tags, PREFIX
import os
import asyncio
import time

load_dotenv()

CLIENT_USERNAME = os.getenv("CLIENT_USERNAME")
CLIENT_PASSWORD = os.getenv("CLIENT_PASSWORD")
APP_URI = os.getenv("APP_URI")
SERVER_URL = os.getenv("SERVER_URL")
APPLICATION_NAME = os.getenv("APPLICATION_NAME")

CERT_PATH = "certs/client_cert.pem"
KEY_PATH  = "certs/client_key.pem"

TAGS = extract_tags(save_to_file=False)

async def main():

    print(f"Connecting to {SERVER_URL}...")
    input("Press Enter to continue...")

    client = Client(url=SERVER_URL)

    client.application_uri = APP_URI
    client.name = APPLICATION_NAME

    await client.set_security(
        SecurityPolicyBasic256Sha256,
        CERT_PATH,
        KEY_PATH,
        mode=ua.MessageSecurityMode.SignAndEncrypt,
    )

    client.set_user(CLIENT_USERNAME)
    client.set_password(CLIENT_PASSWORD)
    
    node = None

    async with client:
        print("✅ Connected to KepServer")
        
        server_time = client.get_node("ns=0;i=2258")
        print(await server_time.read_value())

        try:
            while True: 
                print("================================================")

                nodes = [client.get_node(tag) for tag in TAGS]
                values = await client.read_values(nodes)
                timestamp = time.time()

                await asyncio.gather(*[
                    save_to_csv(node.nodeid.to_string().removeprefix(PREFIX), value, timestamp)
                    for node, value in zip(nodes, values)
                ])

                await asyncio.sleep(5)
        
        except KeyboardInterrupt:
            print("Stopping logger...")

        finally:
            await flush_buffer()
            print("✅ Buffer flushed, disconnected.")

    await client.disconnect()
    print("✅ Disconnected from KepServer")
            

if __name__ == "__main__":
    asyncio.run(main())
