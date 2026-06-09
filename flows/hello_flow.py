from prefect import flow, task
from datetime import datetime
import time

@task(log_prints=True)
def say_hello(name: str) -> str:
    print(f"Ciao, {name}!")
    return f"Hello, {name}"

@task(log_prints=True, retries=2, retry_delay_seconds=5)
def fetch_data(source: str) -> dict:
    """Task con retry automatico in caso di errore."""
    print(f"Fetching data from {source}...")
    time.sleep(1)  # simula latenza
    return {"source": source, "timestamp": datetime.now().isoformat(), "records": 42}

@task(log_prints=True)
def process_data(data: dict) -> str:
    records = data["records"]
    print(f"Processing {records} records from {data['source']}")
    result = f"Processed {records} records"
    return result

@flow(name="hello-pipeline", log_prints=True)
def hello_pipeline(name: str = "World", source: str = "api"):
    """Flow principale che orchestra i task."""
    greeting = say_hello(name)
    raw = fetch_data(source)
    result = process_data(raw)
    print(f"Pipeline completed: {greeting} | {result}")
    return result

if __name__ == "__main__":
    hello_pipeline(name="Sandro", source="database")

