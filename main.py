import json
import gzip
import os
import uuid
import logging
from collections import defaultdict
from typing import Any, List, Dict
import argparse
from datetime import datetime, timedelta


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


class LocalStoragePartitionWriter:
    def __init__(self, bucket: str, prefix: str) -> None:
        if not bucket or not prefix:
            raise ValueError("Bucket and prefix cannot be empty.")
        self.base_path = os.path.join(bucket, prefix)
        self.manifest_path = os.path.join(self.base_path, "manifest.json")
        logging.info(f"LocalStoragePartitionWriter initialized for path: {self.base_path}")

        os.makedirs(self.base_path, exist_ok=True)

    def write_events(self, events: List[Dict[str, Any]]) -> None:
        if not events:
            logging.warning("No events to write.")
            return

        partitioned_events = defaultdict(list)
        for event in events:
            event_date = event.get("event_date")
            if event_date:
                partitioned_events[event_date].append(event)
            else:
                logging.warning(f"Skipping event due to missing 'event_date': {event}")

        manifest_data = {}

        for date, event_list in partitioned_events.items():
            try:
                partition_dir = os.path.join(self.base_path, f"event_date={date}")
                os.makedirs(partition_dir, exist_ok=True)

                filename = f"events-{uuid.uuid4()}.json.gz"
                file_path = os.path.join(partition_dir, filename)

                json_data = json.dumps(event_list, indent=4).encode('utf-8')
                with gzip.open(file_path, 'wb') as f_out:
                    f_out.write(json_data)

                logging.info(f"Successfully wrote {len(event_list)} events to {file_path}")
                manifest_data[f"event_date={date}"] = {
                    "event_count": len(event_list)
                }
            except Exception as e:
                logging.error(f"Failed to write partition for date {date}: {e}")

        self._write_manifest(manifest_data)

    def _write_manifest(self, manifest_data: Dict[str, Dict[str, int]]) -> None:
        existing_manifest_data: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))

        try:
            try:
                with open(self.manifest_path) as f:
                    existing_manifest_data = json.load(f)
            except Exception:
                pass

            for date, data in manifest_data.items():
                existing_manifest_data[date]['event_count'] += data['event_count']

            with open(self.manifest_path, 'w') as f:
                json.dump(existing_manifest_data, f, indent=4)
            logging.info(f"Manifest file written to {self.manifest_path}")
        except Exception as e:
            logging.error(f"Failed to write manifest file: {e}")


def simulate_events(num_events: int) -> List[Dict[str, Any]]:
    """Generates a list of sample events for testing."""
    events = []
    today = datetime.now()
    for i in range(num_events):
        # Simulate events from the last 3 days
        event_day = today - timedelta(days=i % 3)
        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": "user_login" if i % 2 == 0 else "page_view",
            "user_id": f"user_{i % 10}",
            "event_timestamp": event_day.isoformat(),
            "event_date": event_day.strftime('%Y-%m-%d'), # The crucial partition key
            "payload": {"source": "web", "ip": f"192.168.1.{i}"}
        }
        events.append(event)
    logging.info(f"Generated {num_events} simulated events.")
    return events


def main():
    parser = argparse.ArgumentParser(
        description="Write partitioned event data to local storage."
    )
    parser.add_argument(
        "--bucket",
        type=str,
        default="local_s3_bucket",
        help="The local directory to act as an S3 bucket."
    )
    parser.add_argument(
        "--prefix",
        type=str,
        default="data/events",
        help="The prefix (subdirectory) for the data."
    )
    parser.add_argument(
        "--num-events",
        type=int,
        default=50,
        help="The number of events to simulate."
    )

    args = parser.parse_args()

    # 1. Initialize the writer
    writer = LocalStoragePartitionWriter(bucket=args.bucket, prefix=args.prefix)

    # 2. Simulate some events
    events_to_write = simulate_events(args.num_events)

    # 3. Write the events
    writer.write_events(events_to_write)

    logging.info("--- Script finished ---")
    logging.info(f"Check the output in the '{os.path.join(args.bucket, args.prefix)}' directory.")


if __name__ == "__main__":
    main()

