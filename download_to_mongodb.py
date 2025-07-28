"""
Pebble Device Data Importer

A robust data importer for Pebble IoT device records with concurrent processing,
graceful shutdown handling, and comprehensive error management.
"""

import os
import sys
import signal
import logging
from typing import Dict, List, Optional, Tuple, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from contextlib import contextmanager

import requests
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pymongo import MongoClient
from dateutil import parser as date_parser
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Suppress SSL warnings for development environments
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class Config:
    """Application configuration with sensible defaults."""
    graphql_url: str = "https://pebble.iotex.me/v1/graphql"
    chunk_size: int = int(os.getenv("CHUNK_SIZE", 1000))
    mongo_uri: str = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    db_name: str = os.getenv("DB_NAME", "pebble-dataset")
    records_collection: str = os.getenv("RECORDS_COLLECTION", "pebble_device_record")
    max_workers: int = max(os.cpu_count() or 4, 4)
    # to_created_at converted to isoformat
    to_created_at: str = date_parser.parse(os.getenv("TO_CREATED_AT", "2025-01-01T00:00:00Z")).isoformat()
    # HTTP connection pool settings
    pool_connections: int = max_workers * 2
    pool_maxsize: int = max_workers * 2
    max_retries: int = 3

    @classmethod
    def from_env(cls) -> 'Config':
        """Create configuration from environment variables."""
        return cls(
            mongo_uri=os.getenv("MONGO_URI", cls.mongo_uri),
            chunk_size=int(os.getenv("CHUNK_SIZE", cls.chunk_size)),
            max_workers=int(os.getenv("MAX_WORKERS", cls.max_workers)),
            pool_connections=int(
                os.getenv("POOL_CONNECTIONS", cls.pool_connections)),
            pool_maxsize=int(os.getenv("POOL_MAXSIZE", cls.pool_maxsize))
        )


class GracefulShutdown:
    """Handles graceful shutdown on interrupt signals."""

    def __init__(self):
        self.shutdown_requested = False
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum: int, frame) -> None:
        """Handle shutdown signals gracefully."""
        logger.warning(
            "⚠️ Shutdown signal received. Finishing current operations...")
        self.shutdown_requested = True

    @property
    def should_stop(self) -> bool:
        """Check if shutdown was requested."""
        return self.shutdown_requested


class GraphQLClient:
    """GraphQL client for Pebble API operations."""

    RECORDS_QUERY = """
    query($limit: Int!, $offset: Int!, $from_created_at: timestamptz, $to_created_at: timestamptz!) {
      pebble_device_record(
        limit: $limit,
        offset: $offset,
        order_by: {created_at: asc},
        where: {created_at: {_gte: $from_created_at, _lt: $to_created_at}}
      ) {
        id imei created_at accelerometer gas_resistance gyroscope
        humidity latitude light longitude operator pressure
        signature snr temperature temperature2 timestamp
        updated_at vbat
      }
    }
    """

    RECORDS_QUERY_NO_FROM = """
    query($limit: Int!, $offset: Int!, $to_created_at: timestamptz!) {
      pebble_device_record(
        limit: $limit,
        offset: $offset,
        order_by: {created_at: asc},
        where: {created_at: {_lt: $to_created_at}}
      ) {
        id imei created_at accelerometer gas_resistance gyroscope
        humidity latitude light longitude operator pressure
        signature snr temperature temperature2 timestamp
        updated_at vbat
      }
    }
    """

    def __init__(self, config: Config):
        self.url = config.graphql_url
        self.session = self._create_session(config)

    def _create_session(self, config: Config) -> requests.Session:
        """Create a properly configured requests session with connection pooling."""
        session = requests.Session()

        # Configure retry strategy
        retry_strategy = Retry(
            total=config.max_retries,
            status_forcelist=[429, 500, 502, 503, 504],
            # Updated from method_whitelist
            allowed_methods=["HEAD", "GET", "POST"],
            backoff_factor=1
        )

        # Configure HTTP adapter with proper connection pooling
        adapter = HTTPAdapter(
            pool_connections=config.pool_connections,
            pool_maxsize=config.pool_maxsize,
            max_retries=retry_strategy,
            pool_block=False
        )

        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.verify = False

        # Set connection timeout and keep-alive
        session.headers.update({
            'Connection': 'keep-alive',
            'User-Agent': 'Pebble-Data-Importer/1.0'
        })

        return session

    def execute_query(self, query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a GraphQL query with error handling."""
        try:
            response = self.session.post(
                self.url,
                json={"query": query, "variables": variables},
                timeout=(10, 30)  # (connect_timeout, read_timeout)
            )
            response.raise_for_status()

            data = response.json()
            if "errors" in data:
                raise RuntimeError(f"GraphQL errors: {data['errors']}")

            return data["data"]

        except requests.RequestException as e:
            logger.error(f"Network error during GraphQL query: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during GraphQL query: {e}")
            raise

    def fetch_records(self, offset: int, limit: int,
                      from_created_at: Optional[str] = None,
                      to_created_at: str = "2025-01-01T00:00:00Z") -> List[Dict[str, Any]]:
        """Fetch a batch of records from the GraphQL API."""

        if from_created_at:
            variables = {
                "limit": limit,
                "offset": offset,
                "from_created_at": from_created_at,
                "to_created_at": to_created_at
            }
            query = self.RECORDS_QUERY
        else:
            variables = {
                "limit": limit,
                "offset": offset,
                "to_created_at": to_created_at
            }
            query = self.RECORDS_QUERY_NO_FROM

        result = self.execute_query(query, variables)
        return result.get("pebble_device_record", [])

    def close(self):
        """Close the session and cleanup resources."""
        if self.session:
            self.session.close()


class MongoManager:
    """MongoDB operations manager with connection pooling."""

    def __init__(self, config: Config):
        self.config = config
        self._client = None

    @contextmanager
    def get_client(self):
        """Context manager for MongoDB client connections."""
        client = MongoClient(self.config.mongo_uri)
        try:
            yield client
        finally:
            client.close()

    def get_latest_created_at(self) -> Optional[str]:
        """Get the latest created_at timestamp from the database."""
        try:
            with self.get_client() as client:
                collection = client[self.config.db_name][self.config.records_collection]
                doc = collection.find_one({}, sort=[("created_at", -1)])

                if doc and "created_at" in doc:
                    return doc["created_at"].isoformat()
                return None

        except Exception as e:
            logger.error(f"Error getting latest created_at: {e}")
            return None

    def insert_records(self, records: List[Dict[str, Any]]) -> int:
        """Insert records into MongoDB with date parsing."""
        if not records:
            return 0

        try:
            # Parse dates in records
            processed_records = []
            for record in records:
                processed_record = record.copy()

                for date_field in ["created_at", "updated_at"]:
                    if date_field in processed_record and processed_record[date_field]:
                        try:
                            processed_record[date_field] = date_parser.parse(
                                processed_record[date_field]
                            )
                        except (ValueError, TypeError) as e:
                            logger.warning(
                                f"Failed to parse {date_field}: {e}")
                            processed_record[date_field] = None

                processed_records.append(processed_record)

            with self.get_client() as client:
                collection = client[self.config.db_name][self.config.records_collection]
                result = collection.insert_many(
                    processed_records, ordered=False)
                return len(result.inserted_ids)

        except Exception as e:
            logger.error(f"Error inserting records: {e}")
            return 0


class PebbleDataImporter:
    """Main importer class orchestrating the data import process."""

    def __init__(self, config: Config):
        self.config = config
        self.graphql_client = GraphQLClient(config)
        self.mongo_manager = MongoManager(config)
        self.shutdown_handler = GracefulShutdown()
        self.total_processed = 0

    def process_chunk(self, offset_and_from: Tuple[int, Optional[str]]) -> int:
        """Process a single chunk of data."""
        offset, from_created_at = offset_and_from

        try:
            records = self.graphql_client.fetch_records(
                offset=offset,
                limit=self.config.chunk_size,
                from_created_at=from_created_at,
                to_created_at=self.config.to_created_at
            )

            if records:
                inserted_count = self.mongo_manager.insert_records(records)
                logger.info(
                    f"Offset {offset}: {inserted_count}/{len(records)} records processed")
                return len(records)

            return 0

        except Exception as e:
            logger.error(f"Error processing chunk at offset {offset}: {e}")
            return 0

    def run(self) -> None:
        """Execute the main import process."""
        logger.info(
            f"🔄 Starting import with chunks of {self.config.chunk_size} records")
        logger.info(f"Using {self.config.max_workers} workers")

        # Get initial resumption point
        from_created_at = self.mongo_manager.get_latest_created_at()
        if from_created_at:
            logger.info(f"⏩ Resuming from created_at > {from_created_at} to {self.config.to_created_at}")

        while not self.shutdown_handler.should_stop:
            offset = 0
            batch_has_data = False

            with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
                # Prepare batch of work
                batch_tasks = [
                    (offset + i * self.config.chunk_size, from_created_at)
                    for i in range(self.config.max_workers)
                ]

                # Submit tasks
                future_to_offset = {
                    executor.submit(self.process_chunk, task): task[0]
                    for task in batch_tasks
                }

                # Process results
                for future in as_completed(future_to_offset):
                    if self.shutdown_handler.should_stop:
                        break

                    record_count = future.result()
                    if record_count > 0:
                        batch_has_data = True
                        self.total_processed += record_count

            if not batch_has_data:
                logger.info("No more data to process")
                break

            # Refresh from_created_at and reset offset
            from_created_at = self.mongo_manager.get_latest_created_at()
            logger.info(f"🔄 Refreshing from_created_at: {from_created_at}")
            logger.info(f"Total processed so far: {self.total_processed}")

        logger.info(
            f"✅ Import completed. Total records processed: {self.total_processed}")

    def cleanup(self):
        """Cleanup resources."""
        try:
            self.graphql_client.close()
        except Exception as e:
            logger.warning(f"Error during cleanup: {e}")


def main():
    """Main entry point."""
    importer = None
    try:
        config = Config.from_env()
        importer = PebbleDataImporter(config)
        importer.run()

    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)
    finally:
        if importer:
            importer.cleanup()


if __name__ == "__main__":
    main()
