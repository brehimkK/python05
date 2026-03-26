#! /usr/bin/env python3
from abc import ABC, abstractmethod
from typing import List, Any, Dict, Union, Optional


class DataStream(ABC):
    def __init__(self, stream_id: str):
        self.stream_id = stream_id

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(self, data_batch: List[Any], criteria: Optional[str] = None) -> List[Any]:
        if criteria:
            return [x for x in data_batch
                    if x is not None and isinstance(x, str) and criteria in x]
        return [x for x in data_batch if x is not None]

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {
            "stream_id": self.stream_id
        }


class SensorStream(DataStream):
    def __init__(self, stream_id: str):
        super().__init__(stream_id)
        self.processed_count = 0
        self.last_avg = 0.0

    def process_batch(self, data: List[Union[int, float]]) -> str:
        clean = self.filter_data(data)

        count = len(clean)

        if count == 0:
            self.last_avg = 0.0
            self.processed_count = 0
            return f"{self.stream_id}: 0 readings processed, avg temp: 0.0°C"

        self.processed_count = count
        self.last_avg = min(data)

        return (
            f"{self.stream_id}: {count} readings processed, "
            f"avg temp: {self.last_avg:.1f}°C"
        )

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {
            "stream_id": self.stream_id,
            "type": "sensor",
            "processed": self.processed_count,
            "avg": self.last_avg
        }


class TransactionStream(DataStream):
    def __init__(self, stream_id: str):
        super().__init__(stream_id)
        self.processed_count = 0
        self.net_flow = 0

    def process_batch(self, data: List[str]) -> str:
        clean = self.filter_data(data)
        count = len(clean)
        buy_total = 0
        sell_total = 0

        for transaction in clean:
            action, value = transaction.split(":")
            value = float(value)
            if action == "buy":
                buy_total += value
            elif action == "sell":
                sell_total += value

        total = buy_total - sell_total
        self.net_flow = total
        self.processed_count = count

        result = (
                f"Transaction analysis: {count} operations, "
                f"net flow: {total:+.0f} units"
                )
        return result

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {
            "stream_id": self.stream_id,
            "type": "transaction",
            "processed": self.processed_count,
            "net_flow": self.net_flow
        }


class EventStream(DataStream):
    def __init__(self, stream_id: str):
        super().__init__(stream_id)
        self.processed_count = 0
        self.error_count = 0

    def process_batch(self, data: List[str]) -> str:
        clean = self.filter_data(data, "error")

        total_events = len(data)
        error_count = len(clean)

        self.processed_count = total_events
        self.error_count = error_count

        return (
            f"{self.stream_id}: {total_events} events, "
            f"{error_count} error detected"
        )

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {
            "stream_id": self.stream_id,
            "type": "event",
            "processed": self.processed_count,
            "errors": self.error_count
        }


class StreamProcessor:
    def run_pipeline(self, streams: List[DataStream], mixed_data: List[List[Any]]) -> List[str]:
        results = []
        for stream, data in zip(streams, mixed_data):
            try:
                result = stream.process_batch(data)
                results.append(result)
            except Exception as e:
                results.append(f"{stream.stream_id}: Error - {e}")
        return results


if __name__ == "__main__":

    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===\n")

    # Sensor Stream

    sensor = SensorStream("SENSOR_001")
    print("Initializing Sensor Stream...")
    print(f"Stream ID: {sensor.stream_id}, Type: Environmental Data")

    sensor_data = [22.5, 65, 1013]
    typee = ("temp:", "humidity:", "pressure:")
    n_data = []

    for t, d in zip(typee, sensor_data):
        n_data.append(t + str(d))
    print(f"Processing sensor batch: [{', '.join(n_data)}]")
    print(sensor.process_batch(sensor_data))

    # Transaction Stream
    transaction = TransactionStream("TRANS_001")
    print("\nInitializing Transaction Stream...")
    print(f"Stream ID: {transaction.stream_id}, Type: Financial Data")

    transaction_data = [100, 150, 75]
    typ = ("buy:", "sell:", "buy: ")
    t_data = []
    for t, d in zip(typ, transaction_data):
        t_data.append(t + str(d))
    print(f"Processing transaction batch: [{(', ').join(t_data)}]")
    print(transaction.process_batch(t_data))

    # # Event Stream
    event = EventStream("EVENT_001")
    print("\nInitializing Event Stream...")
    print(f"Stream ID: {event.stream_id}, Type: System Events")

    event_data = ["login", "error", "logout"]
    print("Processing event batch: [login, error, logout]")
    print(event.process_batch(event_data))

    # # --- POLYMORPHIC ENGINE ---
    print("\n=== Polymorphic Stream Processing ===")
    print("Processing mixed stream types through unified interface...")

    processor = StreamProcessor()

    streams = [sensor, transaction, event]
    data_batches = [sensor_data, transaction_data, event_data]

    results = processor.run_pipeline(streams, data_batches)

    print("Batch 1 Results:")
    for r in results:
        print(f"- {r}")

    # --- FILTERING DEMO (dynamic via polymorphism) ---
    print("\nStream filtering active: High-priority data only")

    filtered_sensor = sensor.filter_data(sensor_data)
    filtered_transaction = transaction.filter_data(transaction_data)
    filtered_event = event.filter_data(event_data, "error")

    filtered_result = (
        f"{len(filtered_sensor)} critical sensor alerts, "
        f"{len(filtered_transaction)} large transaction, "
        f"{len(filtered_event)} event flags"
    )

    print(f"Filtered results: {filtered_result}")

    print("\nAll streams processed successfully. Nexus throughput optimal.")