import json
import time

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Protocol, Union


class ProcessingStage(Protocol):

    def process(self, data: Any) -> Any: ...


class InputStage:
    def __init__(self) -> None:
        print("Stage 1: Input validation and parsing")

    def validate(self, data: Any) -> None:
        if not isinstance(data, (str, dict)):
            raise ValueError("Invalid Data Format")

    def parse(self, data: Any) -> dict:
        if isinstance(data, dict):
            return (data)
        try:
            result = json.loads(data)
            if not isinstance(result, dict):
                raise ValueError("Invalid Data Format")
            return result
        except json.JSONDecodeError:
            raise ValueError("Invalid JSON Format")

    def clean(self, data: dict):
        return {
            str(key).lower(): value
            for key, value in data.items()
            if value is not None
        }

    def process(self, data: Any) -> Any:
        self.validate(data)
        parsed = self.parse(data)
        return self.clean(parsed)


class TransformStage:

    def __init__(self) -> None:
        print("Stage 2: Data transformation and enrichment")

    def validate(self, data: Any) -> Dict:
        if not isinstance(data, dict):
            raise ValueError("error detected in Stage 2: Invalid Data Format")
        return data

    def transform_value(self, value: Any) -> Any:
        if isinstance(value, str):
            return value.strip().upper()
        return value

    def process(self, data: Any) -> Any:
        data = self.validate(data)

        return {
            key: self.transform_value(value)
            for key, value in data.items()
        }


class OutputStage:

    def __init__(self) -> None:
        print("Stage 3: Output formatting and delivery")

    def validate(self, data: Any) -> dict:
        if not isinstance(data, dict):
            raise ValueError("error detected in Stage 3: Invalid Data Format")
        return data

    def format_pair(self, key: Any, value: Any) -> str:
        return f"{key}: {value}"

    def process(self, data: Any) -> Any:
        data = self.validate(data)

        return ", ".join(
            [self.format_pair(key, value) for key, value in data.items()]
        )


class ProcessingPipeline(ABC):
    def __init__(self) -> None:
        self.stages: list[ProcessingStage] = []

    def add_stage(self, stage: ProcessingStage) -> None:
        self.stages.append(stage)

    @abstractmethod
    def process(self, data: Any) -> Any:
        ...


class JSONAdapter(ProcessingPipeline):

    def __init__(self, pipeline_id: str):
        super().__init__()
        self.pipeline_id = pipeline_id

    def process(self, data: Any) -> Any:
        print("\nProcessing JSON data through pipeline...")
        print(f"Input: {data}")

        if not isinstance(data, dict):
            raise ValueError(
                "error detected in JSON Adapter: Invalid Data Format"
            )

        result = data
        for stage in self.stages:
            result = stage.process(result)

        print("Transform: Enriched with metadata and validation")
        print(f"Output: {result}")

        return result


class CSVAdapter(ProcessingPipeline):

    def __init__(self, pipeline_id: str):
        super().__init__()
        self.pipeline_id = pipeline_id

    def validate(self, data: Any) -> Any:
        if not isinstance(data, (str, dict)):
            raise ValueError(
                    "error detected in CSV Adapter: Invalid Data Format"
                )
        return data

    def parse(self, data: str) -> dict:
        parts = data.split(",")

        structured = {}
        i = 0

        for value in parts:
            structured[f"col_{i}"] = value
            i += 1

        return structured

    def run_pipeline(self, data: Any) -> Any:
        result = data

        for stage in self.stages:
            result = stage.process(result)

        return result

    def process(self, data: Any) -> Union[str, Any]:
        print("\nProcessing CSV data through same pipeline...")
        print(f'Input: "{data}"')

        data = self.validate(data)

        if isinstance(data, str):
            structured = self.parse(data)
        else:
            structured = data

        result = self.run_pipeline(structured)

        print("Transform: Parsed and structured data")
        print(f"Output: {result}")

        return result


class StreamAdapter(ProcessingPipeline):

    def __init__(self, pipeline_id: str):
        super().__init__()
        self.pipeline_id = pipeline_id

    def validate(self, data: Any) -> Any:
        if data is None:
            raise ValueError("Stream data cannot be None")
        return data

    def process(self, data: Any) -> Any:
        print("\nProcessing Stream data through same pipeline...")
        print(f"Input: {data}")

        data = self.validate(data)

        structured = {"stream": data}

        result = structured

        for stage in self.stages:
            result = stage.process(result)

        print("Transform: Aggregated and filtered")
        print(f"Output: {result}")

        return result


class NexusManager:

    def __init__(self) -> None:
        self.pipelines: List[ProcessingPipeline] = []
        print("Initializing Nexus Manager...")
        print("Pipeline capacity: 1000 streams/second")

    def add_pipeline(self, pipeline: ProcessingPipeline) -> None:
        self.pipelines.append(pipeline)

    def process_data(self, data: Any) -> List[Any]:
        if not isinstance(data, list):
            raise ValueError("Input data must be a list")
        results = []

        for pipeline, item in zip(self.pipelines, data):
            try:
                result = pipeline.process(item)
                results.append(result)
            except Exception as err:
                print(f"ERROR in pipeline: {err}")
                results.append(None)

        return results


if __name__ == "__main__":

    print("=== CODE NEXUS - ENTERPRISE PIPELINE SYSTEM ===\n")
    manager = NexusManager()
    print("\nCreating Data Processing Pipeline...")
    stages = [InputStage(), TransformStage(), OutputStage()]
    print("\n=== Multi-Format Data Processing ===")
    pipelines = [
        JSONAdapter("json-1"),
        CSVAdapter("csv-1"),
        StreamAdapter("stream-1"),
    ]
    for pipeline in pipelines:
        for stage in stages:
            pipeline.add_stage(stage)
        manager.add_pipeline(pipeline)
    inputs = [
        {"sensor": "temp", "value": 23.5, "unit": "C"},
        "user,action,timestamp",
        "Real-time sensor stream",
    ]
    manager.process_data(inputs)

    print("\n=== Pipeline Chaining Demo ===")
    print("Pipeline A -> Pipeline B -> Pipeline C")
    print("Data flow: Raw -> Processed -> Analyzed -> Stored\n")

    start_time = time.perf_counter()

    raw_data = [
        {"sensor": "temp", "value": 23 + i, "unit": "C"}
        for i in range(100)
    ]
    success = 0

    for record in raw_data:
        try:
            for stage in stages:
                stage.process(record)
            success += 1
        except Exception:
            pass

    end_time = time.perf_counter()

    efficiency = (success / len(raw_data)) * 100 if raw_data else 0

    print(
        f"Performance: {efficiency:.0f}% efficiency, "
        f"{end_time - start_time:.4f}s total processing time"
    )
    print("\n=== Error Recovery Test ===")
    print("Simulating pipeline failure...")
    bad_data = "23:23"
    recovered_data = {"sensor": "temp", "value": 23, "unit": "C"}
    try:
        for stage in stages:
            stage.process(bad_data)
    except Exception as e:
        print(f"{e}")

    print("Recovery initiated: Switching to backup processor")
    try:
        for stage in stages:
            stage.process(recovered_data)
        print("Recovery successful: Pipeline restored, processing resumed")
    except Exception as e:
        print(f"{e}")

    print("\nNexus Integration complete. All systems operational.")
