#!/usr/bin/env python3
from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional


class DataProcessor(ABC):
    @abstractmethod
    def process(self, data: Any) -> str:
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    def format_output(self, result: str) -> str:
        return str(result)


class NumericProcessor(DataProcessor):
    def __init__(self):
        pass

    def validate(self, data: Any) -> bool:

        if not isinstance(data, (list, int, float)):
            print("Error: data must be a number or list of numbers")
            return False

        if isinstance(data, (int, float)):
            data = [data]

        if isinstance(data, list) and not all(isinstance
                                              (i, (int, float)) for i in data):
            return False
        return True

    def process(self, data: Any) -> str:
        if self.validate(data):
            number: Optional[Union[int, float]] = None

            if isinstance(data, (int, float)):
                number = data
                data = [number]

            length = len(data)
            total = 0
            for i in data:
                total = total + i

            return (f"Processed {length} numeric values,"
                    f" sum={total}, avg={total / length}")
        else:
            raise ValueError("Error: all items must be numbers (int or float)")

    def format_output(self, result: str) -> str:
        print("Validation: Numeric data verified")
        return f"Output: {result}"


class TextProcessor(DataProcessor):
    def __init__(self) -> None:
        pass

    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True
        else:
            return False

    def process(self, data: Any) -> str:
        if self.validate(data):
            length_data = len(data)
            cont_words = len(data.split())
            return (f"Processed text: {length_data} characters,"
                    f" {cont_words} words")

        else:
            raise ValueError("Error: Invalid input must be (str)")

    def format_output(self, result: str) -> str:
        print("Validation: Text data verified")
        return f"Output: {result}"


class LogProcessor(DataProcessor):
    def __init__(self) -> None:
        pass

    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            if data[0:6] == "ERROR:":
                return True
            elif data[0:5] == "INFO:":
                return True
            elif data[0:6] == "DEBUG:":
                return True
        return False

    def process(self, data: Any) -> str:
        if self.validate(data):
            colon_index = data.find(":")
            if colon_index == -1:
                raise Exception("Data format invalid: no colon found")
            level = data[:colon_index].strip()
            message = data[colon_index + 1:].strip()
            return f"{level}:{message}"
        else:
            raise Exception(f"Unknown log level: {data}")

    def format_output(self, result: str) -> str:
        colon_index = result.find(":")
        level = result[:colon_index]
        message = result[colon_index + 1:].strip()

        prefix_map: Dict[str, str] = {
            "ERROR": "[ALERT]",
            "INFO": "[INFO]",
            "DEBUG": "[DEBUG]"
            }

        prefix = prefix_map.get(level, "[UNKNOWN]")
        print("Validation: Log entry verified")

        return f"Output: {prefix} {level} level detected: {message}"


if __name__ == "__main__":
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n")
    processors: List[DataProcessor] = [
        NumericProcessor(),
        TextProcessor(),
        LogProcessor()
        ]
    data_list = [[1, 2, 3, 4, 5], "Hello Nexus World",
                 "ERROR: Connection timeout"]
    for processor, data in zip(processors, data_list):
        try:
            if isinstance(processor, NumericProcessor):
                print("Initializing Numeric Processor...")
                print(f"Processing data: {data}")

            elif isinstance(processor, LogProcessor):
                print("Initializing Log Processor...")
                print(f"Processing data: {data}")

            elif isinstance(processor, TextProcessor):
                print("Initializing Text Processor...")
                print(f"Processing data: {data}")

            result = processor.process(data)
            output = processor.format_output(result)
            print(output)
            print()
        except Exception as e:
            print(f"Error: {e}")
    print("=== Polymorphic Processing Demo ===")
    list_data = [[1, 2, 3], "Hello World", "INFO: System ready"]
    print("\nProcessing multiple data types through same interface...")

    for i, (processor, data) in enumerate(zip(processors, list_data), start=1):
        try:
            result = processor.process(data)
            print(f"Result {i}: {result.replace('Output: ', '')}")
        except Exception as e:
            print(f"Error: {e}")

    print("\nFoundation systems online. Nexus ready for advanced streams")
