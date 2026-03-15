#!/usr/bin/env python3
from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    @abstractmethod
    def process(self, data: Any) -> str:
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    def format_output(self, result: Any) -> str:
        return str(result)


class NumericProcessor(DataProcessor):
    def __init__(self):
        print("Initializing Numeric Processor...")

    def validate(self, data: Any) -> bool:
        print(f"Processing data: {data}")

        if not isinstance(data, (list, int, float)):
            print("Error: data must be a number or list of numbers")
            return False

        if isinstance(data, (int, float)):
            data = [data]

        if isinstance(data, list) and not all(isinstance(i,(int, float)) for i in data):
            return False
        print("Validation: Numeric data verified")
        return True

    def process(self, data: Any) -> str:

        if self.validate(data):
            length = len(data)
            total = 0
            for i in data:
                total = total + i
            return (f"Processed {length} numeric values,"
                    f"sum={total}, avg={total / length}")
        else:
            raise ValueError("Error: all items must be numbers (int or float)")

    def format_output(self, result: str) -> str:
        return f"Output: {result}"


class TextProcessor(DataProcessor):
    def __init__(self) -> None:
        print("Initializing Text Processor...")

    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            print("Validation: Text data verified")
            return True
        else:
            return False

    def process(self, data: Any) -> str:
        print(f'Processing data: "{data}"')
        if self.validate(data):
            length_data = len(data)
            cont_words = len(data.split())
            return (f"Output: Processed text: {length_data} characters,"
                    f"{cont_words} words")

        else:
            raise ValueError("Error: Invalid input must be (str)")

    def format_output(self, result: str) -> str:
        return f"Output: {result}"


class LogProcessor(DataProcessor):
    def __init__(self) -> None:
        print("Initializing Log Processor...")

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
        print(f'Processing data: "{data}"')
        if self.validate(data):
            colon_index = data.find(":")
            if colon_index == -1:
                raise Exception("Data format invalid: no colon found")
            level = data[:colon_index].strip()
            message = data[colon_index + 1:].strip()

            if level == "ERROR":
                print("Validation: Log entry verified")
                return f"[ALERT] {level} level detected: {message}"
            elif level == "INFO":
                print("Validation: Log entry verified")
                return f"[INFO] {level} level detected: {message}"
            elif level == "DEBUG":
                print("Validation: Log entry verified")
                return f"[DEBUG] {level} level detected: {message}"
            else:
                raise Exception(f"Unknown log level: {level}")

    def format_output(self, result: str) -> str:
        return f"Output: {result}"


if __name__ == "__main__":
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n")
    data = [1, 4, 3, 4, 4]

    NumericProcessor_pro = NumericProcessor()
    try:
        a = NumericProcessor_pro.process([1, 2, 3, 4, 5])
        print(NumericProcessor_pro.format_output(a))
        print("")
    except Exception as e:
        print(f"{e}")

    TextProcessor_pro = TextProcessor()
    try:
        b = TextProcessor_pro.process("Hello Nexus World")
        print(TextProcessor_pro.format_output(b))
        print("")
    except Exception as e:
        print(e)
    LogProcessor_pro = LogProcessor()
    try:
        c = LogProcessor_pro.process("ERROR: Connection timeou")
        print(f"{LogProcessor_pro.format_output(c)}")
        print("")
    except Exception as e:
        print(f"{e}")
