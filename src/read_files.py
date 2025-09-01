import os
import csv

class ReadFile:

    @staticmethod
    def read_txt_file(path):
        if path.split('.')[-1] != "txt":
            raise FileExistsError("that file not txt")
        if not os.path.exists(path):
            raise FileNotFoundError(f"file not found: {path}")

        with open(path, "r") as file:
            return file.read()

    @staticmethod
    def read_csv_file(path):
        data = []
        with open(path, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                data.append(row)
        return data