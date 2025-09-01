import os
import csv

class ReadFile:

    def __init__(self, path):
        self.path = path


    def read_txt_file(self):
        if self.path.split('.')[-1] != "txt":
            raise FileExistsError("that file not txt")
        if not os.path.exists(self.path):
            raise FileNotFoundError(f"file not found: {self.path}")

        with open(self.path, "r") as file:
            return file.read()

    def read_csv_file(self):
        with open(self.path, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            return reader
