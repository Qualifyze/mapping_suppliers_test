import uuid
import os
from openai import OpenAI

from dotenv import load_dotenv


class BatchGenUtil:
    def __init__(self, main_folder, batch_size=1000, dry_run=False):
        load_dotenv()
        self.batch_size = batch_size
        self.main_folder = main_folder
        self.batches = []
        self.client = OpenAI()
        self.dry_run = dry_run

        self.create_batch_file()


    def create_batch_file(self):
        self.current_file_path = f"{self.main_folder}/{uuid.uuid4()}.jsonl"
        self.current_count = 0
        with open(self.current_file_path, 'a'):
            os.utime(self.current_file_path, None)

    def add_to_batch(self, data, increment=1):
        if self.current_count >= self.batch_size:
            self.call_batch_api()
            self.create_batch_file()

        with open(self.current_file_path, 'a') as f:
            f.write(data + "\n")

        self.current_count += increment

    def call_batch_api(self):
        if self.dry_run:
            return
            
        batch_input_file = self.client.files.create(
            file=open(self.current_file_path, "rb"),
            purpose="batch"
        )
        batch = self.client.batches.create(
            input_file_id=batch_input_file.id,
            endpoint="/v1/chat/completions",
            completion_window="24h"
        )
        self.batches.append({
            "batch_id": batch.id,
            "batch_input_file_id": batch_input_file.id,
            "batch_input_file_path": self.current_file_path
        })

    def conclude_session(self):
        if self.current_count > 0:
            self.call_batch_api()

        return self.batches

        