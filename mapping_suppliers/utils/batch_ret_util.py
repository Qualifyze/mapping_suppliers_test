import uuid
import os
from openai import OpenAI
import json

from dotenv import load_dotenv
import pandas as pd
import datetime
import time


class BatchRetUtil:
    def __init__(self, batches_folder: str):
        load_dotenv()
        self.client = OpenAI()

        self.batch_ids = self.get_latest_batch_ids(batches_folder)
        self.batches = self.get_batch_details()
        
    
    def get_latest_batch_ids(self, batches_folder):
        batch_ids = []

        batch_files = os.listdir(batches_folder)
        # the folder names are date in the %Y_%m_%d_%H_%M_%S format
        batch_files.sort(reverse=True)
        latest_batch_folder = batch_files[0]
        latest_batch_path = os.path.join(batches_folder, latest_batch_folder)
        self.latest_batch_path = latest_batch_path

        with open(os.path.join(latest_batch_path, "inputs/batch_recap.json"), "r") as f:
            recap = json.load(f)
            for batch in recap["batches"]:
                batch_ids.append(batch["batch_id"])

        return batch_ids
    
    def get_batch_details(self):
        batches = []
        for batch_id in self.batch_ids:
            batch = self.client.batches.retrieve(batch_id)
            batches.append(batch)

        latest_batch = None
        # count the number of non completed batches
        non_completed_batches = 0
        for batch in batches:
            # print(f"Batch : {batch}")
            if batch.status != "completed":
                non_completed_batches += 1

            if batch.status == "completed":
                # print(f"Batch end time: {batch_end_time}")
                if latest_batch is None or (batch.completed_at > latest_batch.completed_at):
                    latest_batch = batch
        
        if non_completed_batches > 0:
            raise Exception(f"There are {non_completed_batches} / {len(batches)} non completed batches. Wait for them to be completed before running this script.")
        
        latest_batch_completed_at = datetime.datetime.fromtimestamp(latest_batch.completed_at, datetime.timezone.utc).astimezone()

        print(f"All ({len(batches)}) batches are completed with the latest completion time at {latest_batch_completed_at}")
        
        # print(f"Batch Details: {latest_batch}")

        return batches
    
    def get_contents(self):
        contents = []
        contents_as_json = []
        os.makedirs(os.path.join(self.latest_batch_path, "outputs"), exist_ok=True)

        corrupted_content_count = 0
        total_content_count = 0

        for batch in self.batches:
            if batch.status == "completed":
                response_file = self.client.files.content(batch.output_file_id)

                json_response =  pd.read_json(path_or_buf=response_file, lines=True)

                # delete the file if it exists
                if os.path.exists(os.path.join(self.latest_batch_path, f"outputs/{batch.id}.jsonl")):
                    os.remove(os.path.join(self.latest_batch_path, f"outputs/{batch.id}.jsonl"))
                with open(os.path.join(self.latest_batch_path, f"outputs/{batch.id}.jsonl"), 'a') as f:
                    f.write(json_response.to_json(orient="records", lines=True))

                for i, row in json_response.iterrows():
                    total_content_count += len(row['response']['body'])
                    for choice in row['response']['body']['choices']:
                        # print(f"Choice: {choice}")

                        # dump the choice to a json file


                        # gg = choice.replace(',{"item_1":"Kukje Pharmaceutical Ind Co Ltd Seongnam-City KR","is_item_1_supplier_site":true,"item_2":"', "}]")
                        # result = json.loads(gg)


                        contents.append({
                            'custom_id': row['response'],
                            'content': choice['message']['content']
                        })

        for item in contents:
            content = item['content']
            try:
                content_json = json.loads(content)
                contents_as_json.append(content_json)
            except json.JSONDecodeError as e:
                corrupted_content_count += 1
                try:

                    last_brace_index = content.rfind(',{')
                    contents_as_json.append(json.loads(content[:last_brace_index]+']}'))
                except json.JSONDecodeError as e:
                    print(f"Biig Parsing error: {e}")

        print(f"Corrupted JSON count: {corrupted_content_count} / {total_content_count}")
                        
            
        return contents_as_json
                    

