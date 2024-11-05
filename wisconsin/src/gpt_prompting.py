from openai import OpenAI
from dotenv import load_dotenv
import time
import aiolimiter
import os
# from pydantic import BaseModel
from tooldantic import OpenAiResponseFormatBaseModel as BaseModel
import pandas as pd
import base64
import json
load_dotenv()




class GPTApiHelper:
    def __init__(self, api_key=None, rate_per_second=None):
        self.start_time = time.time()
        self.model = "gpt-4o-mini-2024-07-18"
        if api_key is None:
            api_key = os.getenv("OPENAI_API_KEY")
        self.client = OpenAI(api_key=api_key)
        self.total_completion_tokens = 0
        self.total_prompt_tokens = 0
        self.total_error_count = 0
        self.max_errors = 10
        self.is_ratelimited = False
        if rate_per_second is not None:
            self.is_ratelimited = True
            self.rate_per_second = rate_per_second
            self.ratelimit = aiolimiter.AsyncLimiter(rate_per_second, 1)
            print(f"Rate limited with {rate_per_second} requests per second")
 

    def make_request_structured(self, prompts, structured_format="pydantic",schema={
                    "name": "email_schema",
                    "schema": {
                        "type": "object",
                        "properties": {
                            "email": {
                                "description": "The email address that appears in the input",
                                "type": "string"
                            },
                            "additionalProperties": False
                        } } }):
        """make a request to the API
        
        Args:
            prompts (list): list of prompts as dict with role and content e.g. [{"role": "system", "content": "system prompt"}]
            json_schema (dict): json schema"""
        messages = []
        for prompt in prompts:
            messages.append({"role": prompt["role"], "content": prompt["content"]})
        if structured_format =="json":
            schema = {"type": "json_schema", "json_schema": schema}
        response = self.client.beta.chat.completions.parse(
            model=self.model,
            temperature=0.0,
            messages=messages,
            response_format=schema
        )
        self.total_prompt_tokens += response.usage.prompt_tokens
        self.total_completion_tokens += response.usage.completion_tokens
        return response


    def estimate_cost(self,response=None, completion_token_cost_1m=0.6, prompt_token_cost_1m=0.15,is_batch=False):
        """estimate the cost of the response or the cost of the total tokens"""
        completion_token_cost = completion_token_cost_1m / 1e6
        prompt_token_cost = prompt_token_cost_1m / 1e6
        if response is None:
            return round(self.total_completion_tokens * completion_token_cost + self.total_prompt_tokens * prompt_token_cost, 5)
        if is_batch:
            completion_tokens = 0
            prompt_tokens = 0
            for r in response:
                completion_tokens += r["response"]["body"]["usage"]["completion_tokens"]
                prompt_tokens += r ["response"]["body"]["usage"]["prompt_tokens"]
            completion_token_cost = completion_token_cost / 2
            prompt_token_cost = prompt_token_cost / 2
        else:
            completion_tokens = response.usage.completion_tokens
            prompt_tokens = response.usage.prompt_tokens
        return round(completion_tokens * completion_token_cost + prompt_tokens * prompt_token_cost,5)


    def create_jsonl(self, prompts, schema, custom_id_prefix="request_"):
        """create a jsonl file from list of prompts (which are a lists of dicts with role and content messages). Schema needs to be of type BaseModel"""
        jsonl = []
        for i,prompt in enumerate(prompts):
            body = {"model": self.model, "messages": prompt, "temperature": 0.0, "response_format": schema.schema()} 
            json = {"custom_id": f"{custom_id_prefix}{i}", "method": "POST", "url": "/v1/chat/completions", "body": body}
            jsonl.append(json)
        return jsonl

 
    def upload_jsonl(self, filename):
        """upload jsonl to the API"""
        batch_input_file = self.client.files.create(
        file=open(filename, "rb"),
            purpose="batch"
                )
        return batch_input_file
    


    def create_batch(self, batch_id, description):
        """make a batch request to the API"""
        batch_res  = self.client.batches.create(
        input_file_id=batch_id,
        endpoint="/v1/chat/completions",
        completion_window="24h",
        metadata={
      "description": description,
        }
    )
        return batch_res 


    def check_batch_status(self, batch_id):
        """check the status of the batch"""
        return self.client.batches.retrieve(batch_id)
        

    def get_file_content(self, file_id):
        """get the content of the file"""
        return self.client.files.content(file_id)    
    

    def convert_response_list(self, res_list):
        """convert the response to a jsonl format"""
        res_df = pd.DataFrame(columns = ["is_crime_news", "perpetrator_name", "victim_name", "state"])
        for res in res_list:
            res_df = pd.concat([res_df,pd.DataFrame(json.loads(res["response"]["body"]["choices"][0]["message"]["content"]), index=[0])], ignore_index=True)
        res_df["perpetrator_name"] = res_df["perpetrator_name"].apply(lambda x: x if x != "" else None)
        res_df["victim_name"] = res_df["victim_name"].apply(lambda x: x if x != "" else None)
        res_df["state"] = res_df["state"].apply(lambda x: x if x != "" else None)
        return res_df


