import json
import os

import requests
from dotenv import load_dotenv
load_dotenv()

def ask_ai_for_response(prompt: str) -> str:
    payload = json.dumps({
        "messages": [
            {
                "content": "你是一个严格按照用户要求生成符合要求和规范的模拟数据的人工智能助手，"
                           "你使用中文生成数据内容，你必须生成合法的json格式字符串，"
                           "你的回复不应该使用markdown中的‘```’代码块符号包裹，"
                           "你不能回复除用户要求外的其他任何内容，你必须严格按照用户的要求生成规范、合法的模拟数据。",
                "role": "system"
            },
            {
                "content": prompt,
                "role": "user"
            }
        ],
        "model": "deepseek-chat",
        "frequency_penalty": 0,
        "max_tokens": 2048,
        "presence_penalty": 0,
        "response_format": {
            "type": "json_object"
        },
        "stop": None,
        "stream": False,
        "stream_options": None,
        "temperature": 1,
        "top_p": 1,
        "tools": None,
        "tool_choice": "none",
        "logprobs": False,
        "top_logprobs": None
    })
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': f'Bearer {os.getenv("OPENAI_API_KEY")}'
    }

    response = requests.request("POST", os.getenv("OPENAI_API_URL"), headers=headers, data=payload)
    return response.json()['choices'][0]['message']['content']


if __name__ == "__main__":
    print(ask_ai_for_response("Hello"))
