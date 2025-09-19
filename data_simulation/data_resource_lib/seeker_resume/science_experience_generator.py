import json
import pprint
import random
from typing import Dict, Any

from data_simulation.data_resource_lib.generate_job_seeker_data import generate_job_seeker
from data_simulation.utils.ai_model_tools import ask_ai_for_response

USE_AI_GENERATOR = True

def use_ai_generator():
    global USE_AI_GENERATOR
    USE_AI_GENERATOR = True


def unuse_ai_generator():
    global USE_AI_GENERATOR
    USE_AI_GENERATOR = False


def generate_science_experience(job_seeker_info: Dict[str, Any], number: int = 1):
    if USE_AI_GENERATOR:
        result = ask_ai_for_response(f"请你帮我根据下面的求职者的信息，使用 json 的格式生成 {number} 条科研经历，"
                                     f"生成的科研经历需包含以下字段：'title'、'time'、'desc'，请勿加入其他字段，"
                                     f"科研经历中的desc的字数控制在100字左右，"
                                     f"科研经历按照时间顺序排列，时间格式为：2020-01-01。\n\n"
                                     f"{json.dumps(job_seeker_info, ensure_ascii=False)}")
    else:
        result = []
    return json.loads(result)


if __name__ == '__main__':
    jsi = generate_job_seeker(2)[0]
    we = generate_science_experience(jsi, random.randint(1, 5))
    pprint.pprint(we)
