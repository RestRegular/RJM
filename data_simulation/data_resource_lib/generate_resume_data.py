import json
from typing import List, Dict, Any

from data_simulation.data_resource_lib.generate_job_seeker_data import generate_job_seeker
from data_simulation.data_resource_lib.seeker_resume.resume_photo_url_obtainer import import_photo_urls
from data_simulation.utils.ai_model_tools import ask_ai_for_response

USE_AI_GENERATOR = True


def get_resume_id_generator():
    _resume_id = 0
    def generate():
        nonlocal _resume_id
        _resume_id += 1
        return f"resume-{_resume_id}"

    return generate


generate_resume_id = get_resume_id_generator()


def use_ai_generator():
    global USE_AI_GENERATOR
    USE_AI_GENERATOR = True


def unuse_ai_generator():
    global USE_AI_GENERATOR
    USE_AI_GENERATOR = False


def generate_resume_title(expected_job: List[str]):
    return [f"{job}的简历" for job in expected_job]


def generate_resume_description(job_seeker_info: Dict[str, Any]) -> str:
    if USE_AI_GENERATOR:
        prompt = (f"请以第一人称视角生成一段基于下面求职者的 json 数据信息的简历描述，"
                  f"请使用中文，请使用json格式返回，不要使用markdown格式中的‘```’代码块符号进行包裹，简历描述内容字数控制在200字左右，不需要包含隐私信息。"
                  f"返回的json格式数据必须包含'resume_description'字段。\n\n"
                  f"{json.dumps(job_seeker_info, ensure_ascii=False)}")
        response = ask_ai_for_response(prompt)
        return json.loads(response)["resume_description"]
    else:
        resume_description = (f"{job_seeker_info['name']}，今年{job_seeker_info['age']}岁，"
                              f"性别{'男' if job_seeker_info['gender'] == 'male' else '女'}，"
                              f"位于{job_seeker_info['location']}，联系方式为手机号{job_seeker_info['phone']}，"
                              f"邮箱是{job_seeker_info['email']}，工作台{'已经' if job_seeker_info['workbench'] else '还未'}"
                              f"开通，期望工作城市为{'、'.join(job_seeker_info['idealCity'])}，"
                              f"期望职位为{'、'.join(job_seeker_info['idealJob'])}，期望行业为{'、'.join(job_seeker_info['idealIndustry'])}，"
                              f"期望薪资为{job_seeker_info['idealSalary']['range']['lowerLimit']}~{job_seeker_info['idealSalary']['range']['upperLimit']}{job_seeker_info['idealSalary']['period'][0]}/{job_seeker_info['idealSalary']['unit'][0]}。")
        return resume_description


def get_resume_photo_url_generator():
    _resume_photo_urls = import_photo_urls()
    def generate():
        url = _resume_photo_urls.pop(0)
        return url

    return generate


generate_resume_url = get_resume_photo_url_generator()


if __name__ == '__main__':
    use_ai_generator()
    # unuse_ai_generator()
    print(generate_resume_description(generate_job_seeker(1)[0]))
