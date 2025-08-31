from pprint import pprint

import requests

from test import *

result = requests.post(f"{BASE_URL}/calculating/match_degree", json={
    'resume_id': 3,
    'job_id': 1,
    'config_dict': {
        'job_title_match': 0.1,        # 维持职位名称基础权重
        'skill_match': 0.08,           # 进一步提升技能匹配权重，突出核心能力
        'work_experience_match': 0.2,  # 保持工作经验权重，强调实践积累
        'location_match': 0.05,        # 降低地点权重，适应远程办公趋势
        'salary_match': 0.05,          # 降低薪资权重，减少初期筛选影响
        'education_match': 0.35,       # 适当学历权重，更注重实际能力
        'project_experience_match': 0.15, # 提高项目经验权重，强调实战成果
        'other_factors': 0.02          # 维持其他因素基础权重
    }
})

pprint(result.json())