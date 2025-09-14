import json
import random
import string
from typing import Optional, List, Dict, Any

from pinyin.pinyin import get as get_pinyin  # 导入pinyin库

from data_simulation.data_resource_lib.job_seeker.city_list_obtainer import import_city_list
from data_simulation.data_resource_lib.job_seeker.extract_name_samples import extract_name_samples
from data_simulation.data_resource_lib.job_seeker.job_info_obtainer import import_job_info
from data_simulation.data_resource_lib.job_seeker.signature_obtainer import import_signatures


def import_city_rank_info() -> Dict[str, List[str]]:
    with open("./resource/city_rank.json", "r", encoding="utf-8") as f:
        return json.load(f)


JOB_INFOS = import_job_info()
CITY_INFOS = import_city_list()
SIGNATURES = import_signatures()
CITY_RANK_INFOS = import_city_rank_info()


def generate_phone_number():
    """生成符合规则的手机号，前三位为三大运营商号段"""
    # 三大运营商主要号段
    china_mobile = ['134', '135', '136', '137', '138', '139', '150', '151', '152', '157', '158', '159', '182', '183', '184', '187', '188', '178']
    china_unicom = ['130', '131', '132', '155', '156', '185', '186', '176']
    china_telecom = ['133', '153', '180', '181', '189', '177']

    # 随机选择运营商号段
    prefix = random.choice(china_mobile + china_unicom + china_telecom)
    # 生成后8位数字
    suffix = ''.join(random.choices('0123456789', k=8))
    return f"{prefix}{suffix}"

def generate_email(full_name):
    """根据姓名使用pinyin库生成邮箱地址"""
    # 常见邮箱域名
    domains = ['@qq.com', '@163.com', '@126.com', '@gmail.com', '@outlook.com', '@sina.com']

    # 使用pinyin库将中文姓名转换为拼音列表
    # 例如: "张三" -> [['zhang'], ['san']]
    name_pinyin_list = get_pinyin(full_name, delimiter='_', format='strip')  # 确保全小写

    # 转换为连续字符串，如"zhangsan"
    full_pinyin = ''.join([item[0] for item in name_pinyin_list])

    # 随机添加数字或小写字母增加唯一性
    random_suffix = ''.join(random.choices(string.digits + string.ascii_lowercase, k=random.randint(0, 5)))

    # 随机选择域名
    domain = random.choice(domains)

    return f"{full_pinyin}{random_suffix}{domain}"

def generate_age():
    """按照指定分布生成年龄：22-30岁(60%)，31-40岁(30%)，其他(10%)"""
    # 使用随机数确定年龄区间
    rand = random.random()

    if rand < 0.6:
        # 22-30岁
        return random.randint(22, 30)
    elif rand < 0.9:
        # 31-40岁
        return random.randint(31, 40)
    else:
        # 其他年龄：18-21岁或41-60岁
        if random.random() < 0.5:
            return random.randint(18, 21)
        else:
            return random.randint(41, 60)


def generate_job_type():
    """生成随机的工作类型"""
    job_types = ["全职", "兼职", "实习"]
    return random.choice(job_types)


def generate_expected_industry() -> List[str]:
    """
    生成随机的期望行业列表
    """
    return random.sample(list(JOB_INFOS.keys()), random.randint(1, 3))


def generate_expected_job(expected_industry: List[str]) -> Optional[Dict[str, List[str]]]:
    """
    生成随机的期望职位列表

    根据每个行业随机选择1到多个具体职位返回

    返回:
        随机选择的职位列表，如果职位信息为空则返回None
    """
    if not JOB_INFOS:  # 检查职位信息是否为空
        return None

    # 存储最终选择的所有职位
    all_chosen_positions: Dict[str, List[str]] = {}

    # 为每个选中的类别选择职位
    for category in expected_industry:
        positions_in_category = [
            job["job"]
            for cls, jobs in JOB_INFOS[category].items()
            for job in jobs
        ]
        # 每个类别随机选择 1-3 个职位
        num_positions = random.randint(1, 3)
        chosen_positions = random.sample(positions_in_category, num_positions)
        all_chosen_positions[category] = chosen_positions

    return all_chosen_positions


def generate_expected_salary(expected_cities: List[str]):
    """
    生成随机的期望薪资，上下限为10/100/1000/10000等整倍数
    薪资按城市等级（一线 / 新一线 / 二线）设置不同梯度
    """
    # 定义城市等级对应的薪资系数（一线城市薪资最高，依次递减）
    city_level_factors = {
        "一线": 2,        # 一线城市薪资系数
        "新一线": 1.6,    # 新一线城市薪资系数
        "二线": 1.3,      # 二线城市薪资系数
        "三线": 1.0,      # 三线城市薪资系数
        "四线": 0.8,      # 四线城市薪资系数
        "五线": 0.6,      # 五线城市薪资系数
        "unknown": 1.0,  # 默认城市等级的薪资系数
    }

    hit_city_level_factors = [
        city_level_factors[city_level]
        for exp_city in expected_cities
        for city_level, cities in CITY_RANK_INFOS.items()
        if exp_city.strip("市区") in cities
           or exp_city.strip("自治区") in cities
           or exp_city.strip("特别行政区") in cities
    ]

    # 获取该城市等级对应的薪资系数
    salary_factor = max(hit_city_level_factors, default=city_level_factors["unknown"])

    # 定义可能的薪资周期
    periods = ["day", "hour", "week", "month", "year"]

    # 根据周期设置不同的薪资范围基数（会根据城市等级进行调整）
    period_configs = {
        "hour": {
            "base_range": (5, 10),    # 基础范围（后续会乘以10）
            "multiplier": 10          # 时薪通常是10的倍数
        },
        "day": {
            "base_range": (1, 10),    # 基础范围（后续会乘以100）
            "multiplier": 100         # 日薪通常是100的倍数
        },
        "week": {
            "base_range": (7, 70),  # 基础范围（后续会乘以100）
            "multiplier": 100         # 周薪通常是100的倍数
        },
        "month": {
            "base_range": (30, 500),  # 基础范围（后续会乘以100）
            "multiplier": 100         # 月薪通常是100的倍数
        },
        "year": {
            "base_range": (50, 1000), # 基础范围（后续会乘以1000）
            "multiplier": 1000        # 年薪通常是1000的倍数
        }
    }

    # 随机选择一个薪资周期
    period = random.choice(periods)

    # 获取对应周期的配置
    config = period_configs[period]
    base_lower, base_upper = config["base_range"]
    multiplier = config["multiplier"]

    # 根据城市等级调整薪资范围
    adjusted_lower = int(base_lower * salary_factor)
    adjusted_upper = int(base_upper * salary_factor)

    # 生成随机基础下限（在调整后的范围内）
    base_lower_limit = random.randint(adjusted_lower, adjusted_upper)
    # 生成随机基础上限（确保上限大于等于下限）
    base_upper_limit = random.randint(base_lower_limit, adjusted_upper)

    # 计算实际薪资范围（乘以对应倍数）
    lower_limit = base_lower_limit * multiplier
    upper_limit = base_upper_limit * multiplier

    # 构建并返回期望薪资字典
    return {
        "range": {
            "upperLimit": upper_limit,
            "lowerLimit": lower_limit
        },
        "unit": "rmb",
        "period": period
    }


def generate_expected_city() -> List[str]:
    """
    生成期望城市数据，随机选取一个省份/直辖市，再在该省份中随机选取1-10个城市/区
    """
    # 随机选择一个省份/直辖市
    province: Dict[str, Any] = random.choice(CITY_INFOS)
    all_cities = []
    # 遍历该省份下的所有子区域（市辖区、县等）
    if province["name"].endswith("市") or province["name"].endswith("自治区") or province["name"].endswith("特别行政区"):
        all_cities.append(province["name"])
    else:
        for pchild in province["pchilds"]:
            # 将子区域下的城市/区名称加入总列表
            if pchild["name"] == "市辖区" or pchild["name"] == "县" or pchild["name"] == "省直辖县级行政区划":
                for city in pchild["cchilds"]:
                    all_cities.append(city["name"])
            else:
                all_cities.append(pchild["name"])
    # 如果没有城市可选，返回空列表
    if not all_cities:
        return []
    # 随机选取1到10个城市（如果城市总数小于10，则选取全部）
    num_cities = random.randint(1, min(10, len(all_cities)))
    selected_cities = random.sample(all_cities, num_cities)
    return selected_cities


def generate_signature():
    """生成签名"""
    return random.choice(SIGNATURES)


def generate_priority():
    """
    生成用户权限

    ## 用户层（0）
    1. **普通用户**："01"
    2. **VIP用户**："02"

    ## 业务层（1）
    1. **部门负责人**："11"
    2. **数据审核员**："12"

    ## 运维层（2）
    1. **基础管理员**："21"
    2. **安全管理员**："22"

    ## 技术层（3）
    1. **前端开发**："31"
    2. **后端开发**："32"
    3. **DBA**："33"

    ## 决策层（4）
    1. **超级管理员**："41"
    """
    r = random.randint(1, 2)
    return "01" if r == 1 else "02"


def generate_workbench() -> bool:
    return random.choice([True, False])


def generate_homepage_hot(
        mean: int = 100,    # 平均值，大多数热门值集中在这个附近
        std_dev: int = 1000, # 标准差，控制数据分散程度
        min_hot: int = 0,     # 最小热门值，不能为负
        max_hot: int = 999999 # 最大热门值，保持原有上限
) -> int:
    """
    生成符合正态分布的首页热门值

    参数:
        mean: 正态分布的均值
        std_dev: 正态分布的标准差
        min_hot: 最小热门值
        max_hot: 最大热门值

    返回:
        符合正态分布的整数热门值
    """
    while True:
        # 生成符合正态分布的随机数
        hot_value = random.gauss(mean, std_dev)

        # 确保热门值在有效范围内，并转换为整数
        if min_hot <= hot_value <= max_hot:
            return int(round(hot_value))


def generate_homepage_type() -> List[str]:
    return []


def generate_homepage_activity() -> int:
    return random.randint(0, 99)


def generate_balance(
        mean: float = 1000.0,  # 平均余额
        std_dev: float = 1000.0,  # 标准差
        min_balance: float = -5000.0,  # 最小可能余额（最大负债）
        max_balance: float = 20000.0  # 最大可能余额
) -> float:
    """
    生成符合正态分布的随机账户余额

    参数:
        mean: 正态分布的均值
        std_dev: 正态分布的标准差
        min_balance: 允许的最小余额（最大负债）
        max_balance: 允许的最大余额

    返回:
        四舍五入到分的账户余额
    """
    while True:
        # 生成符合正态分布的随机数
        balance = random.gauss(mean, std_dev)

        # 确保余额在合理范围内
        if min_balance <= balance <= max_balance:
            # 四舍五入到小数点后两位（表示分）
            return round(balance, 2)


def generate_job_seeker(seeker_count: int = 1000) -> List[Dict[str, Any]]:
    """生成指定数量的求职者数据"""
    # 确保总样本数为偶数，保证男女比例1:1
    if seeker_count % 2 != 0:
        seeker_count += 1

    # 提取姓名样本
    seeker_names = extract_name_samples("./resource/job_seeker_names.json", seeker_count)

    job_seekers = []

    # 生成男性求职者数据
    for first_name, genders in seeker_names.items():
        for gender, names in genders.items():
            for name in names:
                exp_industry = generate_expected_industry()
                exp_cities = generate_expected_city()
                job_seekers.append({
                    "name": name,
                    "age": generate_age(),
                    "gender": gender,
                    "phone": generate_phone_number(),
                    "email": generate_email(name),
                    "idealJobType": generate_job_type(),
                    "idealIndustry": exp_industry,
                    "idealJob": generate_expected_job(exp_industry),
                    "idealSalary": generate_expected_salary(exp_cities),
                    "idealCity": exp_cities,
                    "signature": generate_signature(),
                    "priority": generate_priority(),
                    "homepageHot": generate_homepage_hot(),
                    "homepageType": generate_homepage_type(),
                    "homepageActivity": generate_homepage_activity(),
                    "balance": generate_balance(),
                    "workbench": generate_workbench()
                })

    return job_seekers


if __name__ == '__main__':
    print(generate_job_seeker(100))
