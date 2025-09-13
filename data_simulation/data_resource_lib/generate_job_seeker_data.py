import random
import string
from pinyin.pinyin import get as get_pinyin  # 导入pinyin库
from data_simulation.data_resource_lib.job_seeker.extract_name_samples import extract_name_samples
from data_simulation.data_resource_lib.job_seeker.job_info_obtainer import obtain_job_info

JOB_INFOS = obtain_job_info()


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


def generate_expected_job():
    """生成随机的期望职位"""
    pass


def generate_job_seeker(seeker_count: int = 1000) -> dict:
    """生成指定数量的求职者数据"""
    # 确保总样本数为偶数，保证男女比例1:1
    if seeker_count % 2 != 0:
        seeker_count += 1

    # 提取姓名样本
    seeker_names = extract_name_samples("./resource/job_seeker_names.json", seeker_count)

    job_seekers = {
        "male": [],
        "female": []
    }

    # 生成男性求职者数据
    for first_name, genders in seeker_names.items():
        for name in genders["male"]:
            job_seekers["male"].append({
                "name": name,
                "age": generate_age(),
                "phone": generate_phone_number(),
                "email": generate_email(name)
            })

    # 生成女性求职者数据
    for first_name, genders in seeker_names.items():
        for name in genders["female"]:
            job_seekers["female"].append({
                "name": name,
                "age": generate_age(),
                "phone": generate_phone_number(),
                "email": generate_email(name)
            })

    return job_seekers


if __name__ == '__main__':
    print(generate_job_seeker(100))
