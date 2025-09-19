from typing import Dict, Any, List, Optional, Tuple
import random
from datetime import datetime
import pandas as pd
import pymysql
from pymysql.cursors import DictCursor

from data_simulation.data_resource_lib.job_seeker.universities_obtainer import import_universities

# 数据库配置
DB_CONFIG = {
    "dbname": "data_flow",
    "user": "root",
    "password": "197346285",
    "host": "localhost",  # 假设数据库在本地
    "port": 3306,         # 默认MySQL端口
    "charset": "utf8"
}

# 学校类型映射
SCHOOL_TYPE_MAP = {
    1: "公立小学",
    2: "公立中学",
    3: "公立九年一贯制",
    8: "公立十二年一贯制",
    4: "私立小学",
    5: "私立中学",
    6: "私立九年一贯制",
    9: "私立十二年一贯制",
    7: "培训机构"
}

# 学校类型分组
SCHOOL_TYPE_GROUPS = {
    'primary': [1, 4],         # 小学(公立+私立)
    'middle': [2, 5],          # 中学(公立+私立)
    'nine_year': [3, 6],       # 九年一贯制
    'twelve_year': [8, 9]      # 十二年一贯制
}

# 全局变量存储学校数据
SCHOOLS_CACHE = {
    'primary': pd.DataFrame(),
    'middle': pd.DataFrame(),
    'nine_year': pd.DataFrame(),
    'twelve_year': pd.DataFrame()
}

UNIVERSITIES = import_universities()


def get_db_connection():
    """获取数据库连接"""
    try:
        connection = pymysql.connect(
            host=DB_CONFIG["host"],
            port=DB_CONFIG["port"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            db=DB_CONFIG["dbname"],
            charset=DB_CONFIG["charset"],
            cursorclass=DictCursor
        )
        return connection
    except Exception as e:
        print(f"数据库连接失败: {e}")
        return None


def load_schools_by_type(school_group: str, city_id: int = None) -> pd.DataFrame:
    """
    按学校类型分组加载学校信息，利用索引提高查询效率

    Args:
        school_group: 学校分组(primary/middle/nine_year/twelve_year)
        city_id: 城市ID，用于筛选特定城市的学校

    Returns:
        筛选后的学校数据DataFrame
    """
    # 检查缓存
    if not SCHOOLS_CACHE[school_group].empty and not city_id:
        return SCHOOLS_CACHE[school_group]

    connection = get_db_connection()
    if not connection:
        return pd.DataFrame()

    try:
        with connection.cursor() as cursor:
            # 基础SQL，只选择需要的字段，利用school_type索引
            sql = f"""
                  SELECT id, schoolname, city_id, sctype, province_id
                  FROM nobook_school
                  WHERE sctype IN ({', '.join([str(tp) for tp in SCHOOL_TYPE_GROUPS[school_group]])})
                  {'AND city_id = ' + str(city_id) if city_id else ''}
                  """

            # 执行查询
            cursor.execute(sql)
            result = cursor.fetchall()

            # 转换为DataFrame并添加学校类型名称
            schools_df = pd.DataFrame(result)
            if not schools_df.empty:
                schools_df['school_type_name'] = schools_df['sctype'].apply(
                    lambda x: SCHOOL_TYPE_MAP.get(x, f"未知类型({x})")
                )

                # 只缓存不包含城市筛选的数据
                if not city_id:
                    SCHOOLS_CACHE[school_group] = schools_df

                print(f"成功加载 {len(schools_df)} 所{school_group}学校信息")

            return schools_df

    except Exception as e:
        print(f"加载{school_group}学校数据出错: {e}")
        return pd.DataFrame()
    finally:
        if connection:
            connection.close()


def get_city_id_by_name(city_name: str) -> Optional[int]:
    """根据城市名称获取城市ID，用于精准筛选学校"""
    connection = get_db_connection()
    if not connection:
        return None

    try:
        with connection.cursor() as cursor:
            # 利用城市名称查询，假设name字段有索引
            sql = "SELECT id FROM nobook_city WHERE name LIKE %s LIMIT 1"
            cursor.execute(sql, (f'%{city_name}%',))
            result = cursor.fetchone()

            return result['id'] if result else None

    except Exception as e:
        print(f"查询城市ID出错: {e}")
        return None
    finally:
        if connection:
            connection.close()


def generate_school_education(age: int, location: str) -> List[Dict[str, Any]]:
    """生成中小学教育经历"""
    experiences = []
    current_year = datetime.now().year
    city_id = get_city_id_by_name(location)  # 获取所在地城市ID

    # 检查是否有九年一贯制或十二年一贯制学校
    use_combined_school = False
    use_twelve_year_schools = False
    combined_school = None

    # 尝试获取九年一贯制学校(3-6年级+初中)
    nine_year_schools = load_schools_by_type('nine_year', city_id)
    if not nine_year_schools.empty and random.random() > 0.3:
        use_combined_school = True
        combined_school = nine_year_schools.sample().iloc[0]

        if '十二年' in combined_school['school_type_name']:
            use_twelve_year_schools = True

        # 九年一贯制学校经历(6-15岁)
        start_age = 6
        end_age = 15

        if age > start_age:
            start_year = current_year - (age - start_age)
            end_year = current_year - (age - end_age)

            experiences.append({
                'school_name': combined_school['schoolname'],
                'education_level': '九年一贯制',
                'school_type': combined_school['school_type_name'],
                'start_date': f"{start_year}-09",
                'end_date': f"{end_year}-07",
                'is_graduated': True
            })

    # 如果没有使用九年一贯制学校，则分别生成小学和中学经历
    if not use_combined_school:
        # 小学 (通常6-12岁)
        primary_start_age = 6
        primary_end_age = 12

        if age > primary_start_age:
            primary_start_year = current_year - (age - primary_start_age)
            primary_end_year = current_year - (age - primary_end_age)

            # 获取小学列表
            primary_schools = load_schools_by_type('primary', city_id)

            # 随机选择一所小学
            if not primary_schools.empty:
                school = primary_schools.sample().iloc[0]
                school_name = school['schoolname']
                school_type_name = school['school_type_name']
            else:
                school_name = f"{location}{random.choice(['第一小学', '实验小学', '中心小学'])}"
                school_type_name = random.choice(["公立小学", "私立小学"])

            experiences.append({
                'school_name': school_name,
                'education_level': '小学',
                'school_type': school_type_name,
                'start_date': f"{primary_start_year}-09",
                'end_date': f"{primary_end_year}-07",
                'is_graduated': True
            })

    # 中学教育 (如果没有被一贯制学校覆盖)
    if not (use_twelve_year_schools and age > 15):
        for times in range(0, 1 + int(not use_combined_school)):
            middle_start_age = 13 if not use_combined_school else 16 + times * 3
            middle_end_age = 16 if not use_combined_school else 18 + times * 3

            if age > middle_start_age:
                middle_start_year = current_year - (age - middle_start_age)
                middle_end_year = current_year - (age - middle_end_age)

                # 获取中学列表
                middle_schools = load_schools_by_type('middle', city_id)

                # 随机选择一所中学
                if not middle_schools.empty:
                    school = middle_schools.sample().iloc[0]
                    school_name = school['schoolname']
                    school_type_name = school['school_type_name']
                else:
                    school_name = f"{location}{random.choice(['第一中学', '实验中学', '第二中学'])}"
                    school_type_name = random.choice(["公立中学", "私立中学"])

                experiences.append({
                    'school_name': school_name,
                    'education_level': '中学',
                    'school_type': school_type_name,
                    'start_date': f"{middle_start_year}-09",
                    'end_date': f"{middle_end_year}-07",
                    'is_graduated': True
                })

    return experiences


def generate_higher_education(age: int, school_name: str, location: str) -> List[Dict[str, Any]]:
    """生成高等教育经历"""
    experiences = []
    current_year = datetime.now().year

    # 确定教育层次
    if age <= 22:
        probable_levels = ['专科', '本科']
    elif age <= 25:
        probable_levels = ['本科', '硕士']
    else:
        probable_levels = ['本科', '硕士', '博士']

    probability = \
        3 if random.randint(1, 10) > 9 else 2\
            if random.randint(1, 10) > 6 else 1\
            if random.randint(1, 10) > 0 else 0
    probable_levels = probable_levels[:min(probability, len(probable_levels))]

    for education_level in probable_levels:
        # 计算入学和毕业时间
        if education_level == '本科':
            duration = 4
            grad_age = 22 if age >= 22 else age
        elif education_level == '硕士':
            duration = 3
            grad_age = 25 if age >= 25 else age
        elif education_level == '博士':
            duration = random.randint(3, 5)
            grad_age = 28 if age >= 28 else age
        else:  # 专科
            duration = 3
            grad_age = 21 if age >= 21 else age

        grad_year = current_year - (age - grad_age)
        start_year = grad_year - duration

        # 专业选择
        majors = [
            '计算机科学与技术', '电子信息工程', '市场营销', '会计学',
            '金融学', '机械工程', '土木工程', '英语',
            '汉语言文学', '法学', '医学', '教育学'
        ]
        major = random.choice(majors)

        # 学校名称
        if not school_name or school_name.strip() == "":
            if not UNIVERSITIES.empty:
                school = UNIVERSITIES.sample().iloc[0]
                school_name = school['学校名称']
            else:
                school_name = f"{location}{random.choice(['大学', '学院', '理工大学', '师范大学'])}"

        experiences.append({
            'school_name': school_name,
            'education_level': education_level,
            'major': major,
            'start_date': f"{start_year}-09",
            'end_date': f"{grad_year}-07",
            'is_graduated': True
        })

    return experiences


def generate_education_experience(job_seeker_info: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], str]:
    """
    生成求职者的完整教育经历，包括中小学和高等教育

    Args:
        job_seeker_info: 求职者基本信息字典

    Returns:
        按时间排序的教育经历列表
    """
    education_experiences = []

    # 获取求职者信息
    age = job_seeker_info.get('age', 22)
    school_name = job_seeker_info.get('school', '')
    location = job_seeker_info.get('location', '')

    # 生成中小学教育经历
    school_experiences = generate_school_education(age, location)
    education_experiences.extend(school_experiences)

    # 生成高等教育经历
    higher_education = generate_higher_education(age, school_name, location)
    education_experiences.extend(higher_education)

    # 按时间正序排列（从早到晚）
    education_experiences.sort(key=lambda x: x['start_date'])

    return education_experiences, education_experiences[-1]['end_date'].split('-')[0]


if __name__ == "__main__":
    from data_simulation.data_resource_lib.generate_job_seeker_data import generate_job_seeker
    # 生成求职者并获取教育经历
    job_seeker = generate_job_seeker(2)[0]
    print("求职者信息:", job_seeker)

    edu_exp, _ = generate_education_experience(job_seeker)

    print("\n教育经历:")
    for i, exp in enumerate(edu_exp, 1):
        print(f"{i}. {exp['school_name']} - {exp['education_level']} ({exp['start_date']} 至 {exp['end_date']})")
        if 'major' in exp:
            print(f"   专业: {exp['major']}")
        if 'school_type' in exp:
            print(f"   类型: {exp['school_type']}")
