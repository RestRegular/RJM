import json
import random
from collections import defaultdict

SURNAME_SELECT_RATIO = 0.6              # 随机选择姓氏的比例（0.6=选60%的姓氏，也可直接用SURNAME_SELECT_NUM固定数量）
# SURNAME_SELECT_NUM = 10                 # 可选：直接固定要选择的姓氏数量（优先级高于比例，启用时注释上一行）
MIN_SAMPLES_PER_GENDER_PER_SURNAME = 1  # 选中的姓氏中，每个性别最少抽1个样本（避免配额过小导致无效）
RANDOM_SEED = 42                        # 随机种子（固定后结果可复现，删除则每次随机）


# ---------------------- 读取并校验原始数据 ----------------------
def read_and_validate_data(file_path):
    """读取JSON并过滤无效数据（空列表、缺少male/female键的姓氏）"""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        valid_data = {}
        for surname, gender_dict in data.items():
            # 校验结构：必须包含male和female键，且对应非空列表
            if not isinstance(gender_dict, dict) or "male" not in gender_dict or "female" not in gender_dict:
                print(f"跳过无效姓氏「{surname}」：缺少male/female键")
                continue
            # 过滤空字符串，保留有效姓名
            male_names = [name.strip() for name in gender_dict["male"] if name.strip()]
            female_names = [name.strip() for name in gender_dict["female"] if name.strip()]
            if not male_names and not female_names:
                print(f"跳过姓氏「{surname}」：male和female列表均为空")
                continue
            valid_data[surname] = {"male": male_names, "female": female_names}

        if not valid_data:
            print("错误：无有效姓名数据，请检查JSON格式")
            return None
        print(f"原始数据：共{len(valid_data)}个有效姓氏")
        return valid_data
    except Exception as e:
        print(f"读取JSON失败：{str(e)}")
        return None


# ---------------------- 随机筛选部分姓氏 ----------------------
def random_select_surnames(valid_data):
    """从有效姓氏中随机选择部分（按比例或固定数量）"""
    all_surnames = list(valid_data.keys())
    total_surnames = len(all_surnames)

    # 确定要选择的姓氏数量
    if "SURNAME_SELECT_NUM" in globals():  # 优先用固定数量
        select_num = min(globals().get("SURNAME_SELECT_NUM"), total_surnames)
    else:  # 用比例计算
        select_num = max(1, int(total_surnames * SURNAME_SELECT_RATIO))  # 至少选1个姓氏
        select_num = min(select_num, total_surnames)  # 避免超过总姓氏数

    # 随机选择姓氏
    selected_surnames = random.sample(all_surnames, select_num)
    # 构建选中姓氏的数据集
    selected_data = {surname: valid_data[surname] for surname in selected_surnames}

    print(f"随机选中{len(selected_surnames)}个姓氏：{selected_surnames}")
    return selected_data


# ---------------------- 计算选中姓氏的男/女样本配额 ----------------------
def calculate_gender_quota(selected_data, total_samples):
    """在选中姓氏中，按名字数量分配男/女配额（确保总男女1:1）"""
    # 1. 计算选中姓氏的总男/女名字数
    total_male_names = sum(len(data["male"]) for data in selected_data.values())
    total_female_names = sum(len(data["female"]) for data in selected_data.values())

    # 校验是否有足够的男/女数据
    if total_male_names == 0 or total_female_names == 0:
        print("错误：选中的姓氏中缺少男性或女性姓名，无法抽取1:1样本")
        return None, None

    # 2. 固定总男女配额（总样本数调整为偶数）
    total_quota_per_gender = total_samples // 2
    print(f"总样本调整为{total_quota_per_gender*2}个（男性{total_quota_per_gender}个，女性{total_quota_per_gender}个）")

    # 3. 分配男性配额（按每个姓氏的男性名字数量占比）
    male_quota = _assign_quota(
        selected_data=selected_data,
        gender="male",
        total_quota=total_quota_per_gender,
        min_quota=MIN_SAMPLES_PER_GENDER_PER_SURNAME,
        total_name_count=total_male_names
    )

    # 4. 分配女性配额（逻辑同男性）
    female_quota = _assign_quota(
        selected_data=selected_data,
        gender="female",
        total_quota=total_quota_per_gender,
        min_quota=MIN_SAMPLES_PER_GENDER_PER_SURNAME,
        total_name_count=total_female_names
    )

    return male_quota, female_quota


def _assign_quota(selected_data, gender, total_quota, min_quota, total_name_count):
    """辅助函数：给选中的姓氏分配单个性别的样本配额"""
    quota_dict = {}
    remaining_quota = total_quota

    # 第一步：按名字数量占比分配基础配额（确保每个姓氏该性别至少min_quota个）
    for surname, data in selected_data.items():
        name_count = len(data[gender])
        if name_count == 0:
            quota_dict[surname] = 0
            continue

        # 按权重计算基础配额
        weight = name_count / total_name_count
        base_quota = int(round(weight * total_quota))
        # 调整配额：不低于min_quota，不超过该姓氏该性别的总名字数
        final_quota = max(min_quota, min(base_quota, name_count))

        quota_dict[surname] = final_quota
        remaining_quota -= final_quota

    # 第二步：处理剩余配额（优先分给名字数量多的姓氏，不超过其总名字数）
    sorted_surnames = sorted(
        [s for s in selected_data.keys() if len(selected_data[s][gender]) > 0],
        key=lambda x: len(selected_data[x][gender]),
        reverse=True  # 名字多的姓氏优先
    )

    for surname in sorted_surnames:
        if remaining_quota <= 0:
            break
        current_quota = quota_dict[surname]
        max_possible = len(selected_data[surname][gender])  # 最多抽该姓氏所有名字
        if current_quota < max_possible:
            quota_dict[surname] += 1
            remaining_quota -= 1

    # 第三步：若仍有剩余配额（部分姓氏已无多余名字），减少配额并提示
    if remaining_quota > 0:
        print(f"警告：{gender}性别剩余{remaining_quota}个配额无法分配，最终{gender}样本数为{total_quota - remaining_quota}")
        # 从配额多的姓氏中减少（避免低于min_quota）
        for surname in sorted(sorted_surnames, key=lambda x: quota_dict[x], reverse=True):
            if remaining_quota <= 0 or quota_dict[surname] <= min_quota:
                break
            quota_dict[surname] -= 1
            remaining_quota -= 1

    return quota_dict


# ---------------------- 按配额随机抽取样本 ----------------------
def extract_samples(selected_data, male_quota, female_quota):
    """在选中的姓氏中，按配额抽取男/女样本"""
    random.seed(RANDOM_SEED)  # 固定随机种子（可删除）
    final_samples = defaultdict(lambda: {"male": [], "female": []})

    for surname, data in selected_data.items():
        # 抽取男性样本
        male_need = male_quota.get(surname, 0)
        if male_need > 0 and len(data["male"]) >= male_need:
            final_samples[surname]["male"] = random.sample(data["male"], male_need)

        # 抽取女性样本
        female_need = female_quota.get(surname, 0)
        if female_need > 0 and len(data["female"]) >= female_need:
            final_samples[surname]["female"] = random.sample(data["female"], female_need)

    # 过滤空数据（避免姓氏无任何样本）
    final_samples = {
        s: g_data for s, g_data in final_samples.items()
        if g_data["male"] or g_data["female"]
    }
    return final_samples


def extract_name_samples(json_file_path, total_samples):
    """
    从指定的姓名JSON文件中抽取指定数量的样本

    参数:
        json_file_path (str): 原始姓名JSON文件路径（格式：{"姓": {"male": [...], "female": [...]}}）
        total_samples (int): 最终总样本数（会自动调整为偶数，确保男女比例1:1）

    返回:
        dict: 抽取的样本字典，格式与原始数据一致
    """
    # 步骤1：读取有效数据
    valid_data = read_and_validate_data(json_file_path)
    if not valid_data:
        return None

    # 步骤2：随机选择部分姓氏
    selected_data = random_select_surnames(valid_data)
    if not selected_data:
        return None

    # 步骤3：计算男/女样本配额
    male_quota, female_quota = calculate_gender_quota(selected_data, total_samples)
    if not male_quota or not female_quota:
        return None

    # 步骤4：抽取样本
    sample_result = extract_samples(selected_data, male_quota, female_quota)

    # 直接返回抽取的样本字典
    return sample_result


# ---------------------- 6. 主函数（串联所有步骤） ----------------------
def main():
    JSON_FILE_PATH = "../resource/job_seeker_names.json"       # 原始姓名JSON路径（格式：{"姓": {"male": [...], "female": [...]}}）
    TOTAL_SAMPLES = 5000                      # 最终总样本数（自动调整为偶数，确保1:1）
    OUTPUT_FILE_PATH = "../resource/random_surname_samples_1v1.json"  # 结果保存路径

    # 步骤1：读取有效数据
    valid_data = read_and_validate_data(JSON_FILE_PATH)
    if not valid_data:
        exit()

    # 步骤2：随机选择部分姓氏
    selected_data = random_select_surnames(valid_data)
    if not selected_data:
        exit()

    # 步骤3：计算男/女样本配额
    male_quota, female_quota = calculate_gender_quota(selected_data, TOTAL_SAMPLES)
    if not male_quota or not female_quota:
        exit()

    # 步骤4：打印配额分配（方便核对）
    print("\n=== 选中姓氏的样本配额 ===")
    for surname in selected_data.keys():
        m_q = male_quota[surname]
        f_q = female_quota[surname]
        print(f"「{surname}」：男性{m_q}个（原{len(selected_data[surname]['male'])}个），女性{f_q}个（原{len(selected_data[surname]['female'])}个）")

    # 步骤5：抽取样本
    sample_result = extract_samples(selected_data, male_quota, female_quota)

    # 步骤6：统计并保存结果
    total_male = sum(len(g["male"]) for g in sample_result.values())
    total_female = sum(len(g["female"]) for g in sample_result.values())
    print(f"\n=== 抽取完成 ===")
    print(f"最终样本：男性{total_male}个，女性{total_female}个，比例{total_male}:{total_female}")
    print(f"涉及姓氏：{list(sample_result.keys())}（共{len(sample_result)}个）")

    # 保存结果（格式与原始数据一致）
    with open(OUTPUT_FILE_PATH, "w", encoding="utf-8") as f:
        json.dump(sample_result, f, ensure_ascii=False, indent=2)
    print(f"\n结果已保存到：{OUTPUT_FILE_PATH}")


if __name__ == "__main__":
    main()
