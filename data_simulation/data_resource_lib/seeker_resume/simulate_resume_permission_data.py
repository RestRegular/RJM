import random


def generate_random_time():
    # 生成随机的时间字符串，格式为 YY/MM/DD HH:mm:ss
    year = random.randint(2024, 2025)
    month = random.randint(1, 12)
    day = random.randint(1, 28)
    hour = random.randint(0, 23)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    return f"{year}/{month:02d}/{day:02d} {hour:02d}:{minute:02d}:{second:02d}"


def generate_private_permission():
    # 生成私密状态的简历权限
    return {
        "status": "private",
        "details": {}
    }


def generate_public_permission():
    # 生成公开状态的简历权限
    start_time = generate_random_time()
    end_time = generate_random_time()
    # 确保结束时间在开始时间之后
    while end_time <= start_time:
        end_time = generate_random_time()
    return {
        "status": "public",
        "details": {
            "publicDetails": {
                "timeline": {
                    "start": start_time,
                    "end": end_time
                }
            }
        }
    }


def generate_invite_permission():
    # 生成邀请状态的简历权限，随机生成 1 到 5 个被邀请者
    invitee_count = random.randint(1, 5)
    invitees = []
    for _ in range(invitee_count):
        user_id = f"user_{random.randint(1000, 9999)}"
        start_time = generate_random_time()
        end_time = generate_random_time()
        while end_time <= start_time:
            end_time = generate_random_time()
        invitees.append({
            "id": user_id,
            "timeline": {
                "start": start_time,
                "end": end_time
            }
        })
    return {
        "status": "invite",
        "details": {
            "invitationDetails": {
                "invitees": invitees
            }
        }
    }


if __name__ == "__main__":
    # 随机生成不同状态的简历权限数据
    permission_types = ["private", "public", "invite"]
    random_permission_type = random.choice(permission_types)

    if random_permission_type == "private":
        permission_data = generate_private_permission()
    elif random_permission_type == "public":
        permission_data = generate_public_permission()
    else:
        permission_data = generate_invite_permission()

    # 打印生成的简历权限数据
    import pprint
    pprint.pprint(permission_data)
