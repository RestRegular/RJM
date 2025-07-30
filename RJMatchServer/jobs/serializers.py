from rest_framework import serializers
from .models import Job, job_categories, job_sorts
import re


class JobSerializer(serializers.ModelSerializer):
    """职位模型序列化器"""
    required_skills = serializers.ListField(
        child=serializers.CharField(max_length=50)
    )

    class Meta:
        model = Job
        fields = [
            'job_id', 'job_title', 'company_name', 'city', 'province',
            'country', 'salary_range', 'salary_low', 'salary_high',
            'job_description', 'requirements', 'job_category',
            'minimum_work_time', 'job_sort', 'required_skills',
            'created_at', 'updated_at'
        ]
        read_only_fields = ['created_at', 'updated_at']  # 只读字段

    def validate_job_category(self, value):
        """验证职位类别是否有效"""
        if value not in job_categories:
            raise serializers.ValidationError(
                f"职位类别必须是以下之一: {', '.join(job_categories)}"
            )
        return value

    def validate_job_sort(self, value):
        """验证工作类型是否有效"""
        if value not in job_sorts:
            raise serializers.ValidationError(
                f"工作类型必须是以下之一: {', '.join(job_sorts)}"
            )
        return value

    def validate_salary_range(self, value):
        """验证薪资范围格式"""
        pattern = r'^(\d+)k-(\d+)k$'
        match = re.match(pattern, value)

        if not match or len(match.groups()) != 2:
            raise serializers.ValidationError(
                "薪资范围格式不匹配，正确格式应为 'xxk-yyk'（如'18k-25k'）"
            )

        salary_low = int(match.group(1))
        salary_high = int(match.group(2))

        if salary_low > salary_high:
            raise serializers.ValidationError(
                "薪资下限不能大于薪资上限"
            )

        return value

    def validate_minimum_work_time(self, value):
        """验证最低工作年限格式"""
        if '年' not in value:
            raise serializers.ValidationError(
                "最低工作年限格式不正确，应包含'年'，如'3年'"
            )
        return value

    def validate(self, data):
        """交叉验证薪资范围和上下限是否一致"""
        # 从薪资范围提取数值
        if 'salary_range' in data:
            match = re.match(r'^(\d+)k-(\d+)k$', data['salary_range'])
            if match:
                salary_low = int(match.group(1))
                salary_high = int(match.group(2))

                # 检查是否与单独提供的薪资上下限一致
                if 'salary_low' in data and data['salary_low'] != salary_low:
                    raise serializers.ValidationError({
                        'salary_low': f"与薪资范围不符，应为{salary_low}"
                    })
                if 'salary_high' in data and data['salary_high'] != salary_high:
                    raise serializers.ValidationError({
                        'salary_high': f"与薪资范围不符，应为{salary_high}"
                    })

        return data
