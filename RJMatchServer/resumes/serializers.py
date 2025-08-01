from rest_framework import serializers
from .models import Resume, job_categories


class EducationSerializer(serializers.Serializer):
    """教育经历子序列化器"""
    school = serializers.CharField(max_length=100)
    major = serializers.CharField(max_length=100)
    time = serializers.CharField(max_length=50)
    level = serializers.CharField(max_length=20)


class WorkExperienceSerializer(serializers.Serializer):
    """工作经历子序列化器"""
    company = serializers.CharField(max_length=100)
    job = serializers.CharField(max_length=100)
    desc = serializers.CharField()
    time = serializers.CharField(max_length=50)


class ProjectExperienceSerializer(serializers.Serializer):
    """项目经历子序列化器"""
    title = serializers.CharField(max_length=100)
    desc = serializers.CharField()
    time = serializers.CharField(max_length=50)
    role = serializers.CharField(max_length=50)


class ResumeSerializer(serializers.ModelSerializer):
    """简历模型序列化器"""
    # 嵌套序列化器用于验证复杂结构
    education = EducationSerializer(many=True)
    work_experience = WorkExperienceSerializer(many=True)
    project_experience = ProjectExperienceSerializer(many=True)
    skill = serializers.ListField(
        child=serializers.CharField(max_length=50)
    )

    class Meta:
        model = Resume
        fields = [
            'resume_id', 'category', 'title', 'desc', 'photo',
            'email', 'name', 'education', 'work_experience',
            'project_experience', 'skill', 'introduction',
            'address', 'created_at', 'updated_at'
        ]
        read_only_fields = ['created_at', 'updated_at']  # 只读字段

    def validate_category(self, value):
        """验证职位类别是否有效"""
        if value not in job_categories:
            raise serializers.ValidationError(
                f"职位类别必须是以下之一: {', '.join(job_categories)}"
            )
        return value

    def validate_email(self, value):
        """验证邮箱格式"""
        if '@' not in value:
            raise serializers.ValidationError("邮箱格式不正确")
        return value
