from django.db import models

job_categories = [
    "技术", "产品", "运营", "市场", "销售", "人力资源", "财务"
]


class Resume(models.Model):
    """简历数据模型，对应ResumeDataBuilder"""
    resume_id = models.IntegerField(primary_key=True, unique=True, verbose_name="简历ID")
    category = models.CharField(
        max_length=20,
        choices=[(c, c) for c in job_categories],  # 限制为预定义类别
        verbose_name="求职岗位类别"
    )
    title = models.CharField(max_length=200, verbose_name="简历标题")
    desc = models.TextField(verbose_name="简历描述（期望薪资/城市等）")
    photo = models.URLField(blank=True, null=True, verbose_name="照片URL")
    email = models.EmailField(verbose_name="联系邮箱")
    name = models.CharField(max_length=50, verbose_name="姓名")
    education = models.JSONField(verbose_name="教育经历")  # 存储列表[{}]
    work_experience = models.JSONField(verbose_name="工作经历")  # 存储列表[{}]
    project_experience = models.JSONField(verbose_name="项目经历")  # 存储列表[{}]
    skill = models.JSONField(verbose_name="技能列表")  # 存储列表[]
    introduction = models.TextField(verbose_name="个人介绍")
    address = models.TextField(verbose_name="联系地址")
    created_at = models.DateTimeField(auto_now_add=True, verbose_name="创建时间")
    updated_at = models.DateTimeField(auto_now=True, verbose_name="更新时间")

    class Meta:
        verbose_name = "简历"
        verbose_name_plural = "简历"
        ordering = ["resume_id"]  # 按创建时间倒序排列

    def __str__(self):
        return f"{self.name}的简历（ID: {self.resume_id}）"
