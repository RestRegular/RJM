from django.db import models
from django.core.validators import MinValueValidator

# 复用原代码中的职位类别定义（保持一致性）
job_categories = [
    "技术", "产品", "运营", "市场", "销售", "人力资源", "财务"
]
# 工作类型选项（复用原代码中的有效类型）
job_sorts = ["全职", "兼职", "实习", "远程"]


class Job(models.Model):
    """职位数据模型，对应JobDataBuilder"""
    job_id = models.IntegerField(primary_key=True, unique=True, verbose_name="职位ID")
    job_title = models.CharField(max_length=200, verbose_name="职位名称")
    company_name = models.CharField(max_length=100, verbose_name="公司名称")
    city = models.CharField(max_length=50, verbose_name="工作城市")
    province = models.CharField(max_length=50, verbose_name="所在省份")
    country = models.CharField(max_length=50, verbose_name="所在国家")
    salary_range = models.CharField(max_length=20, verbose_name="薪资范围（如18k-25k）")
    salary_low = models.IntegerField(validators=[MinValueValidator(0)], verbose_name="薪资下限（k）")
    salary_high = models.IntegerField(validators=[MinValueValidator(0)], verbose_name="薪资上限（k）")
    job_description = models.TextField(verbose_name="职位描述")
    requirements = models.TextField(verbose_name="职位要求")
    job_category = models.CharField(
        max_length=20,
        choices=[(c, c) for c in job_categories],  # 限制为预定义类别
        verbose_name="职位类别"
    )
    minimum_work_time = models.CharField(max_length=20, verbose_name="最低工作年限（如3年）")
    job_sort = models.CharField(
        max_length=10,
        choices=[(s, s) for s in job_sorts],  # 限制为预定义工作类型
        verbose_name="工作类型"
    )
    required_skills = models.JSONField(verbose_name="要求的技能列表")  # 存储列表[]
    created_at = models.DateTimeField(auto_now_add=True, verbose_name="创建时间")
    updated_at = models.DateTimeField(auto_now=True, verbose_name="更新时间")

    class Meta:
        verbose_name = "职位"
        verbose_name_plural = "职位"
        ordering = ["job_id"]  # 按创建时间倒序排列

    def __str__(self):
        return f"{self.job_title}（{self.company_name}）"
