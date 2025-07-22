# Resume-JobMatcher

这是一个基于 `Flink`、`MySQL`、`Redis` 和 `Kafka` 的职位与简历自动匹配系统，目前能够生成模拟数据并实时计算简历与岗位的匹配度。

---

## 系统架构

系统主要包含三个核心模块：

- **数据生成器（`data_generator.py`）**：生成模拟的简历和岗位数据
- **数据库管理器（`database_manager.py`）**：统一管理 `MySQL`、`Redis` 和 `Kafka` 的操作
- **Flink 匹配处理器（`flink_matching_processor.py`）**：实时计算简历与岗位的匹配度

---

## 功能说明

- **数据生成**：生成真实感的简历和岗位数据，包括**基本信息**、**技能标签**、**工作经验**、**薪资期望**等
- **多源存储**：支持将数据存储到 `MySQL`（持久化）、`Redis`（缓存与索引）和 `Kafka`（消息队列）
- **实时匹配**：通过 `Flink` 流处理实时计算简历与岗位的匹配度
- **多维度匹配**：综合考虑**技能**、**工作经验**、**薪资期望**和**岗位**类别等多个维度进行匹配

---

## 环境依赖

- `Python 3.7+`
- `MySQL 5.7+`
- `Redis 6.0+`
- `Kafka 2.8+`
- `Apache Flink 1.17+`
- 所需 `Python` 库：
  - `pandas`
  - `faker`
  - `mysql-connector-python`
  - `redis`
  - `kafka-python`
  - `pyflink`
  
---

## 数据存储设计

- **MySQL**

  ![Mysql database tables](https://img.scdn.io/i/687f39aecc97b_1753168302.png "Mysql database tables")

  - **`jobs`**：存储岗位详细信息
    
    ![jobs table structure](https://img.scdn.io/i/687f39d840b6b_1753168344.webp "'jobs' table structure")
  - **`resume_job_matches`**：存储简历与岗位的匹配结果
    
    ![resume_job_matches table structure](https://img.scdn.io/i/687f39ead8000_1753168362.webp "'resume_job_matches' table structure")
  - **`resumes`**：存储简历详细信息
    
    ![resumes table structure](https://img.scdn.io/i/687f39facdcd4_1753168378.webp "resumes table structure")
- **Redis**
  - **哈希表（`Hash`）**：存储简历和岗位的详细信息
  - **集合（`Set`）**：建立类别、地区和技能的索引
  - **有序集合（`ZSet`）**：建立薪资和工作经验的排序索引
- **Kafka**
  - **`resumes_topic`**：用于传输简历数据的消息队列

---

## 扩展计划

- 开发 API 接口供前端调用相关功能
- 实现前端真实数据的解析与处理
- 优化数据存储方案，提高匹配效率与性能