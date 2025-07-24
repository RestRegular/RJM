#
# created by RestRegular on 2025/7/23
#
from functools import wraps
from typing import List, Union
from warnings import warn


class RJMError(Exception):
    """
    所有RJM相关异常的基类

    提供统一的异常信息格式化功能，确保所有自定义异常具有一致的输出格式
    """

    def __init__(self, error_title: str, error_msg: Union[List[str], str] = None):
        # 计算异常类名长度，用于格式化错误信息的缩进对齐
        space = " " * len(self.__class__.__name__)
        error_msg = error_msg or ""

        # 如果错误信息是列表，则转换为带缩进的多行字符串
        if isinstance(error_msg, list):
            error_msg = f'\n{space}- '.join(error_msg)

        # 调用父类构造函数，组装完整的错误信息
        super().__init__(f"\n{self.__class__.__name__}: {error_title}" +
                         (f"\n{space}- {error_msg}" if len(error_msg) > 0 else ""))


class RJMConfigurationError(RJMError):
    """
    配置错误异常类

    当系统配置参数无效或缺失时抛出，用于标识配置相关的错误
    """

    def __init__(self, config_name: str, error_info: Union[str, List[str]]):
        """
        初始化配置错误异常

        Args:
            config_name: 出现错误的配置项名称
            error_info: 错误详情，可以是字符串或字符串列表
        """
        super().__init__(
            "This error is caused by a configuration error.",
            [f"Error config: '{config_name}'",
             *(error_info if isinstance(error_info, list) else [error_info])]
        )


class ResumeValidationError(RJMError):
    """
    简历数据验证错误异常类

    当简历数据构建或验证过程中发现不符合要求的数据时抛出
    """

    def __init__(self, field_name: str, error_info: Union[str, List[str]]):
        """
        初始化简历数据验证错误异常

        Args:
            field_name: 验证失败的字段名称
            error_info: 错误详情，可以是字符串或字符串列表
        """
        super().__init__(
            "Resume data validation failed",
            [f"Builder method execution failed: '{field_name}'",
             *(error_info if isinstance(error_info, list) else [error_info])]
        )


class JobValidationError(RJMError):
    """
    职位数据验证错误异常类

    当职位数据构建或验证过程中发现不符合要求的数据时抛出
    """

    def __init__(self, field_name: str, error_info: Union[str, List[str]]):
        """
        初始化职位数据验证错误异常

        Args:
            field_name: 验证失败的字段名称
            error_info: 错误详情，可以是字符串或字符串列表
        """
        super().__init__(
            "Job data validation failed",
            [f"Builder method execution failed: '{field_name}'",
             *(error_info if isinstance(error_info, list) else [error_info])]
        )


def deprecated(message):
    """
    用于标记函数或类已过时的装饰器

    当调用被装饰的函数或类时，会发出DeprecationWarning警告，提示用户该功能已过时

    Args:
        message: 关于此过时功能的提示信息，说明替代方案或移除计划

    Returns:
        装饰器函数，用于包装被标记为过时的函数或类
    """

    def decorator(func_or_class):
        @wraps(func_or_class)
        def wrapper(*args, **kwargs):
            # 发出警告，stacklevel=2确保警告指向调用处而非装饰器内部
            warn(f"\n{message}", DeprecationWarning, stacklevel=2)
            # 调用原始函数/类并返回结果
            return func_or_class(*args, **kwargs)

        return wrapper

    return decorator


if __name__ == '__main__':
    # 示例：抛出一个配置错误异常，用于演示异常信息格式
    raise RJMConfigurationError("test error", "error info")
