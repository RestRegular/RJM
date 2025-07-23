#
# created by RestRegular on 2025/7/23
#
from typing import List, Optional, Union


class RJMError(Exception):
    def __init__(self, error_title: str, error_msg: Union[List[str], str] = None):
        space = " " * len(self.__class__.__name__)
        error_msg = error_msg or ""
        if isinstance(error_msg, list):
            error_msg = f'\n{space}- '.join(error_msg)
        super().__init__(f"\n{self.__class__.__name__}: {error_title}" +
                         (f"\n{space}- {error_msg}" if len(error_msg) > 0 else ""))


class RJMConfigurationError(RJMError):
    def __init__(self, config_name: str, error_info: Union[str, List[str]]):
        super().__init__("This error is caused by a configuration error.",
                         [f"Error config: '{config_name}'",
                          *(error_info if isinstance(error_info, list) else [error_info])])


class ResumeValidationError(RJMError):
    def __init__(self, field_name: str, error_info: Union[str, List[str]]):
        super().__init__("Resume data validation failed",
                         [f"Builder method execution failed: '{field_name}'",
                          *(error_info if isinstance(error_info, list) else [error_info])])


class JobValidationError(RJMError):
    def __init__(self, field_name: str, error_info: Union[str, List[str]]):
        super().__init__("Job data validation failed",
                         [f"Builder method execution failed: '{field_name}'",
                          *(error_info if isinstance(error_info, list) else [error_info])])


if __name__ == '__main__':
    raise RJMConfigurationError("test error", "error info")
