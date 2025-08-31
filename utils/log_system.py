import datetime
import os
import logging
from typing import Optional, Literal
import traceback

from newrcc.CColor import TextColor, Decoration
from newrcc.CConsole import colorfulText

__all__ = [
    "get_logger",
    "ConsoleHandler"
]

log_level_mapping = {
    'notset': logging.NOTSET,
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'warn': logging.WARN,
    'warning': logging.WARNING,
    'error': logging.ERROR,
    'fatal': logging.FATAL,
    'critical': logging.CRITICAL
}


class ConsoleHandler(logging.Handler):
    _max_logger_name_length = 0
    _level_color_mapping = {
        logging.NOTSET: TextColor.GRAY,
        logging.DEBUG: TextColor.LIGHT_BLUE,
        logging.INFO: TextColor.WHITE,
        logging.WARN: TextColor.LIGHT_YELLOW,
        logging.WARNING: TextColor.LIGHT_YELLOW,
        logging.ERROR: TextColor.LIGHT_RED,
        logging.FATAL: TextColor.RED,
        logging.CRITICAL: TextColor.RED
    }

    def __init__(self, name: str, level: int,
                 colorful_log: bool = True):
        super().__init__()
        self.setLevel(level)
        self.colorful_log = colorful_log
        self.name = '>'.join(reversed([module if len(module) < 15 else f"{module[:12]}..." for module in name.split('.')]))
        ConsoleHandler._max_logger_name_length = max(len(self.name), ConsoleHandler._max_logger_name_length)

    def emit(self, record):
        format_record = self.format(record)
        print(format_record)

    def format(self, record):
        # 格式化时间为固定长度（23字符），包含毫秒
        created_time = datetime.datetime.fromtimestamp(record.created)
        time_str = created_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:23]

        # 日志级别统一为8字符宽度，居中显示
        level_name = f"{record.levelname:^10}"
        log_msg = f">>> {record.getMessage()}"

        # 限制模块名长度，过长时截断并添加省略号
        module_name = self.name

        # 函数名和位置信息格式化
        location = f"{record.funcName if record.funcName else '<unknown>'}@{record.filename}:{record.lineno}"

        if self.colorful_log:
            level_color = self._level_color_mapping.get(record.levelno)
            time_str = colorfulText(time_str, TextColor((146, 146, 215)))
            level_name = colorfulText(level_name, level_color, decorations=[Decoration.BOLD])
            log_msg = colorfulText(log_msg, level_color)
            module_name = colorfulText(module_name, TextColor.PURPLE, decorations=[Decoration.BOLD])
            location = colorfulText(location, TextColor.CYAN, decorations=[Decoration.ITALIC, Decoration.L_UNDERLINE])

        # 处理异常信息
        if record.exc_info:
            exc_text = self.format_exception(record.exc_info)
            log_msg += f"\n{exc_text}"

        # 组合日志消息
        return (
            f"[{time_str}] "
            f"[{level_name}] "
            f"[{module_name}] "
            f"[{location}]\n"
            f"{log_msg}"
        )

    def format_exception(self, exc_info) -> str:
        error_type, error, tb = exc_info
        traceback.format_exception(error)
        result = ''.join([colorfulText(line, TextColor.RED) if self.colorful_log else line for line in traceback.format_exception(error)])
        return result


class LoggerPro(logging.Logger):
    def __init__(self,
        name: str,
        log_level: Literal['notset', 'debug', 'info', 'warn', 'warning', 'error', 'fatal', 'critical'] = 'notset',
        to_console: bool = True,
        log_file: Optional[str] = None,
        colorful_log: bool = True
    ):
        super().__init__(name)
        self.log_level = log_level
        self.to_console = to_console
        self.log_file = log_file
        self.colorful_log = colorful_log

    @staticmethod
    def improve_self(self,
                     log_level: Literal[
                         'notset', 'debug', 'info', 'warn', 'warning', 'error', 'fatal', 'critical'] = 'notset',
                     to_console: bool = True,
                     log_file: Optional[str] = None,
                     colorful_log: bool = True
    ):
        self.log_level = log_level
        self.to_console = to_console
        self.log_file = log_file
        self.colorful_log = colorful_log


def get_logger(
        name: str,
        log_level: Literal['notset', 'debug', 'info', 'warn', 'warning', 'error', 'fatal', 'critical'] = 'notset',
        to_console: bool = True,
        log_file: Optional[str] = None,
        colorful_log: bool = True
) -> logging.Logger:
    """
    获取配置好的日志器
    :param name: 日志器名称
    :param log_level: 是否是否调试模式（DEBUG级别）
    :param to_console: 是否记录日志到控制台
    :param log_file: 日志文件路径
    :param colorful_log: 彩色输出
    """
    log_level = log_level_mapping.get(log_level, logging.INFO)

    logger = logging.getLogger(name)
    logger.__class__ = LoggerPro

    LoggerPro.improve_self(logger, log_level, to_console, log_file, colorful_log)

    logger.setLevel(log_level)

    if to_console:
        console_handler = ConsoleHandler(name, log_level, colorful_log)
        logger.addHandler(console_handler)

    # 文件处理器（可选）
    if log_file:
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)  # 文件始终记录详细日志
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def main():
    logger = get_logger('test', log_level='debug', log_file="../logs/log.txt")
    logger.info('hello world')
    logger.debug('debug')
    logger.warning('warning')
    logger.error('error')
    logger.critical('critical')


if __name__ == '__main__':
    main()