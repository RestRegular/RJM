import os

from app.utils.logging.formater import CustomFormatter, JSONFormatter, ConsoleFormatter


def log_format_adapter():
    type_ = os.environ.get('LOGGING_FORMAT', 'console')
    if type_ == 'console':
        return ConsoleFormatter()
    elif type_ == 'json':
        return JSONFormatter()
    else:
        return CustomFormatter()