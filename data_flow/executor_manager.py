from typing import List, Any, Dict


class ExecutorManager:
    """执行器管理器"""
    _executor_map: Dict = None

    def __init__(self):
        if not self._executor_map:
            self._executor_map = self.scan_executors()




def main():
    executor_manager = ExecutorManager()
    pass

if __name__ == '__main__':
    main()

