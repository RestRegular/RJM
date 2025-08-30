import os
import platform
import subprocess
import sys
from typing import List, Optional

from newrcc.CConsole import colorfulText
from newrcc.CColor import TextColor, Decoration, RESET

# 路径和变量配置
CMAKE_PATH = r"D:\soft\Clion\CLion-2024.2.3\bin\cmake\win\x64\bin\cmake.exe"
NINJA_PATH = r"D:/soft/Clion/Clion-2024.2.3/bin/ninja/win/x64/ninja.exe"

PROJECT_TARGET = "gg"
PROJECT_DIR = os.path.dirname(__file__)
BUILD_DIR = os.path.join(PROJECT_DIR, "cmake-build-debug")
EXECUTABLE_PATH = os.path.join(BUILD_DIR, f"{PROJECT_TARGET}.exe")
COMMAND_OUTPUT_PREFIX = ">>>"


def log(
        msg: str,
        color: str = TextColor.CYAN,
        decorations: Optional[List[str]] = None,
        end: str = "\n",
        start: str = ""
):
    """打印带颜色的日志信息"""
    print(f"{start}{COMMAND_OUTPUT_PREFIX} {colorfulText(msg, color, decorations=decorations)}", end=end)


def error(
        msg: str,
        exit_code: int = 1,
        start: str = "",
        decorations: Optional[List[str]] = None,
        end: str = "\n"
):
    """打印错误信息并退出程序"""
    print(f"{start}{COMMAND_OUTPUT_PREFIX} {colorfulText(msg, TextColor.RED, decorations=decorations)}", end=end)
    sys.exit(exit_code)


def run_command(command: List[str], capture_output: bool = False) -> Optional[subprocess.CompletedProcess]:
    """执行外部命令并处理可能的错误"""
    try:
        log(f"执行命令: {' '.join(command)}", TextColor.GRAY)
        result = subprocess.run(
            command,
            check=True,
            capture_output=capture_output,
            text=True,
            cwd=PROJECT_DIR  # 在项目目录执行命令
        )
        return result
    except subprocess.CalledProcessError as e:
        error(f"命令执行失败: {e.stderr or '未知错误'}")
    except Exception as e:
        error(f"执行命令时发生错误: {str(e)}")
    return None


def initialize_build_directory():
    """初始化构建目录，如果不存在则创建并运行CMake配置"""
    if not os.path.exists(BUILD_DIR):
        log(f"构建目录不存在，创建目录: {BUILD_DIR}")
        os.makedirs(BUILD_DIR, exist_ok=True)

        log("运行CMake配置...")
        cmake_config_cmd = [
            CMAKE_PATH,
            "-G", "Ninja",
            "-DCMAKE_BUILD_TYPE=Debug",
            f"-DCMAKE_MAKE_PROGRAM={NINJA_PATH}",
            "-S", ".",
            "-B", BUILD_DIR
        ]
        try:
            run_command(cmake_config_cmd)
        except Exception:
            os.rmdir(BUILD_DIR, ignore_errors=True)
            raise
    else:
        log(f"使用现有构建目录: {BUILD_DIR}")


def build_project():
    """构建项目"""
    log("开始构建项目...")
    build_cmd = [
        CMAKE_PATH,
        "--build", BUILD_DIR,
        "--target", PROJECT_TARGET,
        "-j", "10"
    ]
    result = run_command(build_cmd, capture_output=True)
    if result and result.stdout:
        log("构建输出:", TextColor.GREEN)
        print(result.stdout)


def run_application():
    """运行生成的可执行文件"""
    if not os.path.exists(EXECUTABLE_PATH):
        error(f"可执行文件不存在: {EXECUTABLE_PATH}")

    args = sys.argv[1:]
    if len(args) == 0:
        log("请输入参数:", TextColor.YELLOW)
        args = [arg.strip(' ') for arg in input(f"<<< {TextColor.YELLOW}").split(' ') if len(arg) > 0]
        print(end=RESET)

    log(f"运行应用程序: {EXECUTABLE_PATH} {' '.join(args)}")

    # 运行主程序
    exe_cmd = [EXECUTABLE_PATH, *args]
    run_command(exe_cmd)


def clear_screen():
    """清屏操作，跨平台支持"""
    if platform.system() == "Windows":
        os.system("cls")
    else:
        os.system("clear")


def reset_console_color():
    """恢复控制台颜色，仅Windows系统有效"""
    if platform.system() == "Windows":
        os.system("color 07")  # 白色文本


def main():
    try:
        # 初始化控制台
        clear_screen()

        # 执行构建流程
        initialize_build_directory()
        build_project()

        # 准备运行应用
        reset_console_color()
        clear_screen()

        # 运行应用程序
        run_application()

    except Exception as e:
        error(f"程序执行失败: {str(e)}")


if __name__ == "__main__":
    main()
