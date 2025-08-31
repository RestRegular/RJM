//
// Created by RestRegular on 2025/8/26.
//

#include "../include/test.h"

std::string wstring_to_utf8(const std::wstring& wstr)
{
    std::wstring_convert<std::codecvt_utf8<wchar_t>> conv;
    return conv.to_bytes(wstr);
}

bool read_utf8_file(const std::string& filename) {
    // 使用宽字符输入流
    std::wifstream file(filename);
    if (!file.is_open()) {
        std::cerr << "无法打开文件: " << filename << std::endl;
        return false;
    }

    // 设置本地化以支持UTF-8
    file.imbue(std::locale(file.getloc(),
               new std::codecvt_utf8<wchar_t, 0x10ffff, std::consume_header>));

    std::wstring line;
    int line_num = 1;

    // 逐行读取文件
    while (std::getline(file, line)) {
        // 转换为UTF-8窄字符串以便输出
        std::string utf8_line = wstring_to_utf8(line);
        std::cout << "行 " << line_num << ": " << utf8_line << std::endl;
        line_num++;
    }

    // 检查读取过程中是否发生错误
    if (file.bad()) {
        std::cerr << "读取文件时发生错误" << std::endl;
        return false;
    }

    file.close();
    return true;
}
