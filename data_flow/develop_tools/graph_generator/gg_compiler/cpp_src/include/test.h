//
// Created by RestRegular on 2025/8/26.
//

#ifndef GG_TEST_H
#define GG_TEST_H

#include <iostream>
#include <fstream>
#include <string>
#include <locale>
#include <codecvt>

// 转换宽字符串到UTF-8编码的窄字符串
std::string wstring_to_utf8(const std::wstring& wstr);

// 读取UTF-8编码的文件并输出内容
bool read_utf8_file(const std::string& filename);

#endif //GG_TEST_H