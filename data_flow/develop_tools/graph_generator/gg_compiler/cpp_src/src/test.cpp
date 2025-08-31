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
    // ʹ�ÿ��ַ�������
    std::wifstream file(filename);
    if (!file.is_open()) {
        std::cerr << "�޷����ļ�: " << filename << std::endl;
        return false;
    }

    // ���ñ��ػ���֧��UTF-8
    file.imbue(std::locale(file.getloc(),
               new std::codecvt_utf8<wchar_t, 0x10ffff, std::consume_header>));

    std::wstring line;
    int line_num = 1;

    // ���ж�ȡ�ļ�
    while (std::getline(file, line)) {
        // ת��ΪUTF-8խ�ַ����Ա����
        std::string utf8_line = wstring_to_utf8(line);
        std::cout << "�� " << line_num << ": " << utf8_line << std::endl;
        line_num++;
    }

    // ����ȡ�������Ƿ�������
    if (file.bad()) {
        std::cerr << "��ȡ�ļ�ʱ��������" << std::endl;
        return false;
    }

    file.close();
    return true;
}
