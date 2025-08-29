#include "../include/analyzer/rcc_lexer.h"
#include "../include/parser/gg_parser.h"
#include "../include/test.h"

void test_parser()
{
    auto lexer = lexer::Lexer(R"(D:\ClionProjects\gg\test\new_graph.gg)", "");
    try
    {
        const auto &tokens = lexer.tokenize();
        auto parser = parser::Parser(tokens);
        parser.parseAsProgram();
        std::string result{};
        for (const auto &[id, component]: parser.getComponentManager())
        {
            result.append(component->toString()).append("\n");
        }
        utils::writeFile(R"(D:\ClionProjects\gg\test\new_graph_result.txt)", result);
        utils::writeFile(R"(D:\repositories\Resume-JobMatcher\test\test_system\new_graph_generate_result.py)", parser.generateGraphCode());
        utils::writeFile(R"(D:\repositories\Resume-JobMatcher\test\test_system\new_graph_build_result.py)", parser.buildGraphCode());
    } catch (std::exception &e)
    {
        std::cout << e.what() << std::endl;
    }
}

void test()
{
    // 设置标准输出为UTF-8编码（在支持的环境中）
    std::cout.imbue(std::locale(std::cout.getloc(),
                   new std::codecvt_utf8<wchar_t>));

    // 读取UTF-8编码的文件
    if (const std::string &filename = R"(D:\ClionProjects\gg\test\new_graph_utf8.gg)";
        read_utf8_file(filename)) {
        std::cout << "\n文件读取完成" << std::endl;
    }
}

int main()
{
    // test();
    test_parser();
}
