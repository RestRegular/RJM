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
    // ���ñ�׼���ΪUTF-8���루��֧�ֵĻ����У�
    std::cout.imbue(std::locale(std::cout.getloc(),
                   new std::codecvt_utf8<wchar_t>));

    // ��ȡUTF-8������ļ�
    if (const std::string &filename = R"(D:\ClionProjects\gg\test\new_graph_utf8.gg)";
        read_utf8_file(filename)) {
        std::cout << "\n�ļ���ȡ���" << std::endl;
    }
}

int main()
{
    // test();
    test_parser();
}
