#include "../include/analyzer/rcc_lexer.h"
#include "../include/parser/gg_parser.h"
#include "../include/lib/rcc_utils.h"
#include "../include/test.h"

#include <stdexcept>
#include <memory>
#include <string_view>

using namespace utils;

// 编译类型枚举，增强类型安全性
enum class CompilationType {
    Generate,
    Build
};

// 命令行参数存储
struct ProgramOptions {
    std::string targetFilePath{};
    std::string archiveFilePath{};
    std::string compilationType = "G";
};

/**
 * @brief 初始化命令行参数解析器
 * @return 配置好的参数解析器
 */
ProgArgParser initializeArgumentParser(ProgramOptions& options) {
    ProgArgParser parser{};

    parser.addOption<std::string>("target", &options.targetFilePath,
        "", "Target file path (required)",
        {"t"});

    parser.addOption<std::string>("compilation-type",
        &options.compilationType,
        "G", "Compilation type (G/B)", {"ct"});

    parser.addOption<std::string>("archive", &options.archiveFilePath,
        "", "Archive file path (required for output)", {"a"});

    return parser;
}

/**
 * @brief 验证命令行参数的有效性
 * @param options 已解析的命令行参数
 * @throws std::invalid_argument 如果参数无效
 */
void validateOptions(const ProgramOptions& options) {
    if (options.targetFilePath.empty()) {
        throw std::invalid_argument("Target file path is required (use --target or -t)");
    }

    if (options.archiveFilePath.empty()) {
        throw std::invalid_argument("Archive file path is required (use --archive or -a)");
    }

    // 可以在这里添加文件存在性检查等额外验证
}

/**
 * @brief 处理文件编译流程
 * @param options 程序配置选项
 */
void processCompilation(const ProgramOptions& options) {
    // 使用智能指针管理资源
    auto lexer = std::make_unique<lexer::Lexer>(options.targetFilePath);
    auto tokens = lexer->tokenize();

    auto parser = std::make_unique<parser::Parser>(std::move(tokens));
    parser->parseAsProgram();

    std::string graphCode;
    if (options.compilationType == "G")
    {
        graphCode = parser->generateGraphCode();
    } else if (options.compilationType == "B")
    {
        graphCode = parser->buildGraphCode();
    } else
    {
        throw std::invalid_argument("Invalid compilation type: " + options.compilationType);
    }

    // 写入文件并检查结果
    if (!writeFile(options.archiveFilePath, graphCode)) {
        throw std::runtime_error("Failed to write to archive file: " + options.archiveFilePath);
    }
}

int main(const int argc, char* argv[]) {
    try {
        ProgramOptions options;
        auto argParser = initializeArgumentParser(options);
        argParser.parse(argc, argv);

        validateOptions(options);
        processCompilation(options);

        return 0;
    }
    catch (const std::invalid_argument& e) {
        // 处理参数错误
        std::cerr << "Argument error: " + std::string(e.what()) << std::endl;
        return 1;
    }
    catch (const std::runtime_error& e) {
        // 处理运行时错误
        std::cerr << "Runtime error: " + std::string(e.what()) << std::endl;
        return 2;
    }
    catch (const std::exception &e) {
        // 处理未知错误
        std::cerr << "Exception: " << std::string(e.what()) << std::endl;
        return 3;
    }
}
