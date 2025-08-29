//
// Created by RestRegular on 2025/6/28.
//

#ifndef GG_GG_BASE_H
#define GG_GG_BASE_H

#include <iostream>
#include <stack>
#include <unordered_set>
#include <unordered_map>
#include <vector>
#include <memory>
#include "lib/rcc_utils.h"

#define GG_VERSION_MAJOR "1"
#define GG_VERSION_MINOR "0"
#define GG_VERSION_PATCH "0"

#define GG_VERSION "v" GG_VERSION_MAJOR "." GG_VERSION_MINOR "." GG_VERSION_PATCH

#define GG_PROGRAM_SIGN "GG Program SIGN"

#define GG_UNDEFINED_CONST "GG-UNDEFINED"
#define GG_UNKNOWN_CONST "GG-UNKNOWN"
#define GG_NULL_CONST "GG-NULL"
#define GG_TOKEN_STREAM_START "$GG_TOKEN_STREAM_START$"
#define GG_TOKEN_STREAM_END "$GG_TOKEN_STREAM_END$"

#define GG_TRUE        "true"
#define GG_FALSE       "false"
#define GG_NULL        "null"
#define GG_REL_RE      "RE"
#define GG_REL_RNE     "RNE"
#define GG_REL_RGE     "RGE"
#define GG_REL_RG      "RG"
#define GG_REL_RLE     "RLE"
#define GG_REL_RL      "RL"
#define GG_REL_AND     "AND"
#define GG_REL_OR      "OR"

namespace base {
    // 自定义类型
    typedef std::stack<std::string> StringStack;
    typedef std::vector<std::string> StringVector;
    typedef std::unordered_set<std::string> StringSet;
    typedef std::unordered_map<std::string, std::string> StringMap;

    // 枚举类声明
    enum class ErrorType;

    // 类声明
    class GGError;
    class GGSyntaxError;

    // 函数声明
    bool containsKeyword(const std::string &str);
    std::string getErrorTypeName(const ErrorType &error_type);

    // 全局变量声明
    extern StringStack PROGRAM_WORKING_DIRECTORY_STACK; // Program working directory
    extern const StringSet KEYWORDS;
    extern const StringSet OPERATORS;
    extern const StringSet ARITHMETIC_OPERATORS;
    extern const StringSet ASSIGN_OPERATORS;
    extern const StringSet ENHANCED_ASSIGN_OPERATORS;
    extern const StringMap ENH_MAT_ORD_OPTORS;
    extern const StringSet COMPARISON_OPERATOR;
    extern const StringSet RELATIONAL_OPERATOR;
    extern const StringSet OTHER_OPERATOR;
    extern const StringSet COMMENT_OPERATOR;
    extern const std::unordered_map<std::string, std::shared_ptr<StringSet>> CLASSIFY_OPERATORS;
    extern const std::vector<std::shared_ptr<StringSet>> OPERATOR_LIST;
    extern const StringSet DELIMITERS;
    extern const StringSet RANGERS;
    extern const StringMap RANGER_MATCH;
    extern const StringSet CONSTANTS;
    extern const StringSet TYPES;
    extern const StringSet DESCRIBE_LABELS;
    extern const std::string NULL_VALUE;
    extern const std::unordered_map<std::string, int> OPERATOR_PRECEDENCE;
    extern const StringVector OPERATOR_PRECEDENCE_LIST;
    extern const StringSet GROUP_SIGNS;
    extern const StringMap RELATION_MAP;

    // 类定义
    class GGError: public std::exception, public utils::Object {
        ErrorType error_type;
        std::string error_type_name;
        std::string error_position;
        std::string error_line;
        StringVector error_info;
        StringVector repair_tips;
        std::list<std::string> trace_info;
        std::string space = std::string(4, ' ');
    public:
        [[nodiscard]] std::string getErrorTip(const std::size_t & spaceSize) const;

        [[nodiscard]] std::string toString() const override;

        [[nodiscard]] std::string getErrorTitle() const;

        [[nodiscard]] std::string getErrorInfo(const size_t &spaceSize) const;

        [[nodiscard]] std::string getErrorLine(const size_t &spaceSize) const;

        [[nodiscard]] std::string getErrorPosition(const size_t &spaceSize) const;

        void addTraceInfo(const std::string &traceInfo);

        [[nodiscard]] std::string briefString() const override;

        static std::string makeTraceInfo(
                const std::string &file_record_, const std::string &error_pos_filepath,
                const std::string &utils_getPosStrFromFilePath, const std::string &makeFileIdentiFromPath,
                const std::string &trace_info, const std::string &error_pos_str,
                const std::string &raw_code, const std::string &scope_leader_pos,
                const std::string &scope_leader_code);

        [[nodiscard]] std::list<std::string> getTraceInfo() const;

    protected:
        GGError(ErrorType error_type, std::string error_position, std::string error_line,
                 StringVector error_info, StringVector repair_tips);

        [[maybe_unused]] GGError(std::string error_type, std::string error_position, std::string error_line,
                 StringVector error_info, StringVector repair_tips);

    };

    class GGSyntaxError final : public GGError {
        GGSyntaxError(std::string error_position, std::string error_line,
                       StringVector error_info, StringVector repair_tips);
    public:
        static GGSyntaxError illegalEscapeCharError(const std::string &error_position, const std::string &error_line, const char &c);
        static GGSyntaxError illegalSymbolError(const std::string &error_position, const std::string &error_line, const std::string& c);
        static GGSyntaxError undefinedExpressionError(const std::string &error_position, const std::string &error_line);
        static GGSyntaxError unclosedQuoteError(const std::string &error_position, const std::string &unclosed_quote_sign_pos, const std::string &error_line, const char &quote_type);
        static GGSyntaxError invalidIdentifierError(const std::string &error_position, const std::string &error_line, const std::string &error_identifier);
        static GGSyntaxError illegalOperatorError(const std::string &error_position, const std::string &error_line, const std::string &illegal_operator, const std::string &expected_operator);
        static GGSyntaxError duplicateTypeLabelError(const std::string &error_position, const std::string &error_line, const std::string &type_label);
    };

    class GGParserError final : public GGError {
        GGParserError(std::string error_position, std::string error_line,
                       StringVector error_info);
    public:
        static GGParserError unexpectedTokenTypeError(const std::string &error_position, const std::string &error_line,
                                                       const std::string &unexpected_token,
                                                       const std::string &expected_token_type);
        static GGParserError expressionBuilderNotFoundError(const std::string &error_position, const std::string &fixName,
                                                             const std::string &errorType);
        static GGParserError unclosedExpressionError(const std::string &error_position, const std::string &expressionBeginToken,
                                                      const std::string &errorToken, const std::string &expectedTokenType);
        static GGParserError syntaxError(const std::string &error_position, const std::string &syntaxErrorMsg, const std::string &errorDetailMsg);
    };

    class GGCompilerError final : public GGError
    {
        GGCompilerError(std::string error_position, std::string error_line,
            StringVector error_info, StringVector repair_tips);
    public:
        static GGCompilerError typeMissmatchError(const std::string &error_position, const std::string &error_line,
                                                   const std::string& error_info, const std::string &expected_type,
                                                   const std::string &actual_type, const std::vector<std::string> &repair_tips);
        static GGCompilerError typeMissmatchError(const std::string &error_position, const std::string &error_line,
                                                   const StringVector& error_infos, const std::string &expected_type,
                                                   const std::string &actual_type, const std::vector<std::string> &repair_tips);
        static GGCompilerError compilerError(const std::string &error_position, const std::string &error_line,
                                              const std::string &rcc_error_code, const std::string& error_info);
        static GGCompilerError symbolNotFoundError(const std::string &error_position, const std::string &error_line,
            const std::string &symbol_name, const std::string &error_info, const StringVector &repair_tips);
        static GGCompilerError symbolNotFoundError(const std::string &error_position, const std::string &error_line,
            const std::string &symbol_name, const StringVector &error_infos, const StringVector &repair_tips);
        static GGCompilerError scopeError(const std::string &error_position, const std::string &error_line,
                                           const std::string &expected_scope_field, const std::string &actual_scope_field, const StringVector& error_infos,
                                           const StringVector &repair_tips);
        static GGCompilerError argumentError(const std::string &error_position, const std::string &error_line,
            const StringSet &error_infos, const StringVector &repair_tips);
        static GGCompilerError semanticError(const std::string &error_position, const std::string &error_line,
            const std::string &error_info, const StringVector &repair_tips);
        static GGCompilerError semanticError(const std::string &error_position, const std::string &error_line,
            const StringVector &error_infos, const StringVector &repair_tips);
    };

    // 枚举类定义
    enum class ErrorType {
        SYNTAX_ERROR,
        PARSER_ERROR,
        COMPILER_ERROR,
    };
}


#endif //GG_GG_BASE_H
