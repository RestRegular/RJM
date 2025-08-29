//
// Created by RestRegular on 2025/6/28.
//

#include <utility>
#include "../include/gg_base.h"
#include "../include/gg_core.h"

#include <chrono>

namespace core {

    Token::Token(): type(TokenType::TOKEN_UNKNOWN) {}

    Token::Token(const utils::Pos &pos, std::string content)
            : pos(pos), value(std::move(content)), type(parseType()) {}

    utils::Pos Token::getPos() const {
        return pos;
    }

    const std::string &Token::getValue() const {
        return value;
    }

    static std::unordered_map<std::string, TokenType> tokenMap = {
            {"true", TokenType::TOKEN_TRUE},
            {"false", TokenType::TOKEN_FALSE},
            {"null", TokenType::TOKEN_NULL},
            {"super", TokenType::TOKEN_SUPER},
            {"class", TokenType::TOKEN_CLASS},
            {"fun", TokenType::TOKEN_FUNCTION},
            {"if", TokenType::TOKEN_IF},
            {"else", TokenType::TOKEN_ELSE},
            {"elif", TokenType::TOKEN_ELIF},
            {"for", TokenType::TOKEN_FOR},
            {"while", TokenType::TOKEN_WHILE},
            {"return", TokenType::TOKEN_RETURN},
            {"break", TokenType::TOKEN_BREAK},
            {"continue", TokenType::TOKEN_CONTINUE},
            {"try", TokenType::TOKEN_TRY},
            {"repeat", TokenType::TOKEN_REPEAT},
            {"until", TokenType::TOKEN_UNTIL},
            {"iter", TokenType::TOKEN_ITER},
            {"ctor", TokenType::TOKEN_CTOR},
            {"rasm", TokenType::TOKEN_RASM},
            {"pass", TokenType::TOKEN_PASS},
            {"link", TokenType::TOKEN_LINK},
            {"var", TokenType::TOKEN_VAR},
            {"encapsulated", TokenType::TOKEN_ENCAPSULATED},
            {"ret", TokenType::TOKEN_RETURN},
            {"+", TokenType::TOKEN_PLUS},
            {"-", TokenType::TOKEN_MINUS},
            {"*", TokenType::TOKEN_STAR},
            {"/", TokenType::TOKEN_SLASH},
            {"%", TokenType::TOKEN_MODULO},
            {"&", TokenType::TOKEN_BIT_AND},
            {"|", TokenType::TOKEN_BIT_OR},
            {"^", TokenType::TOKEN_BIT_XOR},
            {"<<", TokenType::TOKEN_BIT_LEFT_SHIFT},
            {">>", TokenType::TOKEN_BIT_RIGHT_SHIFT},
            {"==", TokenType::TOKEN_EQUAL},
            {"!=", TokenType::TOKEN_NOT_EQUAL},
            {">", TokenType::TOKEN_GREATER},
            {">=", TokenType::TOKEN_GREATER_EQUAL},
            {"<", TokenType::TOKEN_LESS},
            {"<=", TokenType::TOKEN_LESS_EQUAL},
            {"&&", TokenType::TOKEN_AND},
            {"||", TokenType::TOKEN_OR},
            {"=", TokenType::TOKEN_ASSIGN},
            {":", TokenType::TOKEN_COLON},
            {"?", TokenType::TOKEN_QUESTION},
            {"::", TokenType::TOKEN_DOUBLE_COLON},
            {"**", TokenType::TOKEN_DOUBLE_STAR},
            {"++", TokenType::TOKEN_DOUBLE_PLUS},
            {"--", TokenType::TOKEN_DOUBLE_MINUS},
            {"~", TokenType::TOKEN_TILDE},
            {"@", TokenType::TOKEN_AT},
            {"&=", TokenType::TOKEN_BIT_AND_ASSIGN},
            {"|=", TokenType::TOKEN_BIT_OR_ASSIGN},
            {"^=", TokenType::TOKEN_BIT_XOR_ASSIGN},
            {"+=", TokenType::TOKEN_PLUS_ASSIGN},
            {"-=", TokenType::TOKEN_MINUS_ASSIGN},
            {"*=", TokenType::TOKEN_STAR_ASSIGN},
            {"/=", TokenType::TOKEN_SLASH_ASSIGN},
            {"%=", TokenType::TOKEN_MODULO_ASSIGN},
            {".", TokenType::TOKEN_DOT},
            {"(", TokenType::TOKEN_LPAREN},
            {")", TokenType::TOKEN_RPAREN},
            {"[", TokenType::TOKEN_LBRACKET},
            {"]", TokenType::TOKEN_RBRACKET},
            {"{", TokenType::TOKEN_LBRACE},
            {"}", TokenType::TOKEN_RBRACE},
            {"!", TokenType::TOKEN_NOT},
            {",", TokenType::TOKEN_COMMA},
            {"->", TokenType::TOKEN_INDICATOR},
            {"\n", TokenType::TOKEN_NEWLINE},
            {"`", TokenType::TOKEN_RAW_QUOTE},
            {"#", TokenType::TOKEN_HASH},
            {";", TokenType::TOKEN_COLON},
            {"\\", TokenType::TOKEN_ESCAPE}
    };

    TokenType Token::getType() const {
        return type;
    }

    TokenType Token::parseType() const {
        // �����ֵ���
        if (value.empty()) {
            return TokenType::TOKEN_UNDEFINED;
        }
        if (value == GG_TOKEN_STREAM_END) {
            return TokenType::TOKEN_STREAM_END;
        }
        if (value == GG_TOKEN_STREAM_START) {
            return TokenType::TOKEN_STREAM_START;
        }
        // ���ȼ�������ʽ���ַ����ַ��������֣�
        if (utils::StringManager::isCharFormat(value)) {
            return TokenType::TOKEN_CHAR;
        }
        if (utils::StringManager::isStringFormat(value)) {
            return TokenType::TOKEN_STRING;
        }
        if (value.starts_with("`") && value.ends_with("`"))
        {
            return TokenType::TOKEN_RAW_STRING;
        }
        if (utils::isNumber(value)) {
            const auto& num = utils::stringToNumber(value);
            if (num.type == utils::NumType::int_type) {
                return TokenType::TOKEN_INTEGER;
            }
            if (num.type == utils::NumType::double_type) {
                return TokenType::TOKEN_FLOAT;
            }
            return TokenType::TOKEN_UNDEFINED;
        }
        // ��ӳ����в��ң��ҵ��򷵻ض�Ӧ���ͣ����򷵻ر�ʶ����δ����
        if (const auto it = tokenMap.find(value); it != tokenMap.end()) {
            return it->second;
        }
        // ����Ƿ��ǹؼ���
        if (base::KEYWORDS.contains(value)) {
            return TokenType::TOKEN_KEYWORD;
        }
        // ����Ƿ��Ƿָ���
        if (base::DELIMITERS.contains(value)) {
            return TokenType::TOKEN_DELIMITER;
        }
        // ����Ƿ��Ƿ�Χ��
        if (base::RANGERS.contains(value)) {
            return TokenType::TOKEN_RANGER;
        }
        // ����Ƿ��ǲ�����
        if (base::OPERATORS.contains(value)) {
            return TokenType::TOKEN_OPERATOR;
        }
        // ����Ƿ��Ǳ�ǩ
        if (base::DESCRIBE_LABELS.contains(value)){
            return TokenType::TOKEN_LABEL;
        }
        // ��δƥ�䵽�ؼ��ֻ���ţ������Ƿ��ǺϷ���ʶ��
        if (utils::isValidIdentifier(value)) {
            return TokenType::TOKEN_IDENTIFIER;
        }
        return TokenType::TOKEN_UNDEFINED;
    }

    std::string Token::toString() const {
        return "[Token(" + getTokenTypeName(type) + "): \"" + utils::StringManager::escape(value) + "\" at " +
               pos.getFilepath() + ":" + std::to_string(pos.getLine()) + ":" + std::to_string(pos.getColumn()) + "]";
    }

    std::string Token::briefString() const {
        return "[<" + getTokenTypeName(type) + "> \"" + utils::StringManager::escape(value) + "\" (" +
               pos.briefString() + ")]";
    }

    std::string Token::professionalString() const {
        return "Token{type=" + getTokenTypeName(type) + ", value=\"" + utils::StringManager::escape(value) +
               "\", pos=" + pos.professionalString() + "}";
    }

    void Token::setValue(std::string value_) {
        this->value = std::move(value_);
    }

    std::string Token::getPosString() const {
        return pos.toString();
    }

    void Token::setPos(utils::Pos pos_) {
        this->pos = std::move(pos_);
    }

    void Token::setType(const TokenType type_) {
        this->type = type_;
    }

    std::string Token::formatString(const size_t& indent, const size_t& level) const {
        return utils::spaceString(indent * level) + "Token{\n"
        + utils::spaceString(indent * (level + 1)) + "type=" + getTokenTypeName(type) + ",\n"
        + utils::spaceString(indent * (level + 1)) + "value=\"" + utils::StringManager::escape(value) + "\",\n"
        + utils::spaceString(indent * (level + 1)) + "pos={\n" + pos.formatString(indent, level + 2) + "\n"
        + utils::spaceString(indent * (level + 1)) + "}\n"
        + utils::spaceString(indent * level) + "}";
    }

    void Token::acceptRJsonBuilder(rjson::rj::RJsonBuilder &builder) const {
        builder.insertObject("mainToken", {
            {"type", getTokenTypeName(type)},
            {"value", value},
            {"pos", getPosString()}
        });
    }

    rjson::RJValue Token::toRJson() const {
        return rjson::rj::objectRJV({
            {"mainToken", rjson::rj::objectRJV({
                {"type", getTokenTypeName(type)},
                {"value", value},
                {"pos", getPosString()}
            })}
        });
    }

    rjson::RJPair Token::toRJPair() const {
        return {"mainToken", rjson::rj::objectRJV({
                {"type", getTokenTypeName(type)},
                {"value", value},
                {"pos", getPosString()}
        })};
    }

    rjson::RJValue Token::toRJValue() const {
        return rjson::rj::objectRJV({
                {"type", getTokenTypeName(type)},
                {"value", value},
                {"pos", getPosString()}
        });
    }

    bool Token::isLiteral() const
    {
        return type == TokenType::TOKEN_STRING || type == TokenType::TOKEN_INTEGER || type == TokenType::TOKEN_FLOAT ||
            type == TokenType::TOKEN_TRUE || type == TokenType::TOKEN_FALSE || type == TokenType::TOKEN_NULL ||
                type == TokenType::TOKEN_RAW_STRING;
    }

    bool Token::isIdentifier() const
    {
        return type == TokenType::TOKEN_IDENTIFIER;
    }


    std::string getTokenTypeName(const TokenType &type) {
        switch(type)
        {
        case TokenType::TOKEN_AND: return "TOKEN_AND";
        case TokenType::TOKEN_ASSIGN: return "TOKEN_ASSIGN";
        case TokenType::TOKEN_AT: return "TOKEN_AT";
        case TokenType::TOKEN_BIT_AND: return "TOKEN_BIT_AND";
        case TokenType::TOKEN_BIT_AND_ASSIGN: return "TOKEN_BIT_AND_ASSIGN";
        case TokenType::TOKEN_BIT_LEFT_SHIFT: return "TOKEN_BIT_LEFT_SHIFT";
        case TokenType::TOKEN_BIT_OR: return "TOKEN_BIT_OR";
        case TokenType::TOKEN_BIT_OR_ASSIGN: return "TOKEN_BIT_OR_ASSIGN";
        case TokenType::TOKEN_BIT_RIGHT_SHIFT: return "TOKEN_BIT_RIGHT_SHIFT";
        case TokenType::TOKEN_BIT_XOR: return "TOKEN_BIT_XOR";
        case TokenType::TOKEN_BIT_XOR_ASSIGN: return "TOKEN_BIT_XOR_ASSIGN";
        case TokenType::TOKEN_BREAK: return "TOKEN_BREAK";
        case TokenType::TOKEN_CHAR: return "TOKEN_CHAR";
        case TokenType::TOKEN_CLASS: return "TOKEN_CLASS";
        case TokenType::TOKEN_COLON: return "TOKEN_COLON";
        case TokenType::TOKEN_CONTINUE: return "TOKEN_CONTINUE";
        case TokenType::TOKEN_DOUBLE_COLON: return "TOKEN_DOUBLE_COLON";
        case TokenType::TOKEN_DOUBLE_MINUS: return "TOKEN_DOUBLE_MINUS";
        case TokenType::TOKEN_DOUBLE_PLUS: return "TOKEN_DOUBLE_PLUS";
        case TokenType::TOKEN_DOUBLE_STAR: return "TOKEN_DOUBLE_STAR";
        case TokenType::TOKEN_DOT: return "TOKEN_DOT";
        case TokenType::TOKEN_EQUAL: return "TOKEN_EQUAL";
        case TokenType::TOKEN_FALSE: return "TOKEN_FALSE";
        case TokenType::TOKEN_FOR: return "TOKEN_FOR";
        case TokenType::TOKEN_FUNCTION: return "TOKEN_FUNCTION";
        case TokenType::TOKEN_GREATER: return "TOKEN_GREATER";
        case TokenType::TOKEN_GREATER_EQUAL: return "TOKEN_GREATER_EQUAL";
        case TokenType::TOKEN_IDENTIFIER: return "TOKEN_IDENTIFIER";
        case TokenType::TOKEN_IF: return "TOKEN_IF";
        case TokenType::TOKEN_INTEGER: return "TOKEN_INTEGER";
        case TokenType::TOKEN_KEYWORD: return "TOKEN_KEYWORD";
        case TokenType::TOKEN_LABEL: return "TOKEN_LABEL";
        case TokenType::TOKEN_LBRACE: return "TOKEN_LBRACE";
        case TokenType::TOKEN_LBRACKET: return "TOKEN_LBRACKET";
        case TokenType::TOKEN_LPAREN: return "TOKEN_LPAREN";
        case TokenType::TOKEN_LESS: return "TOKEN_LESS";
        case TokenType::TOKEN_LESS_EQUAL: return "TOKEN_LESS_EQUAL";
        case TokenType::TOKEN_MINUS: return "TOKEN_MINUS";
        case TokenType::TOKEN_MINUS_ASSIGN: return "TOKEN_MINUS_ASSIGN";
        case TokenType::TOKEN_MODULO: return "TOKEN_MODULO";
        case TokenType::TOKEN_MODULO_ASSIGN: return "TOKEN_MODULO_ASSIGN";
        case TokenType::TOKEN_NOT: return "TOKEN_NOT";
        case TokenType::TOKEN_NOT_EQUAL: return "TOKEN_NOT_EQUAL";
        case TokenType::TOKEN_NULL: return "TOKEN_NULL";
        case TokenType::TOKEN_OR: return "TOKEN_OR";
        case TokenType::TOKEN_PLUS: return "TOKEN_PLUS";
        case TokenType::TOKEN_PLUS_ASSIGN: return "TOKEN_PLUS_ASSIGN";
        case TokenType::TOKEN_QUESTION: return "TOKEN_QUESTION";
        case TokenType::TOKEN_RETURN: return "TOKEN_RETURN";
        case TokenType::TOKEN_SLASH: return "TOKEN_SLASH";
        case TokenType::TOKEN_SLASH_ASSIGN: return "TOKEN_SLASH_ASSIGN";
        case TokenType::TOKEN_STRING: return "TOKEN_STRING";
        case TokenType::TOKEN_STAR: return "TOKEN_STAR";
        case TokenType::TOKEN_STAR_ASSIGN: return "TOKEN_STAR_ASSIGN";
        case TokenType::TOKEN_SUPER: return "TOKEN_SUPER";
        case TokenType::TOKEN_TRY: return "TOKEN_TRY";
        case TokenType::TOKEN_TRUE: return "TOKEN_TRUE";
        case TokenType::TOKEN_TILDE: return "TOKEN_TILDE";
        case TokenType::TOKEN_UNDEFINED: return "TOKEN_UNDEFINED";
        case TokenType::TOKEN_UNKNOWN: return "TOKEN_UNKNOWN";
        case TokenType::TOKEN_WHILE: return "TOKEN_WHILE";
        case TokenType::TOKEN_CTOR: return "TOKEN_CTOR";
        case TokenType::TOKEN_DELIMITER: return "TOKEN_DELIMITER";
        case TokenType::TOKEN_ENCAPSULATED: return "TOKEN_ENCAPSULATED";
        case TokenType::TOKEN_LINK: return "TOKEN_LINK";
        case TokenType::TOKEN_OPERATOR: return "TOKEN_OPERATOR";
        case TokenType::TOKEN_PASS: return "TOKEN_PASS";
        case TokenType::TOKEN_REPEAT: return "TOKEN_REPEAT";
        case TokenType::TOKEN_UNTIL: return "TOKEN_UNTIL";
        case TokenType::TOKEN_ITER: return "TOKEN_ITER";
        case TokenType::TOKEN_RASM: return "TOKEN_RASM";
        case TokenType::TOKEN_VAR: return "TOKEN_VAR";
        case TokenType::TOKEN_RANGER: return "TOKEN_RANGER";
        case TokenType::TOKEN_RPAREN: return "TOKEN_RPAREN";
        case TokenType::TOKEN_RBRACKET: return "TOKEN_RBRACKET";
        case TokenType::TOKEN_RBRACE: return "TOKEN_RBRACE";
        case TokenType::TOKEN_NEWLINE: return "TOKEN_NEWLINE";
        case TokenType::TOKEN_STREAM_END: return "TOKEN_STREAM_END";
        case TokenType::TOKEN_STREAM_START: return "TOKEN_STREAM_START";
        case TokenType::TOKEN_PROGRAM: return "TOKEN_PROGRAM";
        case TokenType::TOKEN_FLOAT: return "TOKEN_FLOAT";
        case TokenType::TOKEN_BOOL: return "TOKEN_BOOL";
        case TokenType::TOKEN_COMMA: return "TOKEN_COMMA";
        case TokenType::TOKEN_ELSE: return "TOKEN_ELSE";
        case TokenType::TOKEN_ELIF: return "TOKEN_ELIF";
        case TokenType::TOKEN_INDICATOR: return "TOKEN_INDICATOR";
        case TokenType::TOKEN_RAW_QUOTE: return "TOKEN_RAW_QUOTE";
        case TokenType::TOKEN_RAW_STRING: return "TOKEN_RAW_STRING";
        case TokenType::TOKEN_HASH: return "TOKEN_HASH";
        case TokenType::TOKEN_ESCAPE: return "TOKEN_ESCAPE";
        default: return "TOKEN_UNKNOWN";
        }
    }
}
