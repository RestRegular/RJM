//
// Created by RestRegular on 2025/8/25.
//

#ifndef GG_GG_PARSER_H
#define GG_GG_PARSER_H

#include <memory>
#include <queue>

#include "../gg_core.h"
#include "../components/gg_manager.h"

namespace parser
{

    class Parser
    {
        static manager::ComponentManager _componentManager;
        static manager::BaseDataManager _dataManager;
        std::deque<std::shared_ptr<core::Token>> _tokens;
        std::shared_ptr<core::Token> _currentToken = nullptr;
        std::shared_ptr<core::Token> _nextToken = nullptr;

        std::string _rawHeadCode{};
        std::string _rawFootCode{};

        void nextOne();
        void consumeNextNewLineAndNextOne();
        void nextAndConsumeNextNewLine();
        void consumeCurrentNewLineAndNextOne();
        void nextAndConsumeCurrentNewLine();
        void peekOne();
        std::shared_ptr<core::Token> currentToken() const;
        std::shared_ptr<core::Token> nextToken() const;
        std::shared_ptr<core::Token> peekToken() const;
        core::TokenType currentTokenType() const;
        core::TokenType nextTokenType() const;
        core::TokenType peekTokenType() const;
        bool hasNext() const;
        bool hasPeek() const;
        bool nextIs(const core::TokenType &type) const;
        bool peekIs(const core::TokenType &type) const;
        bool currentIs(const core::TokenType &type) const;
        bool nextIsNot(const core::TokenType &type) const;
        bool peekIsNot(const core::TokenType &type) const;
        bool currentIsNot(const core::TokenType &type) const;

        void consumeNextNewLine();
        void consumeCurrentNewLine();
        std::deque<std::shared_ptr<core::Token>> extractRangerTokens();

    public:
        explicit Parser(const std::deque<std::shared_ptr<core::Token>>& tokens);

        manager::ComponentManager getComponentManager() const;

        manager::BaseDataManager getDataManager() const;

        std::string generateGraphCode() const;

        std::string buildGraphCode() const;

        void parseAsProgram();

        std::shared_ptr<model::BaseValue> extractBaseValue();

        void parseAsIdentDefinition();

        std::string parseAsHeadRawString();

        std::string parseAsFootRawString();

        void parseAsPortModel();

        void parseAsNodeModel();

        static std::pair<std::shared_ptr<model::GGNode>, std::shared_ptr<model::GGPort>> parseNodeAndPort(
            const manager::BaseDataManager& configManager, const std::string& prefix);

        void parseAsEdgeModel();

        void parseAsGraphModel();

        manager::BaseDataManager parseAsConfigScope();

        std::shared_ptr<model::BaseValue> parseAsListScope();

        static std::string tokenDequeToString(std::deque<std::shared_ptr<core::Token>> tokens);
    };

}

#endif //GG_GG_PARSER_H