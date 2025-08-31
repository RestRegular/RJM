//
// Created by RestRegular on 2025/8/25.
//

#include "../../include/parser/gg_parser.h"

#include <ranges>
#include <stack>

#include "../../include/gg_base.h"
#include "../../include/components/gg_model.h"

namespace parser
{
    using namespace utils;
    using namespace core;
    using namespace model;
    using namespace manager;

    ComponentManager Parser::_componentManager {};

    BaseDataManager Parser::_dataManager {};

    void Parser::nextOne()
    {
        if (_tokens.empty() ||
            (_nextToken && _nextToken->getType() == TokenType::TOKEN_STREAM_END))
        {
            throw std::runtime_error("Parser::next() called when no more tokens available.");
        }
        if (_currentToken)
        {
            _tokens.push_back(_currentToken);
        }
        _currentToken = _nextToken;
        _nextToken = _tokens.front();
        _tokens.pop_front();
    }

    void Parser::consumeNextNewLineAndNextOne()
    {
        consumeNextNewLine();
        nextOne();
    }

    void Parser::nextAndConsumeNextNewLine()
    {
        nextOne();
        consumeNextNewLine();
    }

    void Parser::consumeCurrentNewLineAndNextOne()
    {
        consumeCurrentNewLine();
        nextOne();
    }

    void Parser::nextAndConsumeCurrentNewLine()
    {
        nextOne();
        consumeCurrentNewLine();
    }

    void Parser::peekOne()
    {
        if (_tokens.empty() || _tokens.back()->getType() == TokenType::TOKEN_STREAM_START)
        {
            throw std::runtime_error("Parser::peek() called when no more tokens available.");
        }
        if (_nextToken)
        {
            _tokens.push_front(_nextToken);
        }
        _nextToken = _currentToken;
        _currentToken = _tokens.back();
        _tokens.pop_back();
    }

    std::shared_ptr<Token> Parser::currentToken() const
    {
        return _currentToken;
    }

    std::shared_ptr<Token> Parser::nextToken() const
    {
        return _nextToken;
    }

    std::shared_ptr<Token> Parser::peekToken() const
    {
        return _tokens.back();
    }

    TokenType Parser::currentTokenType() const
    {
        return currentToken()->getType();
    }

    TokenType Parser::nextTokenType() const
    {
        return nextToken()->getType();
    }

    TokenType Parser::peekTokenType() const
    {
        return peekToken()->getType();
    }

    bool Parser::hasNext() const
    {
        return _nextToken != nullptr && _nextToken->getType() != TokenType::TOKEN_STREAM_END;
    }

    bool Parser::hasPeek() const
    {
        return _tokens.empty() ? false :
        _tokens.back() != nullptr &&
        _tokens.back()->getType() != TokenType::TOKEN_STREAM_START &&
        _tokens.back()->getType() != TokenType::TOKEN_STREAM_END;
    }

    bool Parser::nextIs(const TokenType& type) const
    {
        return nextToken()->getType() == type;
    }

    bool Parser::peekIs(const TokenType& type) const
    {
        return peekToken()->getType() == type;
    }

    bool Parser::currentIs(const TokenType& type) const
    {
        return currentToken()->getType() == type;
    }

    bool Parser::nextIsNot(const TokenType& type) const
    {
        return !nextIs(type);
    }

    bool Parser::peekIsNot(const TokenType& type) const
    {
        return !peekIs(type);
    }

    bool Parser::currentIsNot(const TokenType& type) const
    {
        return !currentIs(type);
    }

    void Parser::consumeNextNewLine()
    {
        // 确保下一个 token 不是 TOKEN_NEWLINE
        while (hasNext() && nextIs(TokenType::TOKEN_NEWLINE)) nextOne();
    }

    void Parser::consumeCurrentNewLine()
    {
        while (currentIs(TokenType::TOKEN_NEWLINE)) nextOne();
    }

    std::unordered_map<TokenType, TokenType> rangerTokenMap = {
        {TokenType::TOKEN_LPAREN, TokenType::TOKEN_RPAREN},
        {TokenType::TOKEN_LBRACKET, TokenType::TOKEN_RBRACKET},
        {TokenType::TOKEN_LBRACE, TokenType::TOKEN_RBRACE},
        {TokenType::TOKEN_LESS, TokenType::TOKEN_GREATER},
        {TokenType::TOKEN_HASH, TokenType::TOKEN_COLON},
        {TokenType::TOKEN_SLASH, TokenType::TOKEN_SLASH},
        {TokenType::TOKEN_ESCAPE, TokenType::TOKEN_ESCAPE}
    };

    std::unordered_set rangerLazyMatchingStartTokenSet = {
        TokenType::TOKEN_SLASH, TokenType::TOKEN_ESCAPE, TokenType::TOKEN_HASH
    };

    std::unordered_set rangerEndTokenSet = {
        TokenType::TOKEN_RPAREN, TokenType::TOKEN_RBRACKET, TokenType::TOKEN_RBRACE,
        TokenType::TOKEN_GREATER, TokenType::TOKEN_COLON, TokenType::TOKEN_SLASH,
        TokenType::TOKEN_ESCAPE
    };

    std::deque<std::shared_ptr<Token>> Parser::extractRangerTokens()
    {
        std::deque<std::shared_ptr<Token>> result {};
        // 检查当前token是否是范围开始标记
        if (const auto &it = rangerTokenMap.find(currentTokenType());
            it != rangerTokenMap.end())
        {
            const auto &rangeEndType = it->second;
            std::stack<TokenType> typeStack {};
            typeStack.push(rangeEndType);
            while (hasNext() && !typeStack.empty())
            {
                // 移动到下一个token
                nextOne();
                // 检查当前token是否是范围结束标记
                if (rangerEndTokenSet.contains(currentTokenType()))
                {
                    if (typeStack.top() == currentTokenType())
                    {
                        // 匹配到对应的结束标记，弹出栈顶
                        typeStack.pop();
                        // 如果栈不为空，说明还在嵌套范围内，将结束标记加入结果
                        if (!typeStack.empty())
                        {
                            result.push_back(currentToken());
                        }
                        // 如果栈为空，说明已经找到最外层的结束标记
                        else
                        {
                            // 移动到下一个token并返回结果
                            if (hasNext()) nextOne();
                            break;
                        }
                    }
                    else
                    {
                        // 不匹配的结束标记，抛出错误
                        throw std::runtime_error("Parser::extractRangerTokens(): Mismatched range tokens. Expected: " +
                                               getTokenTypeName(typeStack.top()) +
                                               ", but found: " + getTokenTypeName(currentTokenType()));
                    }
                } // 检查当前token是否是另一个范围开始标记
                else if (const auto &curIt = rangerTokenMap.find(currentTokenType());
                    curIt != rangerTokenMap.end())
                {
                    // 遇到新的开始标记，将其对应的结束标记压入栈
                    typeStack.push(curIt->second);
                    result.push_back(currentToken());
                }
                else
                {
                    // 普通token，加入结果
                    result.push_back(currentToken());
                }
            }
            // 如果循环结束但栈不为空，说明有未闭合的范围标记
            if (!typeStack.empty())
            {
                throw std::runtime_error("Parser::extractRangerTokens(): Unclosed range token. Expected: " +
                                       getTokenTypeName(typeStack.top()));
            }
            result.push_front(std::make_shared<Token>(Pos::UNKNOW_POS, GG_TOKEN_STREAM_START));
            result.push_back(std::make_shared<Token>(Pos::UNKNOW_POS, GG_TOKEN_STREAM_END));
            return result;
        }
        throw std::runtime_error("Parser::extractRangerTokens() called when current token is not a ranger token.");
    }

    Parser::Parser(const std::deque<std::shared_ptr<Token>>& tokens)
        : _tokens(tokens)
    {
        nextOne();
        nextOne();
    }

    ComponentManager Parser::getComponentManager() const
    {
        return _componentManager;
    }

    BaseDataManager Parser::getDataManager() const
    {
        return _dataManager;
    }

    std::string Parser::generateGraphCode() const
    {
        std::string resultCode{
            "from typing import List\n\n"
            "from data_flow import *\n"
            "from data_flow.edge import Edge\n"
            "from data_flow.graph_executor import Graph\n"
            "from data_flow.port import Port\n\n"
        };
        if (!_rawHeadCode.empty())
        {
            resultCode.append(_rawHeadCode).append("\n\n");
        }
        resultCode.append("def generate_graphs() -> List[Graph]:\n");
        std::string graphsCode {};
        for (const auto& component : _componentManager | std::views::values)
        {
            if (component->getModelType() == ModelType::GRAPH)
            {
                resultCode.append(std::static_pointer_cast<GGGraph>(component)->generateCode());
                if (!graphsCode.empty())
                {
                    graphsCode.append(", ");
                }
                graphsCode.append(generateUniqueId(component->getName()));
            }
        }
        resultCode.append("\t""return [").append(graphsCode).append("]\n");
        if (!_rawFootCode.empty())
        {
            resultCode.append("\n\n").append(_rawFootCode).append("\n");
        }
        return resultCode;
    }

    std::string Parser::buildGraphCode() const
    {
        std::string resultCode{
            "from typing import List\n\n"
            "from data_flow import *\n"
            "from data_flow.graph import Graph\n"
            "from data_flow.graph_builder import GraphBuilder\n\n"
        };
        if (!_rawHeadCode.empty())
        {
            resultCode.append(_rawHeadCode).append("\n\n");
        }
        resultCode.append("def build_graphs() -> List[Graph]:\n");
        std::string graphsCode {};
        for (const auto& component : _componentManager | std::views::values)
        {
            if (component->getModelType() == ModelType::GRAPH)
            {
                resultCode.append(std::static_pointer_cast<GGGraph>(component)->buildCode());
                if (!graphsCode.empty())
                {
                    graphsCode.append(", ");
                }
                graphsCode.append(generateUniqueId(component->getName()));
            }
        }
        resultCode.append("\t""return [").append(graphsCode).append("]\n");
        if (!_rawFootCode.empty())
        {
            resultCode.append("\n\n").append(_rawFootCode).append("\n");
        }
        return resultCode;
    }

    void Parser::parseAsProgram()
    {
        while (hasNext())
        {
            consumeNextNewLineAndNextOne();
            if (!rangerTokenMap.contains(currentTokenType()))
            {
                throw std::runtime_error("Parser::parseAsProgram(): Invalid token. Expected: " +
                                       getTokenTypeName(TokenType::TOKEN_LPAREN) +
                                       ", " + getTokenTypeName(TokenType::TOKEN_LBRACKET) +
                                       ", " + getTokenTypeName(TokenType::TOKEN_LBRACE) +
                                       ", " + getTokenTypeName(TokenType::TOKEN_LESS) +
                                       ", but found: " + getTokenTypeName(currentTokenType()));
            }
            const auto &rangeStartType = currentTokenType();
            auto rangerTokens = extractRangerTokens();
            switch (rangeStartType)
            {
            case TokenType::TOKEN_LPAREN:
                {
                    Parser portRangerParser (rangerTokens);
                    portRangerParser.parseAsPortModel();
                } break;
            case TokenType::TOKEN_LBRACKET:
                {
                    Parser nodeRangerParser (rangerTokens);
                    nodeRangerParser.parseAsNodeModel();
                } break;
            case TokenType::TOKEN_LBRACE:
                {
                    Parser graphRangerParser (rangerTokens);
                    graphRangerParser.parseAsGraphModel();
                } break;
            case TokenType::TOKEN_LESS:
                {
                    Parser edgeRangerParser (rangerTokens);
                    edgeRangerParser.parseAsEdgeModel();
                } break;
            case TokenType::TOKEN_HASH:
                {
                    Parser identDefineParser (rangerTokens);
                    identDefineParser.parseAsIdentDefinition();
                } break;
            case TokenType::TOKEN_ESCAPE:
                {
                    Parser footRawStringParser (rangerTokens);
                    _rawFootCode += footRawStringParser.parseAsFootRawString();
                } break;
            case TokenType::TOKEN_SLASH:
                {
                    Parser headRawStringParser (rangerTokens);
                    _rawHeadCode += headRawStringParser.parseAsHeadRawString();
                } break;
            default: throw std::runtime_error("Parser::parse() called when current token is not a ranger token.");
            }
        }
    }

    std::shared_ptr<BaseValue> Parser::extractBaseValue()
    {
        std::shared_ptr<BaseValue> value = nullptr;
        if (currentToken()->isLiteral())
        {
            if (currentIs(TokenType::TOKEN_STRING))
            {
                value = std::make_shared<BaseValue>(BaseValueType::LITERAL,
                    StringManager::getInstance().unescape(currentToken()->getValue()));
            } else if (currentIs(TokenType::TOKEN_RAW_STRING))
            {
                value = std::make_shared<BaseValue>(BaseValueType::RAW_DATA,
                    currentToken()->getValue().substr(1, currentToken()->getValue().size() - 2));
            } else
            {
                value = std::make_shared<BaseValue>(BaseValueType::LITERAL, currentToken()->getValue());
            }
        } else if (currentIs(TokenType::TOKEN_IDENTIFIER) || currentIs(TokenType::TOKEN_LABEL))
        {
            value = std::make_shared<BaseValue>(BaseValueType::LITERAL, currentToken()->getValue());
        } else if (currentIs(TokenType::TOKEN_AT))
        {
            consumeNextNewLineAndNextOne();
            if (currentIsNot(TokenType::TOKEN_IDENTIFIER))
            {
                throw std::runtime_error("Parser::parseAsIdentDefinition() called when current token is not TOKEN_IDENTIFIER.");
            }
            const auto &identToken = currentToken();
            value = _dataManager.get(identToken->getValue());
            if (const auto &component = _componentManager.get(identToken->getValue());
                !value && component != nullptr)
            {
                value = std::make_shared<BaseValue>(BaseValueType::IDENTIFIER, currentToken()->getValue());
            }
            if (!value)
            {
                throw std::runtime_error("Parser::parseAsIdentDefinition() called when current token is not a literal.");
            }
        } else if (currentIs(TokenType::TOKEN_LBRACKET))
        {
            Parser listParser (extractRangerTokens());
            value = listParser.parseAsListScope();
            peekOne();
        } else
        {
            throw std::runtime_error("Parser::parseAsIdentDefinition() called when current token is not a literal.");
        }
        return value;
    }

    void Parser::parseAsIdentDefinition()
    {
        nextOne(); // 跳过 token 流的起始标记
        if (currentIsNot(TokenType::TOKEN_IDENTIFIER))
        {
            throw std::runtime_error("Parser::parseAsIdentDefinition() called when current token is not TOKEN_IDENTIFIER.");
        }
        const auto &identToken = currentToken();
        consumeNextNewLineAndNextOne();
        if (currentIsNot(TokenType::TOKEN_ASSIGN))
        {
            throw std::runtime_error("Parser::parseAsIdentDefinition() called when current token is not TOKEN_ASSIGN.");
        }
        consumeNextNewLineAndNextOne();
        const auto &value = extractBaseValue();
        _dataManager.addData(std::make_shared<IdentItem>(identToken->getValue(), value));
    }

    std::string Parser::parseAsHeadRawString()
    {
        while (hasNext())
        {
            consumeNextNewLineAndNextOne();
            if (currentIsNot(TokenType::TOKEN_RAW_STRING))
            {
                throw std::runtime_error("Parser::parseAsHeadRawString() called when current token is not TOKEN_RAW_STRING.");
            }
            _rawHeadCode += currentToken()->getValue().substr(1, currentToken()->getValue().size() - 2);
        }
        return _rawHeadCode;
    }

    std::string Parser::parseAsFootRawString()
    {
        while (hasNext())
        {
            consumeNextNewLineAndNextOne();
            if (currentIsNot(TokenType::TOKEN_RAW_STRING))
            {
                throw std::runtime_error("Parser::parseAsHeadRawString() called when current token is not TOKEN_RAW_STRING.");
            }
            _rawFootCode += currentToken()->getValue().substr(1, currentToken()->getValue().size() - 2);
        }
        return _rawFootCode;
    }

    void Parser::parseAsPortModel()
    {
        consumeNextNewLineAndNextOne();
        if (currentIsNot(TokenType::TOKEN_IDENTIFIER))
        {
            throw std::runtime_error("Parser::parseAsPortModel() called when current token is not TOKEN_IDENTIFIER.");
        }
        const auto &identToken = currentToken();
        consumeNextNewLine();
        nextOne();
        if (currentIs(TokenType::TOKEN_LBRACE))
        {
            const auto &configRangerTokens = extractRangerTokens();
            Parser configParser (configRangerTokens);
            const auto &configManager = configParser.parseAsConfigScope();
            const auto &nameValue = configManager.get("name");
            const auto &id = configManager.get("id",
                BaseValue{BaseValueType::LITERAL,
                    generateUniqueId(nameValue->getValue<std::string>())});
            const auto &dataType = configManager.get("dataType",
                BaseValue{BaseValueType::LITERAL,
                "any"});
            const auto &required = configManager.get("required",
                BaseValue{BaseValueType::LITERAL,
                "false"});
            _componentManager.addComponent(identToken->getValue(), std::make_shared<GGPort>(
                id->getStringValue(), nameValue->getStringValue(),
                GGPort::getDataTypeFromConfigValue(dataType),
                required->getStringValue() == "true"));
        }
    }

    void Parser::parseAsNodeModel()
    {
        consumeCurrentNewLineAndNextOne();
        if (currentIsNot(TokenType::TOKEN_IDENTIFIER))
        {
            throw std::runtime_error("Parser::parseAsNodeModel() - 预期当前令牌为标识符(TOKEN identifier)，但实际为其他类型");
        }
        const auto &identToken = currentToken();
        consumeCurrentNewLineAndNextOne();
        if (currentIs(TokenType::TOKEN_LBRACE))
        {
            const auto &configRangerTokens = extractRangerTokens();
            Parser configParser (configRangerTokens);
            const auto &configManager = configParser.parseAsConfigScope();
            const auto &nameValue = configManager.get("name");
            const auto &idValue = configManager.get("id",
                BaseValue{BaseValueType::LITERAL,
                    generateUniqueId(nameValue->getValue<std::string>())});
            const auto &nodeTypeValue = configManager.get("type");
            const auto &isStartValue = configManager.get("is_start", BaseValue{BaseValueType::LITERAL, "false"});
            const auto &isEndValue = configManager.get("is_end", BaseValue{BaseValueType::LITERAL, "false"});
            const auto &nodeModel = std::make_shared<GGNode>(
                idValue->getStringValue(), nameValue->getStringValue(), nodeTypeValue->getStringValue(),
                isStartValue->getStringValue() == "true", isEndValue->getStringValue() == "true");
            const auto &inputsValue = configManager.get("inputs");
            if (inputsValue->getType() != BaseValueType::LIST)
            {
                throw std::runtime_error("Parser::parseAsNodeModel() - 节点'" + nameValue->getStringValue() +
                    "'的'inputs'属性预期为列表类型，但实际为其他类型");
            }
            for (const auto &inputPortValue : inputsValue->getListValue())
            {
                if (inputPortValue->getType() == BaseValueType::IDENTIFIER)
                {
                    if (const auto &inputPort = _componentManager.get(inputPortValue->getStringValue());
                        inputPort && inputPort->getModelType() == ModelType::PORT)
                    {
                        nodeModel->addInputPort(std::static_pointer_cast<GGPort>(inputPort));
                    } else
                    {
                        throw std::runtime_error("Parser::parseAsNodeModel() - 节点'" + nameValue->getStringValue() +
                            "'的输入端口'" + inputPortValue->getStringValue() + "'不存在或不是有效的端口类型");
                    }
                } else
                {
                    throw std::runtime_error("Parser::parseAsNodeModel() - 节点'" + nameValue->getStringValue() +
                        "'的'inputs'列表中包含非标识符类型的元素");
                }
            }
            const auto &outputsValue = configManager.get("outputs");
            if (outputsValue->getType() != BaseValueType::LIST)
            {
                throw std::runtime_error("Parser::parseAsNodeModel() - 节点'" + nameValue->getStringValue() +
                    "'的'outputs'属性预期为列表类型，但实际为其他类型");
            }
            for (const auto &outputPortValue : outputsValue->getListValue())
            {
                if (outputPortValue->getType() == BaseValueType::IDENTIFIER)
                {
                    if (const auto &outputPort = _componentManager.get(outputPortValue->getStringValue());
                        outputPort && outputPort->getModelType() == ModelType::PORT)
                    {
                        nodeModel->addOutputPort(std::static_pointer_cast<GGPort>(outputPort));
                    } else
                    {
                        throw std::runtime_error("Parser::parseAsNodeModel() - 节点'" + nameValue->getStringValue() +
                            "'的输出端口'" + outputPortValue->getStringValue() + "'不存在或不是有效的端口类型");
                    }
                } else
                {
                    throw std::runtime_error("Parser::parseAsNodeModel() - 节点'" + nameValue->getStringValue() +
                        "'的'outputs'列表中包含非标识符类型的元素");
                }
            }
            if (const auto &configValue = configManager.get("config"))
            {
                nodeModel->setConfig(configValue->getStringValue());
            }
            _componentManager.addComponent(identToken->getValue(), nodeModel);
        }
        else
        {
            throw std::runtime_error("Parser::parseAsNodeModel() - 预期当前令牌为左花括号'{'，但实际为其他类型");
        }
    }

    std::pair<std::shared_ptr<GGNode>, std::shared_ptr<GGPort>>
    Parser::parseNodeAndPort(const BaseDataManager &configManager, const std::string &prefix)
    {
        const auto &nodeValue = configManager.get(prefix + "_node");
        if (!nodeValue)
        {
            throw std::runtime_error("Parser::parseAsNodeModel() - 配置项'" + prefix + "'缺少'node'属性");
        }
        const auto &nodeModel = _componentManager.get(nodeValue->getStringValue());
        if (!nodeModel || nodeModel->getModelType() != ModelType::NODE)
        {
            throw std::runtime_error("Parser::parseAsNodeModel() - 配置项'" + prefix + "'的'node'属性值'" +
                nodeValue->getStringValue() + "'不存在或不是有效的节点类型");
        }
        const auto &nodeComponent = std::static_pointer_cast<GGNode>(nodeModel);
        const auto &portValue = configManager.get(prefix + "_port");
        if (!portValue)
        {
            throw std::runtime_error("Parser::parseAsNodeModel() - 配置项'" + prefix + "'缺少'port'属性");
        }
        const auto &portModel = _componentManager.get(portValue->getStringValue());
        if (!portModel || portModel->getModelType() != ModelType::PORT)
        {
            throw std::runtime_error("Parser::parseAsNodeModel() - 配置项'" + prefix + "'的'port'属性值'" +
                portValue->getStringValue() + "'不存在或不是有效的端口类型");
        }
        const auto &portComponent = std::static_pointer_cast<GGPort>(portModel);
        return std::make_pair(nodeComponent, portComponent);
    }

    void Parser::parseAsEdgeModel()
    {
        consumeNextNewLineAndNextOne();
        if (currentIsNot(TokenType::TOKEN_IDENTIFIER))
        {
            throw std::runtime_error("Parser::parseAsNodeModel() - 预期当前令牌为标识符(TOKEN identifier)，但实际为其他类型");
        }
        const auto &identToken = currentToken();
        consumeNextNewLineAndNextOne();
        if (currentIs(TokenType::TOKEN_LBRACE))
        {
            const auto &configRangerTokens = extractRangerTokens();
            Parser configParser (configRangerTokens);
            const auto &configManager = configParser.parseAsConfigScope();
            const auto &[sourceNode, sourcePort] = parseNodeAndPort(configManager, "source");
            const auto &[targetNode, targetPort] = parseNodeAndPort(configManager, "target");

            const auto &conditionValue = configManager.get("condition");

            if (conditionValue && conditionValue->getType() != BaseValueType::RAW_DATA)
            {
                throw std::runtime_error("Parser::parseAsNodeModel() - 配置项'condition'属性值预期为字符串类型，但实际为其他类型");
            }
            const auto &enableValue = configManager.get("enable",
                BaseValue{BaseValueType::LITERAL, "true"});
            const auto &descValue = configManager.get("desc",
                BaseValue{BaseValueType::LITERAL, ""});

            const auto &edgeModel = std::make_shared<GGEdge>(
            identToken->getValue(), sourceNode, sourcePort->getId(), targetNode, targetPort->getId(),
            conditionValue ? conditionValue->getStringValue() : "None", enableValue->getStringValue() == "true", descValue->getStringValue());
            _componentManager.addComponent(identToken->getValue(), edgeModel);
        } else
        {
            throw std::runtime_error("Parser::parseAsNodeModel() - 预期当前令牌为左花括号'{'，但实际为其他类型");
        }
    }

    void Parser::parseAsGraphModel()
    {
        consumeNextNewLineAndNextOne();
        if (currentIsNot(TokenType::TOKEN_IDENTIFIER))
        {
            throw std::runtime_error("Parser::parseAsNodeModel() - 预期当前令牌为标识符(TOKEN identifier)，但实际为其他类型");
        }
        const auto &identToken = currentToken();
        consumeNextNewLineAndNextOne();
        if (!currentIs(TokenType::TOKEN_LBRACE))
        {
            throw std::runtime_error("Parser::parseAsNodeModel() - 预期当前令牌为左花括号'{'，但实际为其他类型");
        }
        const auto &configRangerTokens = extractRangerTokens();
        Parser configParser (configRangerTokens);
        const auto &configManager = configParser.parseAsConfigScope();
        const auto &nameValue = configManager.get("name",
            BaseValue{BaseValueType::LITERAL, identToken->getValue()});
        const auto &nodesValue = configManager.get("nodes");
        if (!nodesValue || nodesValue->getType() != BaseValueType::LIST)
        {
            throw std::runtime_error("Parser::parseAsNodeModel() - 配置项'nodes'属性值预期为列表类型，但实际为其他类型");
        }
        const auto &edgesValue = configManager.get("edges");
        if (!edgesValue || edgesValue->getType() != BaseValueType::LIST)
        {
            throw std::runtime_error("Parser::parseAsNodeModel() - 配置项'edges'属性值预期为列表类型，但实际为其他类型");
        }
        const auto &descValue = configManager.get("desc", BaseValue{BaseValueType::LITERAL, ""});
        const auto &graphModel = std::make_shared<GGGraph>(nameValue->getStringValue(),
            descValue->getStringValue());
        for (const auto &nodeValue : nodesValue->getListValue())
        {
            if (nodeValue->getType() != BaseValueType::IDENTIFIER)
            {
                throw std::runtime_error("Parser::parseAsNodeModel() - 配置项'nodes'属性值预期为标识符类型，但实际为其他类型");
            }
            const auto &nodeModel = _componentManager.get(nodeValue->getStringValue());
            if (!nodeModel || nodeModel->getModelType() != ModelType::NODE)
            {
                throw std::runtime_error("Parser::parseAsNodeModel() - 配置项'nodes'属性值'" + nodeValue->getStringValue() + "'不存在或不是有效的节点类型");
            }
            graphModel->addNode(std::static_pointer_cast<GGNode>(nodeModel));
        }
        for (const auto &edgeValue : edgesValue->getListValue())
        {
            if (edgeValue->getType() != BaseValueType::IDENTIFIER)
            {
                throw std::runtime_error("Parser::parseAsNodeModel() - 配置项'edges'属性值预期为标识符类型，但实际为其他类型");
            }
            const auto &edgeModel = _componentManager.get(edgeValue->getStringValue());
            if (!edgeModel || edgeModel->getModelType() != ModelType::EDGE)
            {
                throw std::runtime_error("Parser::parseAsNodeModel() - 配置项'edges'属性值'" + edgeValue->getStringValue() + "'不存在或不是有效的边类型");
            }
            graphModel->addEdge(std::static_pointer_cast<GGEdge>(edgeModel));
        }
        _componentManager.addComponent(identToken->getValue(), graphModel);
    }

    std::shared_ptr<IdentItem> processConfigItem(const std::shared_ptr<Token> &fieldToken, const std::shared_ptr<Token> &valueToken)
    {
        if (fieldToken->getType() != TokenType::TOKEN_IDENTIFIER)
        {
            throw std::runtime_error("Parser::processConfigItem() called when field token is not TOKEN_IDENTIFIER.");
        }
        const auto &fieldName = fieldToken->getValue();
        switch (valueToken->getType())
        {
        case TokenType::TOKEN_STRING: return std::make_shared<IdentItem>(fieldName, BaseValueType::LITERAL, StringManager::getInstance().unescape(valueToken->getValue()));
        case TokenType::TOKEN_FALSE:
        case TokenType::TOKEN_TRUE:
        case TokenType::TOKEN_FLOAT:
        case TokenType::TOKEN_INTEGER:
        case TokenType::TOKEN_IDENTIFIER: return std::make_shared<IdentItem>(fieldName, BaseValueType::LITERAL, valueToken->getValue());
        case TokenType::TOKEN_RAW_QUOTE: return std::make_shared<IdentItem>(fieldName, BaseValueType::RAW_DATA, valueToken->getValue());
        default: throw std::runtime_error("Parser::processConfigItem() called when value token is not a literal.");
        }
    }

    std::shared_ptr<IdentItem> processConfigItem(const std::shared_ptr<Token> &fieldToken, const std::vector<std::shared_ptr<Token>> &valueToken)
    {

        pass();
    }

    BaseDataManager Parser::parseAsConfigScope()
    {
        nextOne(); // 跳过 token 流的开始标记
        BaseDataManager configManager{};
        while (hasNext())
        {
            consumeNextNewLineAndNextOne();
            if (currentIsNot(TokenType::TOKEN_IDENTIFIER))
            {
                throw std::runtime_error("Parser::parseAsConfigScope() called when current token is not TOKEN_IDENTIFIER.");
            }
            const auto &configNameToken = currentToken();
            consumeNextNewLineAndNextOne();
            if (currentIsNot(TokenType::TOKEN_ASSIGN))
            {
                throw std::runtime_error("Parser::parseAsConfigScope() called when current token is not TOKEN_ASSIGN.");
            }
            consumeNextNewLineAndNextOne();
            if (const auto &value = extractBaseValue();
                value->getType() == BaseValueType::IDENTIFIER && hasNext())
            {
                consumeNextNewLineAndNextOne();
                if (currentIs(TokenType::TOKEN_INDICATOR))
                {
                    const auto &component = _componentManager.get(value->getStringValue());
                    if (component->getModelType() != ModelType::NODE)
                    {
                        throw std::runtime_error("Parser::parseAsIdentDefinition() called when current token is not a node.");
                    }
                    const auto &nodeComponent = std::static_pointer_cast<GGNode>(component);
                    consumeNextNewLineAndNextOne();
                    if (currentIsNot(TokenType::TOKEN_IDENTIFIER))
                    {
                        throw std::runtime_error("Parser::parseAsIdentDefinition() called when current token is not TOKEN_IDENTIFIER.");
                    }
                    const auto &portValue = currentToken()->getValue();
                    const auto &portComponent = _componentManager.get(portValue);
                    if (!portComponent)
                    {
                        throw std::runtime_error("Parser::parseAsIdentDefinition() called when current token is not a component.");
                    }
                    if (!nodeComponent->getInputPort(portComponent->getId()) && !nodeComponent->getOutputPort(portComponent->getId()))
                    {
                        throw std::runtime_error("Parser::parseAsIdentDefinition() called when current token is not a port.");
                    }
                    configManager.addData(std::make_shared<IdentItem>(configNameToken->getValue() + "_node", value));
                    configManager.addData(std::make_shared<IdentItem>(configNameToken->getValue() + "_port", std::make_shared<BaseValue>(BaseValueType::IDENTIFIER, portValue)));
                }
            } else
            {
                configManager.addData(std::make_shared<IdentItem>(configNameToken->getValue(), value));
            }
            consumeNextNewLine();
            if (hasNext())
            {
                consumeCurrentNewLineAndNextOne();
                if (currentIsNot(TokenType::TOKEN_COMMA))
                {
                    throw std::runtime_error("Parser::parseAsConfigScope() called when current token is not TOKEN_COMMA.");
                }
            }
        }
        return configManager;
    }

    std::shared_ptr<BaseValue> Parser::parseAsListScope()
    {
        std::vector<std::shared_ptr<BaseValue>> listItems {};
        while (hasNext())
        {
            consumeNextNewLineAndNextOne();
            const auto &listItemValue = extractBaseValue();
            listItems.push_back(listItemValue);
            consumeNextNewLine();
            if (hasNext())
            {
                nextOne();
                if (currentIsNot(TokenType::TOKEN_COMMA))
                {
                    throw std::runtime_error("Parser::parseAsListScope() called when current token is not TOKEN_COMMA.");
                }
            }
        }
        return std::make_shared<BaseValue>(listItems);
    }

    std::string Parser::tokenDequeToString(std::deque<std::shared_ptr<Token>> tokens)
    {
        std::string result {};
        const size_t &token_size = tokens.size();
        for (size_t i = 0; i < token_size; i++)
        {
            result.append(tokens.front()->toString()).append("\n");
            tokens.pop_front();
        }
        return result;
    }
}
