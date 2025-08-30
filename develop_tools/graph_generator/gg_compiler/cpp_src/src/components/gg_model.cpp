//
// Created by RestRegular on 2025/8/25.
//

#include "../../include/components/gg_model.h"

#include "../../include/gg_base.h"

#include <ranges>

namespace model
{
    std::string getBaseDataTypeName(const BaseValueType& type)
    {
        switch (type)
        {
        case BaseValueType::LITERAL: return "LITERAL";
        case BaseValueType::RAW_DATA: return "RAW_DATA";
        case BaseValueType::IDENTIFIER: return "IDENTIFIER";
        case BaseValueType::LIST: return "LIST";
        default: return "UNKNOWN";
        }
    }

    // ConfigValue 实现
    BaseValue::BaseValue(BaseValueType type, const std::string& value)
        : _type(type), _string_value(value)
    {
        if (type == BaseValueType::LIST)
        {
            throw std::invalid_argument("Use list constructor for LIST type");
        }
    }

    BaseValue::BaseValue(const std::vector<std::shared_ptr<BaseValue>>& value)
        : _type(BaseValueType::LIST), _list_value(value)
    {
    }

    BaseValueType BaseValue::getType() const
    {
        return _type;
    }

    std::string BaseValue::getStringValue() const
    {
        if (_type == BaseValueType::LIST)
        {
            throw std::invalid_argument("Cannot get string value for LIST type");
        }
        return _string_value;
    }

    const std::vector<std::shared_ptr<BaseValue>>& BaseValue::getListValue() const
    {
        if (_type != BaseValueType::LIST)
        {
            throw std::invalid_argument("Cannot get list value for non-LIST type");
        }
        return _list_value;
    }

    // ConfigItem 实现
    IdentItem::IdentItem(const std::string& key, BaseValueType type, const std::string& value)
        : _key(key), _value(std::make_shared<BaseValue>(type, value))
    {
    }

    IdentItem::IdentItem(const std::string& key, const std::vector<std::shared_ptr<BaseValue>>& value)
        : _key(key), _value(std::make_shared<BaseValue>(value))
    {
    }

    IdentItem::IdentItem(const std::string& key, std::shared_ptr<BaseValue> value)
        : _key(key), _value(std::move(value))
    {
    }

    std::string IdentItem::getKey() const
    {
        return _key;
    }

    BaseValueType IdentItem::getValueType() const
    {
        return _value->getType();
    }

    std::shared_ptr<BaseValue> IdentItem::getValue() const
    {
        return _value;
    }

    std::string IdentItem::getStringValue() const
    {
        return _value->getStringValue();
    }

    const std::vector<std::shared_ptr<BaseValue>>& IdentItem::getListValue() const
    {
        return _value->getListValue();
    }

    std::string getModelTypeName(const ModelType& type)
    {
        switch (type) {
        case ModelType::NODE: return "NODE";
        case ModelType::EDGE: return "EDGE";
        case ModelType::GRAPH: return "GRAPH";
        case ModelType::PORT: return "PORT";
        default: return "UNKNOWN";
        }
    }

    // GGBaseModel 实现
    GGBaseModel::GGBaseModel(const std::string& name, const std::string& desc)
        : m_id(utils::generateUniqueId(name)), m_name(name), m_desc(desc) {}

    GGBaseModel::GGBaseModel(const std::string& id, const std::string& name, const std::string& desc)
        : m_id(id), m_name(name), m_desc(desc) {}

    const std::string& GGBaseModel::getId() const { return m_id; }
    const std::string& GGBaseModel::getName() const { return m_name; }
    const std::string& GGBaseModel::getDesc() const { return m_desc; }

    void GGBaseModel::setName(const std::string& name) { m_name = name; }
    void GGBaseModel::setDesc(const std::string& desc) { m_desc = desc; }

    std::unordered_map<std::string, GGPort::DataType> dataTypeMap = {
        {"any", GGPort::DataType::ANY},
        {"json", GGPort::DataType::JSON},
        {"text", GGPort::DataType::TEXT},
        {"number", GGPort::DataType::NUMBER},
        {"date", GGPort::DataType::DATE},
        {"boolean", GGPort::DataType::BOOLEAN},
        {"binary", GGPort::DataType::BINARY},
        {"list", GGPort::DataType::LIST},
        {"dict", GGPort::DataType::DICT},
        {"none", GGPort::DataType::NONE},
    };

    GGPort::DataType GGPort::getDataTypeFromConfigValue(const std::shared_ptr<BaseValue>& configValue)
    {
        if (configValue->getType() != BaseValueType::LITERAL)
        {
            throw std::invalid_argument("ConfigValue type must be LITERAL");
        }
        const auto &literalValue = configValue->getStringValue();
        if (const auto it = dataTypeMap.find(literalValue); it != dataTypeMap.end())
        {
            return it->second;
        }
        throw std::invalid_argument("Invalid literal value for data type: " + literalValue);
    }

    // GGPort 实现
    GGPort::GGPort(const std::string& id, const std::string& name, const DataType dataType, const bool required)
        : GGBaseModel(id, name, ""), m_dataType(dataType), m_required(required) {}

    std::string GGPort::getDataType() const { return dataTypeToString(m_dataType); }
    bool GGPort::isRequired() const { return m_required; }

    void GGPort::setData(DataList data)
    {
        std::lock_guard<std::mutex> lock(m_dataMutex);
        m_data = std::move(data);
    }

    DataList GGPort::getData() const
    {
        std::lock_guard<std::mutex> lock(m_dataMutex);
        return m_data;
    }

    bool GGPort::validate() const
    {
        if (m_required && getData().empty())
        {
            std::cerr << "[GGPort] Validate failed: Required port '"
                << getName() << "' has no data" << std::endl;
            return false;
        }
        return true;
    }

    std::string GGPort::toString() const
    {
        const std::string dataTypeStr = dataTypeToString(m_dataType);
        return "Port{id='" + getId() + "', name='" + getName()
            + "', type=" + dataTypeStr
            + ", required=" + (m_required ? "true" : "false") + "}";
    }

    std::string GGPort::generateCode() const
    {
        return "\t" + utils::generateUniqueId(getName()) + " = Port(id=\"" + getId() + "\", name=\"" + getName() +
            "\", data_type=\"" + getDataType() + "\")\n";
    }

    std::string GGPort::buildCode(const std::string& builder) const
    {
        return "\t" + utils::generateUniqueId(getName()) + " = " + builder + ".port(\"" + getId() + "\", \"" + getName() + "\", \"" + getDataType() + "\")\n";
    }

    std::string GGPort::dataTypeToString(const DataType& type)
    {
        switch (type)
        {
        case DataType::JSON: return "JSON";
        case DataType::TEXT: return "TEXT";
        case DataType::NUMBER: return "NUMBER";
        case DataType::DATE: return "DATE";
        case DataType::BOOLEAN: return "BOOLEAN";
        case DataType::BINARY: return "BINARY";
        case DataType::LIST: return "LIST";
        case DataType::DICT: return "DICT";
        case DataType::ANY: return "ANY";
        case DataType::NONE: return "NONE";
        default: return "UNKNOWN";
        }
    }

    ModelType GGPort::getModelType() const
    {
        return ModelType::PORT;
    }

    // GGNode 实现
    GGNode::GGNode(const std::string& id, const std::string& name, const std::string& nodeType, const bool isStart,
                   const bool isEnd)
        : GGBaseModel(id, name), m_nodeType(nodeType), m_isStart(isStart), m_isEnd(isEnd) {}

    bool GGNode::addInputPort(const GGPortPtr& port)
    {
        if (!port) return false;
        const std::string& portId = port->getId();
        if (m_inputPorts.contains(portId))
        {
            std::cerr << "[GGNode] addInputPort failed: Port '"
                << portId << "' already exists" << std::endl;
            return false;
        }
        m_inputPorts[portId] = port;
        return true;
    }

    bool GGNode::addOutputPort(const GGPortPtr& port)
    {
        if (!port) return false;
        const std::string& portId = port->getId();
        if (m_outputPorts.contains(portId))
        {
            std::cerr << "[GGNode] addOutputPort failed: Port '"
                << portId << "' already exists" << std::endl;
            return false;
        }
        m_outputPorts[portId] = port;
        return true;
    }

    GGPortPtr GGNode::getInputPort(const std::string& portId) const
    {
        const auto it = m_inputPorts.find(portId);
        return (it != m_inputPorts.end()) ? it->second : nullptr;
    }

    GGPortPtr GGNode::getOutputPort(const std::string& portId) const
    {
        const auto it = m_outputPorts.find(portId);
        return (it != m_outputPorts.end()) ? it->second : nullptr;
    }

    const StringMap<GGPortPtr>& GGNode::getAllInputPorts() const { return m_inputPorts; }
    const StringMap<GGPortPtr>& GGNode::getAllOutputPorts() const { return m_outputPorts; }

    std::string GGNode::getNodeType() const { return m_nodeType; }
    bool GGNode::isStartNode() const { return m_isStart; }
    const std::string& GGNode::getConfig() const { return m_config; }

    void GGNode::setIsStartNode(const bool isStart) { m_isStart = isStart; }
    void GGNode::setConfig(const std::string& config) { m_config = config; }

    bool GGNode::validate() const
    {
        if (m_isStart)
        {
            if (m_nodeType != "input")
            {
                std::cerr << "[GGNode] Validate failed: Start node must be INPUT type" << std::endl;
                return false;
            }
            if (!m_inputPorts.empty())
            {
                std::cerr << "[GGNode] Validate failed: Start node should have no input ports" << std::endl;
                return false;
            }
        }

        for (const auto& [portId, port] : m_inputPorts)
        {
            if (!port->validate())
            {
                std::cerr << "[GGNode] Validate failed: Input port '" << portId << "' is invalid" << std::endl;
                return false;
            }
        }
        for (const auto& [portId, port] : m_outputPorts)
        {
            if (!port->validate())
            {
                std::cerr << "[GGNode] Validate failed: Output port '" << portId << "' is invalid" << std::endl;
                return false;
            }
        }

        return true;
    }

    std::string GGNode::toString() const
    {
        const std::string typeStr = "[NodeType: " + m_nodeType + "]";

        std::string inputPortsStr = "[";
        for (const auto& port : m_inputPorts | std::views::values)
        {
            inputPortsStr += port->getId() + ",";
        }
        if (!inputPortsStr.empty() && inputPortsStr.back() == ',')
            inputPortsStr.pop_back();
        inputPortsStr += "]";

        std::string outputPortsStr = "[";
        for (const auto& port : m_outputPorts | std::views::values)
        {
            outputPortsStr += port->getId() + ",";
        }
        if (!outputPortsStr.empty() && outputPortsStr.back() == ',')
            outputPortsStr.pop_back();
        outputPortsStr += "]";

        return "Node{id='" + getId() + "', name='" + getName()
            + "', type=" + typeStr
            + ", isStart=" + (m_isStart ? "true" : "false")
            + ", inputs=" + inputPortsStr
            + ", outputs=" + outputPortsStr + "}";
    }

    ModelType GGNode::getModelType() const
    {
        return ModelType::NODE;
    }

    std::string GGNode::generateCode() const
    {
        // 提取处理端口的通用逻辑
        auto processPorts = [](const auto& ports) {
            std::string code;
            std::string ids;
            for (const auto& [_, port] : ports) {  // 使用结构化绑定更清晰
                code.append(port->generateCode());

                if (!ids.empty()) {
                    ids.append(", ");  // 只在需要时添加分隔符，避免后续截断
                }
                ids.append(utils::generateUniqueId(port->getName()));
            }
            return std::make_pair(code, ids);
        };

        // 处理输入端口
        auto [inputCode, inputIds] = processPorts(m_inputPorts);
        // 处理输出端口
        auto [outputCode, outputIds] = processPorts(m_outputPorts);

        // 构建配置字符串
        const std::string configStr = getConfig().empty() ? "" : getConfig();

        // 组合最终结果
        std::string resultCode = inputCode + outputCode;
        resultCode += "\t" + utils::generateUniqueId(getName())
            + " = Node(name=\"" + getName()
            + "\", type=\"" + getNodeType()
            + "\", inputs=[" + inputIds
            + "], outputs=[" + outputIds
            + "]" + (configStr.empty() ? "" : ", config=" + configStr) + ")\n";
        return resultCode;
    }

    std::string GGNode::buildCode(const std::string& builder)
    {
        // 提取处理端口的通用逻辑
        auto processPorts = [builder](const auto& ports) {
            std::string code;
            std::string ids;
            for (const auto& port : ports | std::views::values) {  // 使用结构化绑定更清晰
                code.append(port->buildCode(builder));

                if (!ids.empty()) {
                    ids.append(", ");  // 只在需要时添加分隔符，避免后续截断
                }
                ids.append(utils::generateUniqueId(port->getName()));
            }
            return std::make_pair(code, ids);
        };
        // 处理输入端口
        auto [inputCode, inputIds] = processPorts(m_inputPorts);
        // 处理输出端口
        auto [outputCode, outputIds] = processPorts(m_outputPorts);
        const std::string configStr = getConfig().empty() ? "None" : getConfig();
        std::string resultCode = inputCode + outputCode;
        resultCode += "\t" + utils::generateUniqueId(getName())
            + " = " + builder + ".node(\"" + getName() + "\", \""
            + getNodeType() + "\", ["
            + inputIds + "], ["
            + outputIds + "], " + configStr + ", \""
            + getDesc() + "\", " + (m_isStart ? "True, " : "False, ")
            + (m_isEnd ? "True" : "False") + ")\n";
        return resultCode;
    }

    // GGEdge 实现
    GGEdge::GGEdge(const std::string& name,
                   const GGNodePtr& sourceNode, const std::string& sourcePortId,
                   const GGNodePtr& targetNode, const std::string& targetPortId,
                   const std::string& condition, const bool enable, const std::string& desc)
        : GGBaseModel(name, desc),
          m_sourceNode(sourceNode),
          m_targetNode(targetNode),
          m_sourcePortId(sourcePortId),
          m_targetPortId(targetPortId),
          m_enable(enable),
          m_condition(condition)
    {
    }

    GGNodePtr GGEdge::getSourceNode() const { return m_sourceNode.lock(); }
    GGNodePtr GGEdge::getTargetNode() const { return m_targetNode.lock(); }
    const std::string& GGEdge::getSourcePortId() const { return m_sourcePortId; }
    const std::string& GGEdge::getTargetPortId() const { return m_targetPortId; }

    GGPortPtr GGEdge::getSourcePort() const
    {
        const auto node = getSourceNode();
        return node ? node->getOutputPort(m_sourcePortId) : nullptr;
    }

    GGPortPtr GGEdge::getTargetPort() const
    {
        const auto node = getTargetNode();
        return node ? node->getInputPort(m_targetPortId) : nullptr;
    }

    bool GGEdge::isEnabled() const { return m_enable; }
    void GGEdge::setEnable(bool enable) { m_enable = enable; }

    std::string GGEdge::getCondition() const { return m_condition; }

    bool GGEdge::validate() const
    {
        auto sourceNode = getSourceNode();
        auto targetNode = getTargetNode();
        if (!sourceNode)
        {
            std::cerr << "[GGEdge] Validate failed: Source node is invalid" << std::endl;
            return false;
        }
        if (!targetNode)
        {
            std::cerr << "[GGEdge] Validate failed: Target node is invalid" << std::endl;
            return false;
        }

        auto sourcePort = getSourcePort();
        if (!sourcePort)
        {
            std::cerr << "[GGEdge] Validate failed: Source port '" << m_sourcePortId
                << "' not found in node '" << sourceNode->getName() << "'" << std::endl;
            return false;
        }

        auto targetPort = getTargetPort();
        if (!targetPort)
        {
            std::cerr << "[GGEdge] Validate failed: Target port '" << m_targetPortId
                << "' not found in node '" << targetNode->getName() << "'" << std::endl;
            return false;
        }

        if (sourcePort->getDataType() != targetPort->getDataType())
        {
            std::cerr << "[GGEdge] Validate failed: Data type mismatch - Source port '"
                << sourcePort->getName() << "' (type: " << sourcePort->getDataType()
                << "), Target port '" << targetPort->getName() << "' (type: " << targetPort->getDataType()
                << ")" << std::endl;
            return false;
        }

        return true;
    }

    std::string GGEdge::toString() const
    {
        const auto sourceNode = getSourceNode();
        const auto targetNode = getTargetNode();
        const std::string sourceNodeName = sourceNode ? sourceNode->getName() : "invalid_node";
        const std::string targetNodeName = targetNode ? targetNode->getName() : "invalid_node";

        return "Edge{id='" + getId() + "', name='" + getName()
            + "', source=" + sourceNodeName + "." + m_sourcePortId
            + ", target=" + targetNodeName + "." + m_targetPortId
            + ", enable=" + (m_enable ? "true" : "false") + "}";
    }

    ModelType GGEdge::getModelType() const
    {
        return ModelType::EDGE;
    }

    std::string GGEdge::generateCode() const
    {
        std::string resultCode {};
        resultCode += "\t" + utils::generateUniqueId(getName())
            + " = Edge(name=\"" + getName() + "\", "
            + "source_node_id=" + utils::generateUniqueId(getSourceNode()->getName()) + ".id, "
            + "source_port_id=\"" + m_sourcePortId + "\", "
            + "target_node_id=" + utils::generateUniqueId(getTargetNode()->getName()) + ".id, "
            + "target_port_id=\"" + m_targetPortId + "\", "
            + "condition=" + m_condition + ", "
            + "enable=" + (m_enable ? "True" : "False") + ")\n";
        return resultCode;
    }

    std::string GGEdge::buildCode(const std::string& builder) const
    {
        std::string resultCode {};
        resultCode += "\t" + builder + ".connect(" + utils::generateUniqueId(getSourceNode()->getName()) + ", "
            + utils::generateUniqueId(getSourcePort()->getName()) + ", "
            + utils::generateUniqueId(getTargetNode()->getName()) + ", "
            + utils::generateUniqueId(getTargetPort()->getName()) + ")\n";
        return resultCode;
    }

    // GGGraph 实现
    GGGraph::GGGraph(const std::string& name, const std::string& desc)
        : GGBaseModel(name, desc)
    {
    }

    bool GGGraph::addNode(const GGNodePtr& node)
    {
        if (!node)
        {
            std::cerr << "[GGGraph] addNode failed: Node is null" << std::endl;
            return false;
        }

        const std::string& nodeId = node->getId();
        if (m_nodes.contains(nodeId))
        {
            std::cerr << "[GGGraph] addNode failed: Node '"
                << nodeId << "' already exists" << std::endl;
            return false;
        }

        m_nodes[nodeId] = node;
        return true;
    }

    bool GGGraph::removeNode(const std::string& nodeId)
    {
        auto it = m_nodes.find(nodeId);
        if (it == m_nodes.end())
        {
            std::cerr << "[GGGraph] removeNode failed: Node '"
                << nodeId << "' not found" << std::endl;
            return false;
        }

        // 移除与该节点相关的所有边
        std::vector<std::string> edgesToRemove;
        for (const auto& [edgeId, edge] : m_edges)
        {
            auto sourceNode = edge->getSourceNode();
            auto targetNode = edge->getTargetNode();

            if ((sourceNode && sourceNode->getId() == nodeId) ||
                (targetNode && targetNode->getId() == nodeId))
            {
                edgesToRemove.push_back(edgeId);
            }
        }

        for (const auto& edgeId : edgesToRemove)
        {
            m_edges.erase(edgeId);
        }

        m_nodes.erase(it);
        return true;
    }

    GGNodePtr GGGraph::getNode(const std::string& nodeId) const
    {
        const auto it = m_nodes.find(nodeId);
        return (it != m_nodes.end()) ? it->second : nullptr;
    }

    const StringMap<GGNodePtr>& GGGraph::getAllNodes() const
    {
        return m_nodes;
    }

    GGNodePtr GGGraph::getStartNode() const
    {
        for (const auto& node : m_nodes | std::views::values)
        {
            if (node->isStartNode())
            {
                return node;
            }
        }
        return nullptr;
    }

    bool GGGraph::addEdge(const GGEdgePtr& edge)
    {
        if (!edge)
        {
            std::cerr << "[GGGraph] addEdge failed: Edge is null" << std::endl;
            return false;
        }

        const std::string& edgeId = edge->getId();
        if (m_edges.contains(edgeId))
        {
            std::cerr << "[GGGraph] addEdge failed: Edge '"
                << edgeId << "' already exists" << std::endl;
            return false;
        }

        // 检查边所连接的节点是否存在于图中
        auto sourceNode = edge->getSourceNode();
        auto targetNode = edge->getTargetNode();

        if (!sourceNode || !m_nodes.contains(sourceNode->getId()))
        {
            std::cerr << "[GGGraph] addEdge failed: Source node not found in graph" << std::endl;
            return false;
        }

        if (!targetNode || !m_nodes.contains(targetNode->getId()))
        {
            std::cerr << "[GGGraph] addEdge failed: Target node not found in graph" << std::endl;
            return false;
        }

        m_edges[edgeId] = edge;
        return true;
    }

    bool GGGraph::removeEdge(const std::string& edgeId)
    {
        auto it = m_edges.find(edgeId);
        if (it == m_edges.end())
        {
            std::cerr << "[GGGraph] removeEdge failed: Edge '"
                << edgeId << "' not found" << std::endl;
            return false;
        }

        m_edges.erase(it);
        return true;
    }

    GGEdgePtr GGGraph::getEdge(const std::string& edgeId) const
    {
        const auto it = m_edges.find(edgeId);
        return (it != m_edges.end()) ? it->second : nullptr;
    }

    const StringMap<GGEdgePtr>& GGGraph::getAllEdges() const
    {
        return m_edges;
    }

    std::vector<GGEdgePtr> GGGraph::getIncomingEdges(const std::string& nodeId) const
    {
        std::vector<GGEdgePtr> incomingEdges;
        for (const auto& edge : m_edges | std::views::values)
        {
            auto targetNode = edge->getTargetNode();
            if (targetNode && targetNode->getId() == nodeId)
            {
                incomingEdges.push_back(edge);
            }
        }
        return incomingEdges;
    }

    std::vector<GGEdgePtr> GGGraph::getOutgoingEdges(const std::string& nodeId) const
    {
        std::vector<GGEdgePtr> outgoingEdges;
        for (const auto& edge : m_edges | std::views::values)
        {
            auto sourceNode = edge->getSourceNode();
            if (sourceNode && sourceNode->getId() == nodeId)
            {
                outgoingEdges.push_back(edge);
            }
        }
        return outgoingEdges;
    }

    bool GGGraph::validate() const
    {
        // 验证所有节点
        for (const auto& [nodeId, node] : m_nodes)
        {
            if (!node->validate())
            {
                std::cerr << "[GGGraph] Validate failed: Node '"
                    << nodeId << "' is invalid" << std::endl;
                return false;
            }
        }

        // 验证所有边
        for (const auto& [edgeId, edge] : m_edges)
        {
            if (!edge->validate())
            {
                std::cerr << "[GGGraph] Validate failed: Edge '"
                    << edgeId << "' is invalid" << std::endl;
                return false;
            }
        }

        // 检查是否有且仅有一个起始节点
        int startNodeCount = 0;
        for (const auto& node : m_nodes | std::views::values)
        {
            if (node->isStartNode())
            {
                startNodeCount++;
            }
        }

        if (startNodeCount == 0)
        {
            std::cerr << "[GGGraph] Validate failed: No start node found" << std::endl;
            return false;
        }

        if (startNodeCount > 1)
        {
            std::cerr << "[GGGraph] Validate failed: Multiple start nodes found" << std::endl;
            return false;
        }

        // 检查是否存在循环依赖
        if (hasCycle())
        {
            std::cerr << "[GGGraph] Validate failed: Graph contains cycles" << std::endl;
            return false;
        }

        return true;
    }

    std::string GGGraph::toString() const
    {
        std::string nodesStr = "[";
        for (const auto& node : m_nodes | std::views::values)
        {
            nodesStr += node->getName() + ",";
        }
        if (!nodesStr.empty() && nodesStr.back() == ',')
            nodesStr.pop_back();
        nodesStr += "]";

        std::string edgesStr = "[";
        for (const auto& edge : m_edges | std::views::values)
        {
            edgesStr += edge->getId() + ",";
        }
        if (!edgesStr.empty() && edgesStr.back() == ',')
            edgesStr.pop_back();
        edgesStr += "]";

        return "Graph{id='" + getId() + "', name='" + getName()
            + "', desc='" + getDesc()
            + "', nodes=" + nodesStr
            + ", edges=" + edgesStr + "}";
    }

    bool GGGraph::isEmpty() const
    {
        return m_nodes.empty() && m_edges.empty();
    }

    std::vector<GGNodePtr> GGGraph::topologicalSort() const
    {
        std::vector<GGNodePtr> result;
        StringSet visited;
        StringSet visiting;

        for (const auto& [nodeId, node] : m_nodes)
        {
            if (!visited.contains(nodeId))
            {
                if (!topologicalSortDFS(node, visited, visiting, result))
                {
                    // 发现循环，返回空列表
                    return {};
                }
            }
        }

        std::reverse(result.begin(), result.end());
        return result;
    }

    void GGGraph::clear()
    {
        m_nodes.clear();
        m_edges.clear();
    }

    ModelType GGGraph::getModelType() const
    {
        return ModelType::GRAPH;
    }

    std::string GGGraph::generateCode() const
    {
        std::string resultCode {};
        std::string nodesCode {};
        for (const auto& node : m_nodes | std::views::values)
        {
            resultCode.append(node->generateCode());
            if (!nodesCode.empty())
            {
                nodesCode.append(", ");
            }
            nodesCode.append(utils::generateUniqueId(node->getName()));
        }
        std::string edgesCode {};
        for (const auto& edge : m_edges | std::views::values)
        {
            resultCode.append(edge->generateCode());
            if (!edgesCode.empty())
            {
                edgesCode.append(", ");
            }
            edgesCode.append(utils::generateUniqueId(edge->getName()));
        }
        resultCode.append("\t").append(utils::generateUniqueId(getName()) + " = Graph(name=\"" + getGraphName() + "\")\n");
        resultCode.append("\t").append(utils::generateUniqueId(getName()) + ".add_nodes(" + nodesCode + ")\n");
        resultCode.append("\t").append(utils::generateUniqueId(getName()) + ".add_edges(" + edgesCode + ")\n");
        return resultCode;
    }

    std::string GGGraph::buildCode() const
    {
        std::string resultCode {};
        const auto &builderIndent = utils::generateUniqueId(getName());
        resultCode.append("\t").append(builderIndent + " = GraphBuilder(name=\"" + getName() + "\", description=\"" + getDesc() + "\")\n");
        for (const auto &node : m_nodes | std::views::values)
        {
            resultCode.append(node->buildCode(builderIndent));
        }
        for (const auto &edge : m_edges | std::views::values)
        {
            resultCode.append(edge->buildCode(builderIndent));
        }
        resultCode.append("\t").append(builderIndent + " = " + builderIndent + ".build()\n");
        return resultCode;
    }

    bool GGGraph::hasCycle() const
    {
        StringSet visited;
        StringSet visiting;
        std::vector<GGNodePtr> dummyResult;

        for (const auto& [nodeId, node] : m_nodes)
        {
            if (!visited.contains(nodeId))
            {
                if (!topologicalSortDFS(node, visited, visiting, dummyResult))
                {
                    return true;
                }
            }
        }

        return false;
    }

    bool GGGraph::topologicalSortDFS(const GGNodePtr& node,
                                     StringSet& visited,
                                     StringSet& visiting,
                                     std::vector<GGNodePtr>& result) const
    {
        const std::string nodeId = node->getId();

        if (visiting.contains(nodeId))
        {
            // 发现循环
            return false;
        }

        if (visited.contains(nodeId))
        {
            return true;
        }

        visiting.insert(nodeId);

        // 递归处理所有后继节点
        for (const auto outgoingEdges = getOutgoingEdges(nodeId);
             const auto& edge : outgoingEdges)
        {
            if (auto targetNode = edge->getTargetNode();
                targetNode && !topologicalSortDFS(targetNode, visited, visiting, result))
            {
                return false;
            }
        }

        visiting.erase(nodeId);
        visited.insert(nodeId);
        result.push_back(node);

        return true;
    }
}
