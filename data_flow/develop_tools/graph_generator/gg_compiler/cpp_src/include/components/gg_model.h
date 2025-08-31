//
// Created by RestRegular on 2025/8/25.
//

#ifndef GG_GG_MODEL_H
#define GG_GG_MODEL_H

#include <any>
#include <memory>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <mutex>

#include "../lib/rcc_utils.h"

namespace model
{
    // 基础类型别名（简化代码）
    using StringVector = std::vector<std::string>;
    template <typename ValueT>
    using StringMap = std::unordered_map<std::string, ValueT>;
    using StringSet = std::unordered_set<std::string>;
    using DataList = std::vector<std::any>; // 通用数据列表（支持任意类型）

    enum class BaseValueType
    {
        LITERAL, // 字面量 ..., "...", 123, 1.0, true, null
        RAW_DATA, // 原生数据 `...`
        IDENTIFIER, // 标识符 @...
        LIST, // 列表 [..., ...]
    };

    std::string getBaseDataTypeName(const BaseValueType& type);

    class BaseValue
    {
        BaseValueType _type;
        std::string _string_value;
        std::vector<std::shared_ptr<BaseValue>> _list_value;

    public:
        // 构造函数 - 字符串类型（LITERAL, RAW_DATA, IDENTIFIER）
        explicit BaseValue(BaseValueType type, const std::string& value);

        // 构造函数 - 列表类型
        explicit BaseValue(const std::vector<std::shared_ptr<BaseValue>>& value);

        // 获取类型
        BaseValueType getType() const;

        // 获取字符串值（适用于非LIST类型）
        std::string getStringValue() const;

        // 获取列表值（仅适用于LIST类型）
        const std::vector<std::shared_ptr<BaseValue>>& getListValue() const;

        // 模板方法获取值
        template <typename T>
        T getValue() const;
    };

    class IdentItem
    {
        std::string _key;
        std::shared_ptr<BaseValue> _value;

    public:
        // 构造函数 - 字符串类型值
        IdentItem(const std::string& key, BaseValueType type, const std::string& value);

        // 构造函数 - 列表类型值
        IdentItem(const std::string& key, const std::vector<std::shared_ptr<BaseValue>>& value);

        // 构造函数 - 直接使用ConfigValue
        IdentItem(const std::string& key, std::shared_ptr<BaseValue> value);

        // 获取键名
        std::string getKey() const;

        // 获取值类型
        BaseValueType getValueType() const;

        // 获取值（共享指针）
        std::shared_ptr<BaseValue> getValue() const;

        // 获取字符串值
        std::string getStringValue() const;

        // 获取列表值
        const std::vector<std::shared_ptr<BaseValue>>& getListValue() const;

        // 模板方法获取值
        template <typename T>
        T getValue() const;
    };

    using ItemList = std::vector<std::shared_ptr<BaseValue>>;
    using IdentItemList = std::vector<std::shared_ptr<IdentItem>>;

    // 模板方法的实现必须在头文件中
    template <typename T>
    T BaseValue::getValue() const
    {
        if constexpr (std::is_same_v<T, std::string>)
        {
            return getStringValue();
        }
        else if constexpr (std::is_same_v<T, std::vector<std::shared_ptr<BaseValue>>>)
        {
            return getListValue();
        }
        else
        {
            throw std::invalid_argument("Unsupported value type");
        }
    }

    template <typename T>
    T IdentItem::getValue() const
    {
        return _value->getValue<T>();
    }

    enum class ModelType
    {
        NODE,
        EDGE,
        PORT,
        GRAPH
    };

    std::string getModelTypeName(const ModelType& type);

    /**
     * @brief 所有图形模型的基类
     */
    class GGBaseModel : public utils::Object
    {
    public:
        explicit GGBaseModel(const std::string& name, const std::string& desc = "");
        explicit GGBaseModel(const std::string &id, const std::string& name, const std::string& desc);
        ~GGBaseModel() override = default;

        GGBaseModel(const GGBaseModel&) = delete;
        GGBaseModel& operator=(const GGBaseModel&) = delete;
        GGBaseModel(GGBaseModel&&) noexcept = default;
        GGBaseModel& operator=(GGBaseModel&&) noexcept = default;

        const std::string& getId() const;
        const std::string& getName() const;
        const std::string& getDesc() const;

        void setName(const std::string& name);
        void setDesc(const std::string& desc);

        virtual bool validate() const = 0;
        virtual ModelType getModelType() const = 0;

    protected:
        std::string m_id;
        std::string m_name;
        std::string m_desc;
    };

    /**
     * @brief 端口模型（Port）
     */
    class GGPort final : public GGBaseModel
    {
    public:
        enum class DataType
        {
            JSON,
            TEXT,
            NUMBER,
            DATE,
            BOOLEAN,
            BINARY,
            LIST,
            DICT,
            ANY,
            NONE
        };

        static DataType getDataTypeFromConfigValue(const std::shared_ptr<BaseValue> &configValue);

        GGPort(const std::string& id, const std::string& name, DataType dataType, bool required = true);

        std::string getDataType() const;
        bool isRequired() const;

        void setData(DataList data);
        DataList getData() const;

        bool validate() const override;
        std::string toString() const override;
        std::string generateCode() const;
        std::string buildCode(const std::string& builder) const;

        static std::string dataTypeToString(const DataType& type);
        ModelType getModelType() const override;

    private:
        DataType m_dataType;
        bool m_required;
        DataList m_data;
        mutable std::mutex m_dataMutex;
    };

    using GGPortPtr = std::shared_ptr<GGPort>;
    using GGPortWeakPtr = std::weak_ptr<GGPort>;

    /**
     * @brief 节点模型（Node）
     */
    class GGNode final : public GGBaseModel
    {
    public:
        GGNode(const std::string& id, const std::string& name, const std::string &nodeType, bool isStart = false, bool isEnd = false);

        bool addInputPort(const GGPortPtr& port);
        bool addOutputPort(const GGPortPtr& port);

        GGPortPtr getInputPort(const std::string& portId) const;
        GGPortPtr getOutputPort(const std::string& portId) const;

        const StringMap<GGPortPtr>& getAllInputPorts() const;
        const StringMap<GGPortPtr>& getAllOutputPorts() const;

        std::string getNodeType() const;
        bool isStartNode() const;
        const std::string& getConfig() const;

        void setIsStartNode(bool isStart);
        void setConfig(const std::string& config);

        bool validate() const override;
        std::string toString() const override;
        ModelType getModelType() const override;
        std::string generateCode() const;
        std::string buildCode(const std::string& builder);

    private:
        std::string m_nodeType;
        bool m_isStart;
        bool m_isEnd;
        std::string m_config;
        StringMap<GGPortPtr> m_inputPorts;
        StringMap<GGPortPtr> m_outputPorts;
    };

    using GGNodePtr = std::shared_ptr<GGNode>;
    using GGNodeWeakPtr = std::weak_ptr<GGNode>;

    /**
     * @brief 边模型（Edge）
     */
    class GGEdge final : public GGBaseModel
    {
    public:
        GGEdge(const std::string& name,
               const GGNodePtr& sourceNode, const std::string& sourcePortId,
               const GGNodePtr& targetNode, const std::string& targetPortId,
               const std::string& condition, bool enable = true,
               const std::string& desc = "");

        GGNodePtr getSourceNode() const;
        GGNodePtr getTargetNode() const;
        const std::string& getSourcePortId() const;
        const std::string& getTargetPortId() const;

        GGPortPtr getSourcePort() const;
        GGPortPtr getTargetPort() const;

        bool isEnabled() const;
        void setEnable(bool enable);

        std::string getCondition() const;

        bool validate() const override;
        std::string toString() const override;
        ModelType getModelType() const override;
        std::string generateCode() const;
        std::string buildCode(const std::string& builder) const;

    private:
        GGNodeWeakPtr m_sourceNode;
        GGNodeWeakPtr m_targetNode;
        std::string m_sourcePortId;
        std::string m_targetPortId;
        bool m_enable;
        std::string m_condition;
    };

    using GGEdgePtr = std::shared_ptr<GGEdge>;
    using GGEdgeWeakPtr = std::weak_ptr<GGEdge>;

    /**
     * @brief 图模型（Graph）
     * 包含节点和边的完整流程图，提供整体验证和执行功能
     */
    class GGGraph final : public GGBaseModel
    {
    public:
        explicit GGGraph(const std::string& name, const std::string& desc = "");

        // ------------------------------ 节点管理接口 ------------------------------
        bool addNode(const GGNodePtr& node);
        bool removeNode(const std::string& nodeId);
        GGNodePtr getNode(const std::string& nodeId) const;
        const StringMap<GGNodePtr>& getAllNodes() const;

        // 获取起始节点
        GGNodePtr getStartNode() const;

        // ------------------------------ 边管理接口 ------------------------------
        bool addEdge(const GGEdgePtr& edge);
        bool removeEdge(const std::string& edgeId);
        GGEdgePtr getEdge(const std::string& edgeId) const;
        const StringMap<GGEdgePtr>& getAllEdges() const;

        // 获取连接到指定节点的边
        std::vector<GGEdgePtr> getIncomingEdges(const std::string& nodeId) const;
        std::vector<GGEdgePtr> getOutgoingEdges(const std::string& nodeId) const;

        // ------------------------------ 图属性访问 ------------------------------
        const std::string& getGraphName() const { return getName(); }
        const std::string& getGraphDesc() const { return getDesc(); }

        // ------------------------------ 基础接口实现 ------------------------------
        bool validate() const override;
        std::string toString() const override;

        // ------------------------------ 图操作接口 ------------------------------
        // 检查图是否为空
        bool isEmpty() const;

        // 获取图的拓扑排序（用于执行顺序）
        std::vector<GGNodePtr> topologicalSort() const;

        // 清空图
        void clear();
        ModelType getModelType() const override;

        // ------------------------------- 生成代码 -------------------------------
        std::string generateCode() const;
        std::string buildCode() const;

    private:
        // 检查图中是否存在循环依赖
        bool hasCycle() const;

        // DFS 辅助函数用于拓扑排序和循环检测
        bool topologicalSortDFS(const GGNodePtr& node,
                                StringSet& visited,
                                StringSet& visiting,
                                std::vector<GGNodePtr>& result) const;

        StringMap<GGNodePtr> m_nodes; // 节点集合（ID -> 节点）
        StringMap<GGEdgePtr> m_edges; // 边集合（ID -> 边）
    };

    using GGGraphPtr = std::shared_ptr<GGGraph>;
    using GGGraphWeakPtr = std::weak_ptr<GGGraph>;
}

#endif // GG_GG_MODEL_H
