//
// Created by RestRegular on 2025/8/25.
//

#ifndef GG_GG_MANAGER_H
#define GG_GG_MANAGER_H

#include <memory>
#include <optional>
#include <unordered_map>

#include "gg_model.h"

namespace manager
{
    using namespace model;

    class BaseDataManager
    {
        std::unordered_map<std::string, std::shared_ptr<BaseValue>> _dataMap{};
    public:
        BaseDataManager() = default;

        void addData(const std::shared_ptr<IdentItem> &config);
        std::shared_ptr<BaseValue> get(const std::string &key, const std::optional<BaseValue> &defaultValue = std::nullopt) const;

        // 迭代器支持
        auto begin() const { return _dataMap.begin(); }
        auto end() const { return _dataMap.end(); }
        auto cbegin() const { return _dataMap.cbegin(); }
        auto cend() const { return _dataMap.cend(); }
    };

    class ComponentManager
    {
        std::unordered_map<std::string, std::shared_ptr<GGBaseModel>> _componentMap{};

    public:
        ComponentManager() = default;

        void addComponent(const std::string &ident, const std::shared_ptr<GGBaseModel> &component);
        std::shared_ptr<GGBaseModel> get(const std::string &ident, const std::optional<std::shared_ptr<GGBaseModel>>& defaultValue = std::nullopt) const;

        // 迭代器支持
        auto begin() const { return _componentMap.begin(); }
        auto end() const { return _componentMap.end(); }
        auto cbegin() const { return _componentMap.cbegin(); }
        auto cend() const { return _componentMap.cend(); }
    };


}

#endif //GG_GG_MANAGER_H