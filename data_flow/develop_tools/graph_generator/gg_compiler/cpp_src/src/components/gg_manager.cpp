//
// Created by RestRegular on 2025/8/25.
//

#include "../../include/components/gg_manager.h"

namespace manager
{
    void BaseDataManager::addData(const std::shared_ptr<IdentItem>& config)
    {
        _dataMap.insert({config->getKey(), config->getValue()});
    }

    std::shared_ptr<BaseValue> BaseDataManager::get(const std::string& key, const std::optional<BaseValue> &defaultValue) const
    {
        if (const auto &it = _dataMap.find(key);
            it != _dataMap.end())
        {
            return it->second;
        }
        if (defaultValue.has_value())
        {
            return std::make_shared<BaseValue>(defaultValue.value());
        }
        return nullptr;
    }

    void ComponentManager::addComponent(const std::string& ident, const std::shared_ptr<GGBaseModel>& component)
    {
        _componentMap.insert({ident, component});
    }

    std::shared_ptr<GGBaseModel> ComponentManager::get(const std::string& ident, const std::optional<std::shared_ptr<GGBaseModel>>& defaultValue) const
    {
        if (const auto &it = _componentMap.find(ident);
            it != _componentMap.end())
        {
            return it->second;
        }
        if (defaultValue.has_value())
        {
            return defaultValue.value();
        }
        return nullptr;
    }
}
