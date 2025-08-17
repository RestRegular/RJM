from app.service.data_flow.domain.edge import Edge
from pydantic_core import core_schema


class Edges(dict):
    def validate(self, nodes):
        for id_, edge in self.items():  # type: Edge
            edge.is_valid(nodes)

    @classmethod
    def __get_pydantic_core_schema__(cls, source_type, handler):
        """
        为Pydantic提供schema，使其能够识别和验证Edges类型
        """

        # 处理实例验证
        def validate_instance(v):
            if isinstance(v, cls):
                return v
            # 如果是字典，尝试转换为Edges实例
            elif isinstance(v, dict):
                edges_instance = cls()
                for k, v in v.items():
                    # 假设Edge类可以从字典实例化
                    edges_instance[k] = Edge(**v) if not isinstance(v, Edge) else v
                return edges_instance
            else:
                raise ValueError(f"无法将{type(v)}转换为Edges实例")

        return core_schema.no_info_plain_validator_function(validate_instance)