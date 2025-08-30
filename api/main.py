import warnings

import uvicorn
from pydantic.json_schema import PydanticJsonSchemaWarning
from fastapi import FastAPI
from api.endpoints import graph, execution

warnings.filterwarnings('ignore', category=PydanticJsonSchemaWarning)

app = FastAPI(title="数据流转系统API", version="1.0")

# 注册路由
app.include_router(graph.router, prefix="/graphs", tags=["流程图"])
app.include_router(execution.router, prefix="/executions", tags=["执行控制"])

@app.get("/health")
async def health_check():
    return {"status": "healthy"}


def main():
    uvicorn.run("api.main:app", port=8080)
    pass


if __name__ == '__main__':
    main()

