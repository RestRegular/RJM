import os
import sys
import warnings

import uvicorn
from fastapi import FastAPI
from pydantic.json_schema import PydanticJsonSchemaWarning

PROJECT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from data_flow.api.endpoints import graph, execution, executor

warnings.filterwarnings('ignore', category=PydanticJsonSchemaWarning)

app = FastAPI(title="数据流转系统API", version="1.0")

# 注册路由
app.include_router(graph.router, prefix="/graphs", tags=["流程图"])
app.include_router(execution.router, prefix="/executions", tags=["执行控制"])
app.include_router(executor.router, prefix="/executors", tags=["执行器"])

@app.get("/health")
async def health_check():
    return {"status": "healthy"}


def main():
    uvicorn.run(
        "data_flow.api.main:app",
        host="0.0.0.0",
        port=8897,
        reload=False
    )


if __name__ == '__main__':
    main()
