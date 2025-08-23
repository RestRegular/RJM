import uvicorn
from fastapi import FastAPI
from api.endpoints import graph, node, edge, execution

app = FastAPI(title="数据流转系统API", version="1.0")

# 注册路由
app.include_router(graph.router, prefix="/graphs", tags=["流程图"])
app.include_router(node.router, prefix="/nodes", tags=["节点"])
app.include_router(edge.router, prefix="/edges", tags=["边"])
app.include_router(execution.router, prefix="/executions", tags=["执行控制"])

@app.get("/health")
async def health_check():
    return {"status": "healthy"}


def main():
    uvicorn.run("api.main:app", port=8080)
    pass


if __name__ == '__main__':
    main()

