import os.path

from fastapi import APIRouter, UploadFile, File

from data_flow import CACHE_DIR_PATH
from data_flow.api.schemas import ExecutorResponse
from data_flow.domain.node_executor_factory import NodeExecutorFactory
from data_flow.utils.file_manager import delete_cache_files

router = APIRouter()


@router.post("/upload_executor/", response_model=ExecutorResponse)
async def upload_executor(file: UploadFile = File(...)):
    content = await file.read()
    filepath = str(os.path.join(CACHE_DIR_PATH, file.filename))
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content.decode("utf-8"))
    try:
        NodeExecutorFactory.dynamic_import_executor_from_cache(file.filename, filepath)
        return ExecutorResponse(message="上传成功")
    except Exception as e:
        return ExecutorResponse(
            code=500,
            succeed=False,
            message=f"上传失败：{e}"
        )
    finally:
        delete_cache_files(filepath)
