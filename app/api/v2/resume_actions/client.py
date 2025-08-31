import grpc
import stub.resume_action_pb2 as pb
import stub.resume_action_pb2_grpc as rpc


class ResumeActionClient:
    def __init__(self, host='localhost', port=50051):
        self.channel = grpc.insecure_channel(f'{host}:{port}')
        self.stub = rpc.ResumeActionServiceStub(self.channel)

    def apply_resume_access(self, applicant_uid, target_uid, resume_id, purpose):
        """接口1: 申请访问简历"""
        try:
            # 1. 构造权限检查请求
            perm_request = pb.PermissionCheck(
                actor_uid=applicant_uid,
                action="ACCESS_RESUME",
                resource_id=resume_id
            )

            # 2. 调用权限检查RPC
            perm_response = self.stub.CheckPermission(perm_request)

            if not perm_response.success:
                return {
                    "success": False,
                    "error": f"权限验证失败: {perm_response.message}",
                    "code": perm_response.error_code
                }

            # 3. 构造访问申请
            access_request = pb.ApplyAccessRequest(
                applicant_uid=applicant_uid,
                target_uid=target_uid,
                resume_id=resume_id,
                purpose=purpose
            )

            # 4. 调用访问申请RPC
            response = self.stub.ApplyResumeAccess(access_request)

            return {
                "success": response.success,
                "message": response.message,
                "code": response.error_code
            }

        except grpc.RpcError as e:
            return {
                "success": False,
                "error": f"RPC调用失败: {e.details()}",
                "code": e.code()
            }


# 使用示例
if __name__ == "__main__":
    client = ResumeActionClient(host="localhost", port=9092)

    result = client.apply_resume_access(
        applicant_uid="user_123",
        target_uid="user_456",
        resume_id="resume_789",
        purpose="招聘评估"
    )

    if result["success"]:
        print("✅ 简历访问申请成功")
    else:
        print(f"❌ 失败: {result.get('error', '未知错误')} Code: {result.get('code', -1)}")