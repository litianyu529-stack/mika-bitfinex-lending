# 参与 Mika Lending Bot

在 [Issues](https://github.com/litianyu529-stack/mika-bitfinex-lending/issues) 搜索类似问题后再提交。请注明应用版本、Windows 和 Python 版本、复现步骤、预期结果及实际结果。可以附脱敏后的错误代码和最小配置片段。请先移除 API 密钥、令牌、账户余额、订单标识和本机路径。

修复代码时，请从 `main` 创建分支，针对改动运行相关测试，再执行完整验证。

```powershell
python -m pip install -r requirements-dev.txt
.\verify.ps1
```

测试使用模拟账户和临时状态库。PR 请说明改动、复现或验证方式，以及受影响的配置与迁移路径。
