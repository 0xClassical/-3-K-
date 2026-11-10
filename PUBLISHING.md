# 发布到 GitHub：所有者检查清单

这份清单用于检查经过脱敏的源码副本。仓库上传及其可见性以实际 GitHub 状态为准；公开发布前仍需完成下列确认。

## 公开前必须确认

- [ ] 确认原始基础压缩包及新增代码的版权来源、原有许可证和再分发权。
- [ ] 确认采用 MIT，以及 LICENSE 中最终版权署名（目前暂用项目贡献者名称）。
- [ ] 确认公开 A/B/C 策略及实验性交易实现。2026-11-10 已按作者明确要求公开 189 个监控钱包地址，存放在 `config/targets.example.json`；其他个人名单仍需单独授权。
- [ ] 选择 GitHub 账号和仓库名；Git commit 邮箱可使用 GitHub 提供的 noreply 地址。
- [ ] 先运行所有离线测试和发布扫描。扫描不是专业密钥审计，仍须人工审查。

## 建议发布步骤

1. 只在本目录操作。不要回到旧运行目录执行 `git add .`，也不要复制旧 .git。
2. 使用 GitHub Desktop 登录，添加本地项目（或按下方命令初始化）。先发布成 **Private**。
3. 检查仓库文件、全部提交历史和 CI 日志：不得出现个人配置、钱包、RPC Key、数据库、浏览器资料或真实交易记录。
4. 在干净机器/全新 clone 中按 README 复制公开名单或自行填写监控地址，再编译与启动。
5. 确认权限来源、许可证、署名、风险说明及测试后，再由所有者主动切换 Public。
6. 配置 GitHub 安全告警、推送保护和私密漏洞报告（以账号/仓库实际可用功能为准）。先发布源码，不发布带本机文件的二进制压缩包。

如使用命令行，先安装 Git 和 GitHub CLI。以下命令**只供审查通过后由所有者执行**；不要把 API Key 当 Git 密码：

```powershell
git init -b main
python scripts/check-release.py
git add src web browser-extension config scripts docs .github Cargo.toml Cargo.lock README.md LICENSE SECURITY.md CONTRIBUTING.md THIRD_PARTY_NOTICES.md TRADING.md PUBLISHING.md RELEASE-REVIEW.md trading-config.example.json start-backend.ps1 start-system.ps1 initialize-runtime.ps1 .gitignore .gitattributes
git diff --cached --stat
# 人工检查暂存内容；使用自己的 Git 作者信息或 noreply 邮箱。
git commit -m "Prepare sanitized open-source release"
gh auth login
gh repo create meme-quant --private --source . --remote origin --push
```

参考官方说明：

- https://docs.github.com/en/migrations/importing-source-code/using-the-command-line-to-import-source-code/adding-locally-hosted-code-to-github
- https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/removing-sensitive-data-from-a-repository
- https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/customizing-your-repository/licensing-a-repository

本地 ZIP 同样只能来自这个干净目录的发布清单，不能对旧运行目录整体打包。可以在干净副本中运行 `python scripts/build-source-archive.py --output ../meme-quant-source.zip`；脚本先扫描、只打包通过的源码文件，并拒绝覆盖已有 ZIP 或输出到源码目录内。以后跑过系统，这里也会生成私有运行文件，应使用新 clone/受控导出重新制作发布包，不要直接全选压缩。
