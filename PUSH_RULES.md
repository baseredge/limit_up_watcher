# PUSH_RULES — 推送前必读

**本文档供 AI 和协作者参考，防止将开发/测试代码误推送到公开仓库。**

## 仓库地址

https://github.com/baseredge/limit_up_watcher

## 可以推送的文件

```
limit_up_watcher/       # pip 包（纯 JSON 接口，用户面）
  __init__.py
  watcher.py
  ws_source.py
  types.py
  status_codes.py
examples/
  live_demo.py
README.md
pyproject.toml
.gitignore
PUSH_RULES.md           # 本文档
```

## 禁止推送的文件/目录

```
dev/                    # 开发者验证脚本（含二进制 7B7D/zlib/5215 解析，仅本地使用）
dev/output/             # 验证输出目录
__pycache__/            # Python 缓存
*.pyc
*.egg-info/
dist/
build/
```

## 为什么 dev/ 不能推送

- `dev/replay_verify.py` 包含 7B7D 帧扫描、zlib 解压、5215 二进制字段解码
- 这些是开发者内部验证工具，并非用户面的 JSON API
- 用户只通过 D201 WebSocket 接收 JSON，不需要二进制解析
- 暴露这些代码会混淆库的定位

## AI 特别注意

1. **每次 push 前必须 `git status`**，确认 staged 文件中没有 `dev/` 下的文件
2. 如果在 `dev/` 下新增了文件，必须确认 `.gitignore` 仍然覆盖它
3. 不要因为方便就把 `dev/replay_verify.py` 临时加入 git
4. 如果验证脚本需要改进，改完保存在本地即可，不要推送

## 本地操作备忘

```bash
# 更新 remote（用户名变更后）
git remote set-url origin git@github.com:baseredge/limit_up_watcher.git

# 推送前检查
git status          # 确认没有 dev/ 文件
git diff --staged   # 确认暂存内容
git push origin master
```
