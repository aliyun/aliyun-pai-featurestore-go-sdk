#!/usr/bin/env python3
"""
AI Code Review Script — 基于阿里云百炼的 PR 代码审查工具（零外部依赖）

使用方式:
    python ai-code-review.py <PR号>
    python ai-code-review.py <PR号> --post-comment   # 审查后自动发评论到 PR
    python ai-code-review.py <PR号> --model qwen-max  # 指定模型

环境变量:
    DASHSCOPE_API_KEY: 阿里云百炼 API Key（必须）
"""

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
import textwrap
import urllib.request
import urllib.error
import ssl


# ───────────────────────────────── 配置 ─────────────────────────────────

BAILIAN_BASE_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1"
DEFAULT_MODEL = "qwen-plus"

SYSTEM_PROMPT = textwrap.dedent("""\
你是一位资深的代码审查专家。请对以下 Pull Request 的代码变更进行全面审查，并用中文回复。

审查时请关注以下方面：
1. 代码质量：命名规范、代码结构、可读性
2. 潜在 Bug：逻辑错误、边界条件、空指针/异常处理
3. 安全问题：敏感信息泄露、注入风险、权限问题
4. 性能问题：不必要的计算、内存泄漏、N+1 查询等
5. 最佳实践：是否符合语言/框架的惯用写法

重要：审查时不仅要看变更的代码（+/- 行），还要结合 diff 中显示的上下文代码一起分析，
判断变更是否与周围代码逻辑一致、是否引入了上下文相关的 bug。

请严格按照以下 JSON 格式返回结果（不要包裹在 markdown 代码块中，直接返回 JSON）：

{
  "summary": "总体评价（Markdown 格式），包含：1) 对 PR 的整体评价和优点；2) 按严重程度分类的问题概要；3) 是否建议合并的结论",
  "comments": [
    {
      "path": "文件路径，必须与 diff 中的路径完全一致",
      "line": 行号整数,
      "body": "具体审查意见"
    }
  ]
}

字段说明：
- summary：总体评价，用 Markdown 格式撰写，包含优点、问题概要和合并建议。优点和肯定性评价只放在 summary 中。
- comments：行内评论数组，**只包含问题和改进建议**，每条关联到具体代码行
  - path：文件路径，必须与 diff 中 "=== 文件:" 后显示的路径完全一致
  - line：新版本文件中的行号（整数），必须是 diff 中左侧标注了行号的行
  - body：审查意见，请以 🔴（严重/必须修复）或 🟡（建议/可改进）开头

注意：
- comments 中**不要包含优点或肯定性评论**（🟢），优点统一写在 summary 中
- 只对有问题或改进建议的代码行添加 comment
- 如果代码没有问题，comments 为空数组即可
- line 必须是 diff 中左侧有行号标注的行
""")


# ───────────────────────────── gh CLI 辅助 ───────────────────────────────

def run_gh(args: list) -> str:
    """执行 gh CLI 命令并返回输出"""
    cmd = ["gh"] + args
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return result.stdout.strip()
    except FileNotFoundError:
        print("错误: 未找到 gh CLI，请先安装 → https://cli.github.com/")
        sys.exit(1)
    except subprocess.CalledProcessError as e:
        print(f"错误: gh 命令失败\n  命令: {' '.join(cmd)}\n  输出: {e.stderr.strip()}")
        sys.exit(1)


def get_pr_info(pr_number: int) -> dict:
    """获取 PR 的基本信息"""
    raw = run_gh([
        "pr", "view", str(pr_number),
        "--json", "title,body,author,baseRefName,headRefName,changedFiles,additions,deletions"
    ])
    return json.loads(raw)


def get_pr_diff(pr_number: int) -> str:
    """获取 PR 的 diff 内容"""
    return run_gh(["pr", "diff", str(pr_number)])


def get_pr_files(pr_number: int) -> list:
    """获取 PR 变更的文件列表"""
    raw = run_gh([
        "pr", "view", str(pr_number),
        "--json", "files"
    ])
    data = json.loads(raw)
    return data.get("files", [])


# ───────────────────────────── AI 调用 ───────────────────────────────

def call_bailian_api(messages: list, model: str, json_mode: bool = False) -> str:
    """
    使用 Python 标准库调用阿里云百炼 OpenAI 兼容接口。
    无需安装任何第三方库。
    """
    api_key = os.getenv("DASHSCOPE_API_KEY")
    if not api_key:
        print("错误: 请设置环境变量 DASHSCOPE_API_KEY")
        print("  export DASHSCOPE_API_KEY='your-api-key'")
        sys.exit(1)

    url = f"{BAILIAN_BASE_URL}/chat/completions"
    payload_dict = {
        "model": model,
        "messages": messages,
        "temperature": 0.3,
    }
    if json_mode:
        payload_dict["response_format"] = {"type": "json_object"}
    payload = json.dumps(payload_dict).encode("utf-8")

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }

    req = urllib.request.Request(url, data=payload, headers=headers, method="POST")

    # 创建 SSL 上下文（兼容各种环境）
    ctx = ssl.create_default_context()
    # macOS 下 Homebrew Python 可能找不到默认 CA 证书，依次尝试备选路径
    if not ctx.get_ca_certs():
        for ca_path in ["/etc/ssl/cert.pem", "/etc/ssl/certs/ca-certificates.crt"]:
            if os.path.exists(ca_path):
                ctx.load_verify_locations(ca_path)
                break

    try:
        with urllib.request.urlopen(req, context=ctx, timeout=300) as resp:
            body = json.loads(resp.read().decode("utf-8"))
            return body["choices"][0]["message"]["content"]
    except urllib.error.HTTPError as e:
        error_body = e.read().decode("utf-8", errors="replace")
        print(f"错误: API 调用失败 (HTTP {e.code})")
        print(f"  响应: {error_body[:500]}")
        sys.exit(1)
    except urllib.error.URLError as e:
        print(f"错误: 网络连接失败 — {e.reason}")
        sys.exit(1)


# ───────────────────────────── 核心逻辑 ───────────────────────────────

def truncate_diff(diff: str, max_chars: int = 60000) -> str:
    """截断过长的 diff，保留头尾"""
    if len(diff) <= max_chars:
        return diff
    half = max_chars // 2
    return (
        diff[:half]
        + "\n\n... [中间部分因长度限制被省略] ...\n\n"
        + diff[-half:]
    )


def annotate_diff_with_line_numbers(diff: str) -> tuple:
    """
    给 diff 的每行添加新版本文件行号注释。
    返回: (annotated_diff, valid_lines)
        valid_lines: set of (file_path, line_number) — 可被行内评论的行
    """
    lines = diff.split('\n')
    annotated = []
    valid_lines = set()
    current_file = None
    new_line = 0
    in_hunk = False

    for line in lines:
        if line.startswith('diff --git'):
            in_hunk = False
            match = re.search(r' b/(.+)$', line)
            if match:
                current_file = match.group(1)
            annotated.append(f"\n=== 文件: {current_file} ===")
        elif line.startswith('@@'):
            in_hunk = True
            match = re.search(r'\+(\d+)', line)
            if match:
                new_line = int(match.group(1))
            annotated.append(line)
        elif not in_hunk:
            # 元信息行 (index, ---, +++, similarity 等)
            continue
        elif line.startswith('+'):
            annotated.append(f"  {new_line:>4} | {line}")
            if current_file:
                valid_lines.add((current_file, new_line))
            new_line += 1
        elif line.startswith('-'):
            annotated.append(f"       | {line}")
            # 删除行没有新版行号，不递增
        elif line.startswith('\\'):
            # "\ No newline at end of file"
            annotated.append(line)
        else:
            # 上下文行
            annotated.append(f"  {new_line:>4} | {line}")
            if current_file:
                valid_lines.add((current_file, new_line))
            new_line += 1

    return '\n'.join(annotated), valid_lines


def build_review_prompt(pr_info: dict, annotated_diff: str) -> str:
    """构建发送给 AI 的审查请求"""
    files = pr_info.get("files", [])
    file_summary = ""
    if files:
        file_lines = [
            f"  - {f['path']} (+{f.get('additions', 0)} -{f.get('deletions', 0)})"
            for f in files
        ]
        file_summary = "\n".join(file_lines)

    body = pr_info.get("body") or "（无描述）"

    prompt = f"""\
## Pull Request 信息

- **标题**: {pr_info.get('title', 'N/A')}
- **作者**: {pr_info.get('author', {}).get('login', 'N/A')}
- **分支**: {pr_info.get('headRefName', '?')} → {pr_info.get('baseRefName', '?')}
- **变更统计**: +{pr_info.get('additions', 0)} -{pr_info.get('deletions', 0)}

### PR 描述
{body}

### 变更文件
{file_summary or '（未获取到文件列表）'}

### 代码 Diff（左侧数字为新版本文件行号）
{annotated_diff}

请对以上代码变更进行审查。注意 path 使用 "=== 文件:" 后面的路径，line 使用左侧标注的行号。
"""
    return prompt


def ai_review(prompt: str, model: str) -> str:
    """调用百炼 API 进行代码审查"""
    print(f"🤖 正在使用 {model} 进行代码审查...\n")
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": prompt},
    ]
    return call_bailian_api(messages, model, json_mode=True)


def parse_review_response(response: str) -> dict:
    """从 AI 回复中解析出结构化 JSON"""
    # 尝试从 markdown 代码块中提取
    json_match = re.search(r'```(?:json)?\s*\n(.*?)\n```', response, re.DOTALL)
    if json_match:
        text = json_match.group(1)
    else:
        text = response.strip()

    try:
        data = json.loads(text)
        # 基本校验
        if "summary" not in data:
            data["summary"] = "（未能解析总结）"
        if "comments" not in data:
            data["comments"] = []
        return data
    except json.JSONDecodeError:
        # 回退：把整个回复当作纯文本总结
        print("⚠️  AI 回复不是有效 JSON，将作为纯文本总结处理。")
        return {"summary": response, "comments": []}


def get_repo_name() -> str:
    """获取当前仓库的 owner/repo 名称"""
    return run_gh(["repo", "view", "--json", "nameWithOwner", "-q", ".nameWithOwner"])


def post_review_with_comments(pr_number: int, summary: str, comments: list, valid_lines: set):
    """通过 GitHub Review API 提交带行内评论的 Review"""
    repo = get_repo_name()

    # 过滤出 diff 中实际存在的行，无效的追加到总结
    valid_comments = []
    skipped = []
    for c in comments:
        key = (c.get("path", ""), c.get("line", 0))
        if key in valid_lines:
            valid_comments.append({
                "path": c["path"],
                "line": c["line"],
                "side": "RIGHT",
                "body": c["body"],
            })
        else:
            skipped.append(c)

    if skipped:
        summary += "\n\n### 其他审查意见\n\n"
        for c in skipped:
            summary += f"- **{c.get('path', '?')}:{c.get('line', '?')}** — {c.get('body', '')}\n"

    review_body = (
        "## 🤖 AI Code Review\n\n"
        f"{summary}\n\n"
        "---\n"
        "_由 AI 自动生成的代码审查，仅供参考。_"
    )

    review_data = {
        "body": review_body,
        "event": "COMMENT",
        "comments": valid_comments,
    }

    fd, temp_path = tempfile.mkstemp(suffix='.json')
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            json.dump(review_data, f, ensure_ascii=False)
        run_gh(["api", f"repos/{repo}/pulls/{pr_number}/reviews",
                "-X", "POST",
                "--input", temp_path])
        print(f"✅ 审查已发布到 PR（{len(valid_comments)} 条行内评论 + 总结评论）。")
        if skipped:
            print(f"   ⚠️  {len(skipped)} 条评论无法关联到 diff 行，已追加到总结中。")
    finally:
        os.unlink(temp_path)


# ───────────────────────────── 主流程 ─────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="AI 代码审查工具 — 基于阿里云百炼（零依赖版）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""\
        示例:
          python ai-code-review.py 42
          python ai-code-review.py 42 --post-comment
          python ai-code-review.py 42 --model qwen-max

        前置条件:
          1. 安装 gh CLI: https://cli.github.com/
          2. 登录 gh: gh auth login
          3. 设置百炼 API Key: export DASHSCOPE_API_KEY='sk-xxx'
        """)
    )
    parser.add_argument("pr_number", type=int, help="PR 编号")
    parser.add_argument("--model", default=DEFAULT_MODEL,
                        help=f"使用的模型 (默认: {DEFAULT_MODEL})")
    parser.add_argument("--post-comment", action="store_true",
                        help="将审查结果发布为 PR Review（行内评论 + 总结）")
    parser.add_argument("--max-diff-chars", type=int, default=60000,
                        help="diff 最大字符数 (默认: 60000)")

    args = parser.parse_args()

    # 1. 获取 PR 信息
    print(f"📋 获取 PR #{args.pr_number} 信息...")
    pr_info = get_pr_info(args.pr_number)
    print(f"   标题: {pr_info.get('title', 'N/A')}")
    print(f"   作者: {pr_info.get('author', {}).get('login', 'N/A')}")
    print(f"   变更: +{pr_info.get('additions', 0)} -{pr_info.get('deletions', 0)}")

    # 2. 获取 diff
    print(f"\n📝 获取代码变更...")
    diff = get_pr_diff(args.pr_number)
    if not diff:
        print("⚠️  PR 没有代码变更。")
        sys.exit(0)

    original_len = len(diff)
    diff = truncate_diff(diff, args.max_diff_chars)
    if len(diff) < original_len:
        print(f"   ⚠️  diff 较长 ({original_len} 字符)，已截断至 {args.max_diff_chars} 字符")

    # 3. 获取文件列表
    files = get_pr_files(args.pr_number)
    pr_info["files"] = files
    print(f"   变更文件数: {len(files)}")

    # 4. 给 diff 添加行号注释
    annotated_diff, valid_lines = annotate_diff_with_line_numbers(diff)

    # 5. 构建 prompt 并调用 AI
    prompt = build_review_prompt(pr_info, annotated_diff)
    review_raw = ai_review(prompt, args.model)

    # 6. 解析 AI 回复
    review_data = parse_review_response(review_raw)
    summary = review_data.get("summary", "（未能解析审查结果）")
    comments = review_data.get("comments", [])

    # 7. 输出结果
    print("=" * 60)
    print("📝 AI 代码审查结果")
    print("=" * 60)
    print(f"\n📌 总结:\n{summary}\n")
    if comments:
        print(f"💬 行内评论 ({len(comments)} 条):")
        for i, c in enumerate(comments, 1):
            print(f"  [{i}] {c.get('path', '?')}:{c.get('line', '?')}")
            print(f"      {c.get('body', '')}")
    else:
        print("💬 没有行内评论。")
    print("=" * 60)

    # 8. 可选：发布 Review
    if args.post_comment:
        post_review_with_comments(args.pr_number, summary, comments, valid_lines)
    else:
        print("\n💡 提示: 添加 --post-comment 参数可以将审查结果发布到 PR。")


if __name__ == "__main__":
    main()
