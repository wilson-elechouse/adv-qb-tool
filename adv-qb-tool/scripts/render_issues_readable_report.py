#!/usr/bin/env python3
import argparse
import json
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path


def read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


ISSUE_LABELS = {
    "location_ref_text": "Location 未自动匹配",
    "source:payment_details_01_multiple_gross_amounts": "Payment Details 01 识别到多个 Gross Amount",
    "source:payment_details_01_multiple_net_amounts": "Payment Details 01 识别到多个 Net Amount",
    "source:due_date_before_bill_date": "Due Date 早于 Bill Date",
    "validate:due_date_before_bill_date": "Due Date 校验失败",
    "source:payment_details_02_multiple_products": "Payment Details 02 识别到多个 Product",
    "source:payment_details_02_multiple_clients": "Payment Details 02 识别到多个 Client",
}


ISSUE_ACTIONS = {
    "location_ref_text": "确认这张 bill 的 Location；如果同类记录都应该映射到同一个地点，建议补充 location 对照规则。",
    "source:payment_details_01_multiple_gross_amounts": "核对 Payment Details 01 中到底哪一个 Gross Amount 才是本次 bill 应使用的金额。",
    "source:payment_details_01_multiple_net_amounts": "核对 Payment Details 01 中到底哪一个 Net Amount 才是本次 bill 应使用的金额。",
    "source:due_date_before_bill_date": "检查源文件日期是否录反；通常需要修正 bill_date 或 due_date。",
    "validate:due_date_before_bill_date": "修正日期后重新跑一次，让校验通过。",
    "source:payment_details_02_multiple_products": "确认本次 bill 应归属哪个 Product，必要时拆单。",
    "source:payment_details_02_multiple_clients": "确认本次 bill 实际属于哪个 Client，必要时拆单。",
}


def normalize_issue_codes(item: dict) -> list[str]:
    codes = []
    for value in item.get("unresolved") or []:
        text = str(value or "").strip()
        if text:
            codes.append(text)
    for value in item.get("source_errors") or []:
        text = f"source:{str(value or '').strip()}"
        if text not in codes and text != "source:":
            codes.append(text)
    for value in item.get("validation_issues") or []:
        text = f"validate:{str(value or '').strip()}"
        if text not in codes and text != "validate:":
            codes.append(text)
    return codes


def issue_label(code: str) -> str:
    return ISSUE_LABELS.get(code, code)


def issue_action(code: str) -> str:
    return ISSUE_ACTIONS.get(code, "人工核对源数据并确认本次 bill 的正确取值。")


def summarize_reason(text: str, limit: int = 140) -> str:
    raw = " ".join(str(text or "").split())
    if len(raw) <= limit:
        return raw
    return raw[: limit - 3] + "..."


def build_markdown(report: dict) -> str:
    items = list(report.get("needs_user_action_items") or [])
    total = int(report.get("total_records") or 0)
    ready_count = int(report.get("ready_count") or 0)
    failed_count = int(report.get("failed_count") or 0)
    issue_count = int(report.get("issue_count") or len(items))

    issue_counter = Counter()
    grouped = defaultdict(list)
    for item in items:
        codes = normalize_issue_codes(item)
        if not codes:
            codes = ["unknown"]
        for code in codes:
            issue_counter[code] += 1
            grouped[code].append(item)

    lines = []
    lines.append("# 问题 Bill 可读报告")
    lines.append("")
    lines.append(f"- 生成时间: {datetime.now().isoformat(timespec='seconds')}")
    lines.append(f"- 当前状态: `{report.get('state')}`")
    lines.append(f"- 总记录数: `{total}`")
    lines.append(f"- 已就绪: `{ready_count}`")
    lines.append(f"- 需处理问题单据: `{issue_count}`")
    lines.append(f"- 失败记录: `{failed_count}`")
    lines.append("")

    if not items:
        lines.append("## 总结")
        lines.append("")
        lines.append("当前没有需要人工介入的问题 bill。")
        return "\n".join(lines) + "\n"

    lines.append("## 问题概览")
    lines.append("")
    for code, count in issue_counter.most_common():
        lines.append(f"- {issue_label(code)}: `{count}` 条")
    lines.append("")

    lines.append("## 处理建议")
    lines.append("")
    for code, count in issue_counter.most_common():
        lines.append(f"- {issue_label(code)}: {issue_action(code)}")
    lines.append("")

    lines.append("## 按问题类型查看")
    lines.append("")
    for code, group_items in issue_counter.most_common():
        items_for_code = grouped[code]
        lines.append(f"### {issue_label(code)}")
        lines.append("")
        lines.append(f"涉及 `{len(items_for_code)}` 条 bill。")
        lines.append("")
        for item in items_for_code:
            request_no = str(item.get("request_no") or "N/A")
            vendor = str(item.get("vendor") or "N/A")
            bill_date = str(item.get("bill_date") or "")
            due_date = str(item.get("due_date") or "")
            reason = summarize_reason(item.get("business_reason") or "")
            lines.append(f"- `{request_no}` | `{vendor}` | bill_date `{bill_date}` | due_date `{due_date}`")
            lines.append(f"  摘要: {reason}")
        lines.append("")

    lines.append("## 逐条处理清单")
    lines.append("")
    for item in items:
        request_no = str(item.get("request_no") or "N/A")
        vendor = str(item.get("vendor") or "N/A")
        bill_date = str(item.get("bill_date") or "")
        due_date = str(item.get("due_date") or "")
        codes = normalize_issue_codes(item)
        labels = [issue_label(code) for code in codes] or ["未分类问题"]
        actions = []
        seen = set()
        for code in codes:
            action = issue_action(code)
            if action not in seen:
                actions.append(action)
                seen.add(action)
        lines.append(f"### {request_no}")
        lines.append("")
        lines.append(f"- Vendor: `{vendor}`")
        lines.append(f"- Bill Date / Due Date: `{bill_date}` / `{due_date}`")
        lines.append(f"- 问题: {'；'.join(labels)}")
        if actions:
            lines.append(f"- 建议: {'；'.join(actions)}")
        lines.append(f"- 业务说明: {summarize_reason(item.get('business_reason') or '', limit=220)}")
        lines.append("")

    return "\n".join(lines) + "\n"


def main():
    ap = argparse.ArgumentParser(description="Render a human-readable issue report from canonical_bills.issues.json")
    ap.add_argument("--issues", required=True, help="canonical_bills.issues.json")
    ap.add_argument("--out", required=True, help="output markdown path")
    args = ap.parse_args()

    issues_path = Path(args.issues).resolve()
    out_path = Path(args.out).resolve()
    report = read_json(issues_path)
    content = build_markdown(report)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(content, encoding="utf-8")
    print(json.dumps({"ok": True, "out": str(out_path), "issues": int(report.get("issue_count") or 0)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
