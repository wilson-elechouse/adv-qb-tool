# Default User Interaction Script (Built-in)

## Step 1 (first reply)
“我先从你上传内容判断交易类型。若识别为 **Payment Request**，默认按 **Bill** 处理；仅在不明确时才问你是 Bill 还是 BillPayment。若你还没上传文件，我会直接问你账单关键字段（不是技术参数）。”

## Step 2
“我会直接使用当前 Collector 账号已绑定的 QBO 公司，不再询问 company/tenant；如你要覆盖目标公司，请明确告诉我。”

## Step 3 (required fields)
- Bill: vendor, lines(account+amount), **Request No. / bill unique ID**（映射 client_ref）, `Billing/SOA NO.`（映射 bill number）
- BillPayment: vendor, bank_account, pay_date, amount, allocations, **payment unique ID**（内部映射 client_ref）
- Bill mapping reminders:
  - `location <- source.location`; if missing/empty, use Collector default location
  - `line.class <- Belongs To` (nullable)
  - `description` includes Feishu/Billing Date/Period Covered/Business Unit/Reason fields
  - bill_date empty: warn first, allow empty if user insists

若用户上传 xlsx：先“解析+识别”再 recap（禁止空模板 recap）：
- 运行：`python scripts/parse_payment_request_xlsx.py --file <uploaded.xlsx> --out <parse_result.json>`
- 仅使用 `status=approved` 的行；非 approved 行全部忽略。
- 若同时存在 `Billing/SOA NO.` + `Billing End Date` + `Billing Start Date`，直接判定为 Bill。
- 若三字段不齐，则由 AI 询问用户指定类型（Bill 或 BillPayment）后再继续。
- 先用当前本地 Bill Rule 字典做匹配（Vendor/Category/Location/Tax），对未明确字段给 AI 建议值（含依据+置信度+Top3）。
- 仅当未命中/低置信度时，再触发一次 Bill Rule 实时拉取并重试匹配。
- 先回传提取到的完整业务值与建议值（整行/整记录完整回显），再追问缺失字段。
- 强制：不能只输出“待确认字段”列表，必须同时显示已解析/已确定字段，供用户整体验证。
- Vendor special rule: if vendor suggestion does not match Bill Rule vendor list, ask user to confirm. If user does not provide confirmation, proceed with empty vendor value and explicitly tell user to fix vendor in Collector UI.
- 若未拿到解析结果，必须停止并回：`parse_failed_or_empty`（附简短原因）；禁止进入确认模板或执行步骤。

当用户未上传文件时，直接按业务表单提问（不要让用户给 payload 路径）：
- Vendor
- Billing/SOA NO.
- Request No.
- Billing End Date
- Location（可空）
- Belongs To（可空）
- Category/Account + Amount（每行）
- Withholding tax（rate/amount，可空）
- Reason for payment

## Step 4
“我先确认必填信息：... 是否正确？”

## Step 5 (optional)
“要不要补充可选信息（memo、税码、class、department、WHT 等）？”

## Step 6 (hard stop)
“我先给你确认本次 Bill 关键数据（仅业务值）：
- Vendor: ...
- Bill Number(Billing/SOA NO.): ...
- Request No.: ...
- Bill Date / Due Date: ...
- Location: ...（若为空已回退默认会标注）
- Category/Account: ...
- Class(Belongs To): ...
- Withholding Tax: rate=... amount=...（若无则写 none）
- Description: ...
- Memo: ...
请确认这些值。回复 **确认执行**（或 继续/yes）后我再创建 submission 并跑 validate/precheck。”

## Step 7 (only after explicit confirmation)
“已创建 submission：...。这是 check_url：...”（check_url 应优先返回域名地址）

## Step 8
- validate ok: “校验通过。要执行 Review（第一次确认）吗？”
- validate fail: “校验未通过，原因是 ...。建议你先 ...，我可以帮你重试。”

## Step 9
“要执行 Final Confirm（第二次确认，可能写入 QBO）吗？”

## Step 10
- final ok: “已提交成功。你可以在 check_url 查看明细和历史。”
- final fail: “提交失败（原因 ...），建议下一步 ...。check_url 仍可继续修正。”
