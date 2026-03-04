# Python Workflow (State-Machine Controlled)

Entry:
```bash
python skills/adv-qbo-tool/scripts/workflow.py \
  --file <uploaded.xlsx> \
  --bill-rules <bill_rules.json> \
  --dir <workdir> \
  --chunk-size 10 \
  --history <category_history.json> \
  --auto-threshold 0.85 \
  --confirm-threshold 0.65
```

AI requirement (mandatory, no fallback):
- Default is built-in local bridge (no per-run user input):
  - `python skills/adv-qbo-tool/scripts/ai_bridge.py`
- Override via one of:
  1) `--ai-cmd "<command>"`
  2) env `OPENCLAW_AI_CMD`
  3) `references/config/ai-runtime.json` -> `ai_cmd`
- Bridge uses local OpenClaw Agent session (default `adv-qbo-ai-judge`).
- If AI call fails/invalid JSON, workflow fails at S2 (no silent fallback).

States:
- S1_PARSE_IDENTIFY
- S2_MATCH_BUILD
- S3_CONFIRM_RENDER
- WAIT_CONFIRMATION

Artifacts:
- `workflow_state.json`
- `parse_result.json`
- `step2_ai_judge.json` (AI-judge facade output)
- `batch/batch_match_summary.json`
- `batch/match.chunk*.json` (python-built)
- `confirmation_recap.json`

Notes:
- Flow control is programmatic (not AI-driven).
- AI is limited to bounded field-judgment roles.
- Step2 now supports optional history-conditioned judging (`--history`) as soft context for category decisions.
- Confidence policy is thresholded: `>=0.85` auto-pass, `0.65~0.85` confirmation recommended, `<0.65` confirmation required.
- Steps 2/3 are Python (`step2_batch_build.py`, `step3_render_recap.py`); Step 4 submit is Python via `step4_submit.py`.
