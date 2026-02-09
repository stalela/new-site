# Agent System

## Purpose
Defines how AI agents operate inside Stalela.

Agents assist workflows.  
Agents do not own workflows.  
All actions must be deterministic, bounded, and auditable.

---

## Allowed Capabilities

Agents may:
- Read structured data
- Classify documents
- Map form fields
- Suggest next steps
- Trigger predefined tools
- Generate drafts

Agents may NOT:
- Invent legal data
- Submit filings autonomously
- Create bank accounts
- Bypass approval gates
- Access data outside scope

---

## Tools

### Database Read
- Scoped to workspace
- Read-only unless explicitly allowed

### Document Vault
- Fetch metadata
- Fetch files via signed URL
- No bulk export

### Playwright
- Navigate portals
- Fill forms
- Upload docs
- Capture screenshots

Requires:
- Human approval before submit

### Workflow Engine
- Start workflow
- Advance state
- Create tasks

---

## Human Approval

Required for:
- CIPC submissions
- SARS filings
- Bank onboarding submission
- Director changes
- Any high-risk action

Approval flow:
1. Agent prepares action
2. Human reviews
3. Human approves
4. Agent executes

---

## Confidence Thresholds

Agent must provide:
- confidence score
- reason

If confidence < threshold:
- escalate to human

Default threshold: 0.85

---

## Logging

Every agent action logs:
- input data reference
- output
- tool used
- timestamp
- actor=AGENT

Screenshots required for RPA steps.

---

## Safety Rules

- Never overwrite source data
- Never fabricate IDs
- Never skip validation
- Never run outside workspace scope

---

## Minimum Guarantees

- Human approval for filings
- Full audit trail
- Deterministic workflows
