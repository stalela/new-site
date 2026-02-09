# Compliance Engine

## Purpose

The compliance engine is the subsystem responsible for:

- Determining what a business must do to remain compliant
- When those actions are due
- What data and documents are required
- Tracking filings and outcomes
- Escalating risk when obligations are missed

This engine converts static regulatory requirements into an operational, automated lifecycle.

It is one of the core differentiators of Stalela.  
Company registration happens once.  
Compliance happens forever.

---

## Design Principles

### Deterministic
Rules must produce the same obligations given the same inputs.

### Versioned
Rules change over time. Every obligation must record the rule version used.

### Auditable
All computed obligations and transitions must be traceable.

### Human-overridable
Operators can override obligations with reason codes.

### Event-driven
Changes in company state trigger recomputation.

---

## Core Concepts

### ObligationDefinition
Defines a type of compliance requirement.

Examples:
- CIPC annual return
- VAT filing
- PAYE submission
- UIF declaration
- BEE certificate renewal

### Obligation
A concrete instance for a specific business and period.

### Filing
An attempt to satisfy an obligation.

### Evidence
Documents and records proving compliance.

---

## Obligation Lifecycle

States:

1. `PLANNED`
2. `DUE_SOON`
3. `DUE`
4. `IN_PROGRESS`
5. `SUBMITTED`
6. `COMPLETED`
7. `FAILED`
8. `WAIVED`

---

## Rule Engine

Rules compute obligations based on:

- Company type
- Registration date
- Revenue
- VAT status
- Payroll presence
- Number of employees
- Jurisdiction

Rules must be:

- Pure functions
- Versioned
- Testable
- Deterministic

---

## Rule Inputs

Each evaluation snapshot includes:

- company_profile_version
- vat_status
- paye_status
- uif_status
- employee_count
- turnover_estimate
- previous filings

Snapshot stored in `rule_inputs`.

---

## Rule Examples

### Annual Return (CIPC)

Trigger:
- Registered company

Due:
- Anniversary month of registration

Frequency:
- Annual

Evidence required:
- Submission confirmation
- Payment receipt

---

### VAT Filing

Trigger:
- VAT registered

Frequency:
- Bi-monthly or monthly

Due:
- 25th of following period

Evidence:
- VAT201 submission
- Payment receipt

---

### PAYE

Trigger:
- Employer registered
- Payroll exists

Frequency:
- Monthly

Due:
- 7th of following month

Evidence:
- EMP201
- Payment confirmation

---

### UIF

Trigger:
- Employees present

Frequency:
- Monthly

Due:
- 7th

Evidence:
- UIF declaration

---

## Obligation Generation

Engine runs:

- On workspace creation
- On company registration
- On rule change
- On status change (e.g. VAT registered)
- Daily scheduled recompute

For each workspace:
- Evaluate all rules
- Create or update obligations

---

## Obligation Deduplication

Each obligation must be unique by:

- workspace_id
- obligation_definition_id
- period_start
- period_end

Idempotency required.

---

## Obligation State Transitions

### Planned → DueSoon
Time-based trigger.

### DueSoon → Due
Due date reached.

### Due → InProgress
Filing started.

### InProgress → Submitted
Submission recorded.

### Submitted → Completed
Confirmation received.

### Any → Failed
Error or rejection.

### Any → Waived
Manual override.

---

## Filing Model

A filing represents an attempt.

Fields:
- filing_id
- obligation_id
- status
- prepared_by
- submitted_by
- external_reference
- evidence_documents
- error_code

---

## Evidence Requirements

Each obligation definition lists required evidence types.

Examples:
- Confirmation PDF
- Screenshot
- Receipt
- Reference number

Filing cannot complete without required evidence.

---

## Notification Engine

Notifications triggered at:

- 30 days before due
- 7 days before due
- Due date
- Overdue

Channels:
- Email
- In-app
- SMS (optional)

---

## Escalation

If obligation overdue:

- Create Task
- Increase severity
- Notify operator
- Notify client

If still unresolved:
- Mark high risk
- Require manual intervention

---

## Waivers

Operators can waive obligations.

Required:
- Reason code
- Notes
- Approver

Audit event required.

---

## Integration Hooks

On obligation events emit:

- ObligationCreated
- ObligationDueSoon
- ObligationOverdue
- FilingSubmitted
- FilingCompleted
- FilingFailed

---

## Data Model Summary

Entities:

- ObligationDefinition
- Obligation
- Filing
- ComplianceCalendarEntry
- EvidenceDocument

---

## Recompute Strategy

Daily job:

For each workspace:
- Load company state snapshot
- Evaluate rules
- Upsert obligations
- Emit events

---

## Failure Handling

If rule engine fails:
- Log error
- Retry
- Alert ops

If obligation creation fails:
- Dead letter queue
- Manual review

---

## Metrics

Track:

- Obligations created
- On-time completion rate
- Late filings
- Failure rate
- Average resolution time

---

## Operator Console Needs

- View obligations by workspace
- Filter by due date
- Bulk actions
- Manual filing recording
- Waiver tools

---

## Security

- Only authorized roles can mark completed
- Evidence required
- Audit logs mandatory

---

## Future Extensions

- Auto-filing integrations
- Accounting sync
- Revenue-based triggers
- Credit scoring signals
- Risk scoring

---

## Minimum Guarantees

- Every registered company has obligations
- Every obligation has a due date
- No completion without evidence
- All transitions logged
- Rules versioned
