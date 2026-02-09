# Company Registration Workflow

## Purpose

This document defines the end‑to‑end workflow for registering a business through Stalela.  
It covers:

- Client intake
- KYC and mandate collection
- Data validation
- CIPC/BizPortal submission (manual or RPA‑assisted)
- Post‑registration state updates
- Downstream compliance and banking triggers

The workflow is designed to be deterministic, auditable, and safe for human‑in‑the‑loop execution.

This is not legal advice. It is an operational workflow specification.

---

## Workflow Goals

1. Collect complete and structured company data.
2. Validate identities and authority.
3. Generate correct submission payloads.
4. Submit to CIPC/BizPortal via human or RPA.
5. Capture evidence and confirmation.
6. Create canonical company state in Stalela.
7. Trigger downstream obligations.

---

## Workflow Definition

**Workflow name:** `company_registration`  
**Category:** Formation  
**Risk level:** High  
**Requires human approval:** Yes  
**Primary external systems:** CIPC, BizPortal  

---

## Actors

- SME client
- Accountant/practice
- Internal operator
- RPA runner (optional)
- System automation

---

## Inputs

### Required inputs

Company details:
- Proposed company names (1–4)
- Company type (Pty Ltd default)
- Financial year end
- Registered address
- Business address (optional if same)
- Share structure

Directors:
- Full names
- SA ID or passport
- Residential address
- Email
- Phone

Shareholders:
- Person or entity
- Share allocation

Authorized representative:
- May be a director or external rep

Documents:
- ID documents for directors
- Proof of address
- Signed mandate
- Incorporation resolution

---

## Output Artifacts

- CIPC registration number
- Company registration certificate
- MOI
- Confirmation documents
- Canonical CompanyProfile created
- Initial compliance obligations generated

---

## State Machine

### States

1. `NOT_STARTED`
2. `INTAKE_IN_PROGRESS`
3. `AWAITING_KYC`
4. `AWAITING_MANDATE`
5. `DATA_VALIDATION`
6. `READY_FOR_SUBMISSION`
7. `SUBMISSION_IN_PROGRESS`
8. `SUBMITTED`
9. `REJECTED`
10. `REGISTERED`
11. `POST_REGISTRATION_SETUP`
12. `COMPLETED`
13. `FAILED`
14. `CANCELLED`

---

## Detailed Flow

### Step 1: Workspace creation

System creates:
- Tenant
- Workspace
- Draft CompanyProfile version 1

Status → `INTAKE_IN_PROGRESS`

---

### Step 2: Data capture

Client or operator enters:

- Company name options
- Directors
- Shareholders
- Addresses
- Contact details

System actions:
- Validate required fields
- Create Person records
- Create PersonRoles
- Upload documents to vault

---

### Step 3: KYC verification

For each director and authorized rep:

- Identity document uploaded
- Proof of address uploaded
- Screening checks (optional)
- KYCProfile updated

Status transitions:
- If all verified → proceed
- If any failed → `FAILED`
- If manual review needed → `AWAITING_KYC`

---

### Step 4: Mandate collection

Client signs authorization allowing Stalela/practice to act.

Create Mandate record:
- Type: `CIPC_AGENT`
- Evidence document stored
- Signed_at timestamp

Status → `DATA_VALIDATION`

---

### Step 5: Data validation

System validates:

- Director eligibility
- Share totals = 100%
- Required documents present
- Address formats valid
- Names meet CIPC rules

If issues:
- Create Task
- Status → `INTAKE_IN_PROGRESS`

If valid:
- Status → `READY_FOR_SUBMISSION`

---

### Step 6: Name reservation (optional)

Operator or RPA:

- Submit name reservation
- Capture outcome

Outcomes:
- Approved → continue
- Rejected → request new names

---

### Step 7: Submission preparation

System generates submission payload:

- Company data
- Director data
- Shareholder data
- Documents bundle

Create SubmissionRecord:
- target_system = CIPC/BizPortal
- status = PREPARED

Human approval required.

---

### Step 8: Submission execution

Two modes:

#### Manual mode
Operator logs into portal and submits.

#### RPA mode
Playwright runner:
- Logs in
- Fills forms
- Uploads docs
- Submits
- Captures screenshots

SubmissionRecord updated:
- status = SUBMITTED
- external_reference stored

Workflow → `SUBMITTED`

---

### Step 9: Outcome handling

Possible outcomes:

#### Approved
- Registration number returned
- Certificate downloaded
- MOI stored

Create CompanyProfile version:
- status = REGISTERED
- registration_number set

Workflow → `REGISTERED`

#### Rejected
- Capture rejection reason
- Create Task for correction
- Workflow → `REJECTED`

#### Timeout/unknown
- Operator follow‑up
- Remains in `SUBMITTED`

---

### Step 10: Post‑registration setup

System actions:

- Generate compliance obligations:
  - Annual return
  - SARS income tax
- Create tasks:
  - Bank account onboarding
  - VAT threshold monitoring

Upload:
- Registration certificate
- MOI

Status → `POST_REGISTRATION_SETUP`

---

### Step 11: Completion

Workflow → `COMPLETED`

Outputs:
- Active CompanyProfile
- Compliance calendar initialized
- Workspace ready for operations

---

## Error States

### Data errors
- Missing director ID
- Invalid share allocation

→ Return to intake

### Portal errors
- Captcha failure
- Portal downtime

→ Retry with backoff  
→ Escalate to operator

### Legal errors
- Director disqualified
- Name blocked

→ Manual review

---

## RPA Constraints

- Must use practitioner account
- Must log every action
- Must store screenshots
- Must never fabricate data
- Must require human approval before final submission

---

## Audit Requirements

Every step must log:

- Actor
- Timestamp
- Data used
- Outcome
- Evidence

SubmissionRecord must include:
- Payload hash
- Screenshots
- Confirmation PDFs

---

## Downstream Triggers

On successful registration:

Create obligations:
- CIPC annual return
- SARS registration follow‑up

Create tasks:
- Bank onboarding referral
- Accounting setup
- VAT monitoring

Emit events:
- CompanyRegistered
- ComplianceInitialized

---

## Cancellation

Workflow may be cancelled if:

- Client withdraws
- Payment fails
- KYC fails permanently

Status → `CANCELLED`

---

## Minimum Guarantees

- No submission without signed mandate.
- No submission without validated directors.
- Every submission has evidence.
- Every rejection produces a task.
- CompanyProfile updated only after confirmation.
