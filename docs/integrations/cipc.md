# CIPC Integration

## Purpose

This document defines how Stalela interacts with the Companies and Intellectual Property Commission (CIPC) for company lifecycle operations.

CIPC is the authoritative registry for companies in South Africa.  
There is no stable public API for general startup use.  
Therefore the system must support:

- Manual submission workflows
- RPA-assisted submission (Playwright)
- Future partner/API integration if available

This document describes constraints, data mapping, and safe automation patterns.

---

## Core Constraints

- CIPC access requires a registered customer profile tied to a South African ID or authorized entity.
- Multiple companies can be registered under one practitioner account.
- Submissions are legally attributable to the account holder.
- Portal changes can break automation.
- Captchas and OTP may require human intervention.

Stalela must treat CIPC as a high-risk external dependency.

---

## Supported Operations

### Formation
- Name reservation
- Company registration
- MOI submission

### Maintenance
- Director changes
- Address updates
- Annual returns
- Company status queries

### Document retrieval
- Registration certificate
- Disclosure certificate
- Filed documents

---

## Integration Modes

### Manual
Operator logs into CIPC portal and submits data.

### Assisted RPA
Playwright runner performs actions with operator approval.

### Future API
If CIPC provides partner API access, adapter layer will be added.

---

## Account Management

Stalela uses practitioner accounts.

Requirements:
- Credentials stored in secure vault
- Access scoped per operator session
- MFA handled manually
- Activity logged

No automated creation of CIPC accounts.

---

## Data Mapping

### Company fields

| Stalela | CIPC |
|--------|------|
legal_name | company name |
company_type | entity type |
registered_address | registered address |
financial_year_end | FYE |
directors | directors |
shareholders | incorporators |

### Person fields

| Stalela | CIPC |
|--------|------|
full_name | name |
id_number | ID |
address | residential |
email | email |

---

## Workflow: Registration

1. Collect data in Stalela
2. Validate
3. Generate payload
4. Create SubmissionRecord
5. Execute manual or RPA submission
6. Capture result
7. Store documents
8. Update CompanyProfile

---

## RPA Guidelines

- Use Playwright
- Script deterministic steps
- Capture screenshots
- Capture HTML snapshots
- Log every action
- Require human approval before submit

---

## Failure Modes

### Name rejected
Return to intake.

### Portal error
Retry with backoff.

### Session timeout
Re-authenticate.

### Submission unclear
Create follow-up task.

---

## Evidence Capture

Must store:
- Screenshots
- Confirmation PDFs
- Reference numbers
- Timestamp

---

## Rate Limits

Avoid:
- High-frequency submissions
- Parallel submissions on same account

Implement queue.

---

## Security

- Credentials in vault
- Role-based access
- Audit logs
- Redact ID numbers in logs

---

## Future API Adapter

If API becomes available:

Adapter must:
- Map payloads
- Handle auth
- Store responses
- Maintain same workflow interface

---

## Events Emitted

- CIPCSubmissionPrepared
- CIPCSubmissionSent
- CIPCSubmissionSucceeded
- CIPCSubmissionFailed
- CIPCNameRejected
- CompanyRegistered

---

## Minimum Guarantees

- No submission without mandate.
- No submission without validated data.
- Every submission logged.
- Every response captured.
