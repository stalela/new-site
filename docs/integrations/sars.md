# SARS Integration

## Purpose

Defines how Stalela interacts with SARS (South African Revenue Service) for:

- Income tax registration
- VAT registration and filing
- PAYE and UIF employer registrations
- Ongoing tax compliance workflows

SARS does not provide fully open APIs for general SaaS usage.  
Integration is typically done through:

- Practitioner access to SARS eFiling
- Manual submission
- Semi-automated workflows
- Future accredited partner integrations

This document defines safe operational patterns.

---

## Core Constraints

- SARS eFiling accounts tied to practitioner or company.
- Access requires authorization to act on behalf of client.
- Many workflows require OTP or manual verification.
- No public API for full tax lifecycle.
- Filing errors have financial consequences.

Stalela must treat SARS as a high‑risk external system.

---

## Practitioner Model

Stalela or partner accountants act as tax practitioners.

Requirements:
- Signed mandate from client
- Practitioner registered with SARS
- Access added to client profile
- Activity logged

No filing allowed without mandate.

---

## Supported Operations

### Registration
- Income tax registration
- VAT registration
- PAYE employer registration
- UIF employer registration

### Filing
- VAT201
- EMP201
- EMP501
- Income tax returns
- Supporting documents

### Status queries
- VAT status
- Account balance
- Compliance status

---

## VAT Rules

### Threshold
VAT required if turnover exceeds threshold (currently ZAR 1M annually).  
Engine must monitor turnover signals.

### Frequency
- Bi‑monthly default
- Monthly for large entities

### Due date
25th of following period.

### Evidence required
- Submission confirmation
- Payment receipt

---

## PAYE Rules

Trigger:
- Business hires employees

Frequency:
- Monthly

Due:
- 7th of following month

Evidence:
- EMP201
- Payment confirmation

---

## UIF Rules

Trigger:
- Employees present

Frequency:
- Monthly

Due:
- 7th

Evidence:
- UIF declaration

---

## Workflow: VAT Registration

1. Detect eligibility or manual trigger
2. Collect financial details
3. Validate directors
4. Generate submission package
5. Submit via practitioner portal
6. Capture reference number
7. Store VAT certificate
8. Update CompanyProfile

---

## Authentication

Methods:
- Practitioner login
- OTP
- Delegated access

Credentials stored in secure vault.  
Never hardcoded.

---

## Document Requirements

Common uploads:
- Registration certificate
- Bank confirmation letter
- ID documents
- Proof of address
- Financial statements (sometimes)

All documents must exist in vault before submission.

---

## Automation Strategy

### Manual
Operator submits via eFiling.

### Assisted
Playwright runner navigates portal with approval.

### Future
Accredited API integration.

---

## Error Handling

### Rejected registration
Capture reason.
Create task.

### Filing rejected
Mark Filing as FAILED.
Escalate.

### Payment mismatch
Create reconciliation task.

---

## Evidence Capture

Store:
- Reference numbers
- Screenshots
- PDFs
- Payment receipts

---

## Events

- TaxRegistrationStarted
- VATRegistered
- PAYERegistered
- FilingSubmitted
- FilingAccepted
- FilingRejected

---

## Minimum Guarantees

- No filing without mandate.
- No filing without validated data.
- Every submission logged.
- Evidence required for completion.
