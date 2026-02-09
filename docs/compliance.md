# Regulatory Compliance Model

## Purpose

Defines regulatory constraints for operating in South Africa.

Covers:
- FICA
- POPIA
- Mandates
- Record retention
- Liability boundaries

This informs product design, UI flows, and backend logic.

---

## FICA

Financial Intelligence Centre Act requirements.

Applies when:
- Acting on behalf of clients
- Handling identity documents
- Banking referrals

Requirements:
- Verify identity
- Keep records
- Store documents securely
- Track mandates

KYC must include:
- ID verification
- Proof of address
- Director checks

---

## POPIA

Protection of Personal Information Act.

Principles:

### Purpose limitation
Only collect data needed.

### Consent
User must agree to:
- Data storage
- Processing
- Sharing with banks or SARS

### Access rights
User can request:
- Data access
- Correction
- Deletion (where allowed)

### Retention
Keep data only as long as required.

---

## Mandates

Required before acting for client.

Types:
- CIPC agent
- SARS practitioner
- Bank referral

Mandate must include:
- Signature
- Scope
- Date
- Evidence document

No filing without mandate.

---

## Record Retention

Documents must be retained:

Typical minimum:
- 5 years (tax)
- 7 years (financial)
- Longer for legal disputes

Retention policy per document type.

---

## Consent Tracking

Store:
- Consent type
- Timestamp
- Scope
- Revocation

Required for:
- Bank linking
- Data sharing
- Tax representation

---

## Liability Boundaries

System must:

- Record client-provided data
- Record operator actions
- Require confirmation for filings
- Provide audit trail

Stalela provides:
- Operational support
- Not legal advice

---

## UI Requirements

Must show:
- What data is collected
- Why
- How used
- How long stored

Must allow:
- Consent
- Revocation
- Download

---

## Backend Constraints

- No action without mandate
- No filing without verified data
- No bank linking without consent
- All actions logged

---

## Data Subject Requests

Support:
- Access request
- Correction
- Deletion (where allowed)

Process:
- Verify requester
- Log request
- Fulfill within SLA

---

## Minimum Guarantees

- Mandate required for filings
- Consent required for data sharing
- Retention enforced
- Audit logs retained
