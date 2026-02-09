# Security Architecture

## Purpose

This document defines the security model for Stalela.

The system handles:
- South African ID numbers
- Passports
- Corporate records
- Tax identifiers
- Banking metadata
- Legal documents

Security is a core architectural requirement.  
All Copilot agents and contributors must treat this document as a hard constraint layer.

---

## Threat Model

Primary risks:

- Exposure of identity documents
- Unauthorized filings
- Insider misuse
- Credential theft
- Automation misuse
- Data exfiltration
- Tenant data leakage

Security model must assume:
- Multi-tenant environment
- High‑value identity data
- Legal liability for errors

---

## Identity Model

### User
Authenticated human actor.

### Person
Real-world individual tied to a business.

### Operator
Internal staff with restricted scopes.

### System Actor
Automations and background jobs.

### RPA Actor
Automation identity with limited permissions.

---

## Authentication

Required:
- Email/password
- MFA mandatory for operators
- Step‑up auth for high-risk actions

Supported:
- TOTP
- Magic link (optional)
- SSO (future)

Session rules:
- Short-lived tokens
- Device tracking
- Session revocation support

---

## Authorization Model

RBAC with workspace scoping.

Roles:
- Owner
- Admin
- Accountant
- Operator
- Viewer

Permissions must be:
- Explicit
- Least-privilege
- Logged

High-risk actions:
- Company submissions
- Director changes
- Document downloads (IDs)
- Bank linking

Require:
- Step-up authentication
- Audit logging

---

## Data Classification

### Restricted
- ID numbers
- Passports
- Tax numbers
- Bank identifiers
- Mandates

### Sensitive
- Addresses
- Contact info
- Financials

### Standard
- Company name
- Registration number

---

## Encryption

### At Rest
- Database encryption
- Field-level encryption for ID numbers
- Object storage encryption
- Key management via KMS

### In Transit
- TLS everywhere
- No plaintext internal traffic

---

## Secrets Management

- All credentials in vault
- No secrets in repo
- Rotation policy required
- RPA credentials scoped and ephemeral

---

## Document Storage

Documents stored in object storage.

Requirements:
- Encrypted
- Signed URLs
- Time-limited access
- Access logging
- Hash verification

Never store:
- Documents inline in DB
- Base64 documents in logs

---

## Audit Logging

All sensitive actions must log:

- Actor
- Timestamp
- Workspace
- Action
- Target
- IP (optional)

Audit logs must be:
- Immutable
- Queryable
- Retained long-term

---

## RPA Security

- Separate credentials
- Vault access only
- Screenshots redacted
- No persistent sessions
- Human approval for submit

---

## Logging Rules

Never log:
- Full ID numbers
- Full account numbers
- Tokens
- Passwords

Mask:
- ID numbers
- Emails
- Bank refs

---

## Data Access Policies

- Access only when needed
- Purpose-bound access
- Document download tracked
- Admin overrides logged

---

## Incident Response

If breach suspected:

1. Disable access
2. Rotate credentials
3. Notify stakeholders
4. Preserve logs
5. Investigate

---

## Minimum Guarantees

- No plaintext IDs stored
- All documents encrypted
- Every sensitive action logged
- MFA for operators
- Vault for secrets
