# Banking Orchestration

## Purpose

This document defines how Stalela handles business banking across the SME lifecycle.

The system does NOT directly create bank accounts through public APIs.  
Instead it orchestrates:

- Bank account onboarding preparation
- Referral workflows
- Document packaging
- Account linking after creation
- Transaction sync (with consent)
- Compliance visibility

The goal is to make banking operationally seamless for SMEs while respecting regulatory constraints.

---

## Design Principles

### Partnership-first
Business bank account creation is handled by licensed banks.  
Stalela prepares and routes clean onboarding packages.

### Data once
Company and director data collected during registration should never be re-entered.

### Track state
Every banking interaction must have a tracked lifecycle.

### Consent-driven
No banking data is accessed without explicit workspace consent.

### Bank-agnostic
Architecture must support multiple banks and aggregators.

---

## Core Concepts

- BankOnboardingCase
- BankAccount
- BankConsent
- Transaction (optional storage)
- IntegrationConnection

---

## Workflow: Bank Onboarding

### Actors
- Client
- Internal operator
- Bank partner
- System automation

### Inputs
- Registered company
- Directors KYC complete
- Mandate signed
- Required documents uploaded

---

## State Machine

States:

1. NOT_STARTED
2. PREPARING_PACKAGE
3. REFERRED_TO_BANK
4. PENDING_VERIFICATION
5. APPROVED
6. REJECTED
7. ACCOUNT_ACTIVE
8. CLOSED

---

## Step-by-Step Flow

### Step 1: Trigger

Triggered when:
- Company registered
- Client requests bank account
- Operator initiates onboarding

Create BankOnboardingCase.

---

### Step 2: Document bundle

System assembles:

- Registration certificate
- MOI
- Director IDs
- Proof of address
- Resolution
- Contact info

Store bundle as Document.

---

### Step 3: Referral

Two patterns:

#### Manual referral
Operator sends package to banker.

#### Integrated referral
System sends payload via partner API.

Update case → REFERRED_TO_BANK

---

### Step 4: Bank verification

Bank performs:
- FICA
- Risk checks
- Director verification

System tracks status manually or via webhook.

---

### Step 5: Outcome

#### Approved
- Account details returned
- Create BankAccount record
- Status → ACCOUNT_ACTIVE

#### Rejected
- Capture reason
- Create task
- Status → REJECTED

---

## BankAccount Model

Fields:

- bank_account_id
- workspace_id
- bank_name
- account_type
- masked_account_number
- status
- linked_at
- provider

---

## Account Linking

After account creation:

### Linking methods
- Open banking aggregator
- Direct bank integration
- Manual entry

Create BankConsent.

---

## BankConsent

Fields:

- consent_id
- workspace_id
- provider
- scopes
- granted_at
- revoked_at
- status

No transaction sync without active consent.

---

## Transaction Sync (Optional)

If enabled:

- Pull transactions
- Normalize
- Store or proxy

Fields:

- transaction_id
- bank_account_id
- amount
- currency
- posted_at
- description
- source_payload

---

## Integration Patterns

### Aggregators
Example:
- Stitch
- Ozow
- Others

### Direct bank APIs
Partner agreements required.

---

## Security

- Encrypt account identifiers
- Do not store full account numbers
- Use tokenized linking
- Log access

---

## Notifications

Trigger:

- On referral
- On approval
- On rejection
- On disconnect

---

## Failure Handling

### Missing documents
Create task.

### Bank rejection
Update case.
Notify client.

### Consent revoked
Disable sync.

---

## Metrics

Track:

- Referral → approval time
- Approval rate
- Rejection reasons
- Linked account count

---

## Future Extensions

- Payment initiation
- Reconciliation engine
- Lending signals
- Cash flow analytics

---

## Minimum Guarantees

- No account linking without consent.
- Every referral tracked.
- Every status change logged.
- No storage of full credentials.
