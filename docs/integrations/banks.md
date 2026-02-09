# Bank Integrations

## Purpose

Defines how Stalela integrates with South African banks and banking infrastructure.

Focus areas:
- Bank onboarding orchestration
- Account linking
- Transaction sync
- Consent management

Stalela does not create bank accounts directly.  
Banks remain the regulated entity.

---

## Integration Types

1. Referral integrations
2. Open banking aggregators
3. Direct bank APIs (partnership)
4. Manual linking

---

## Supported Banks (initial)

- FNB
- Standard Bank
- Nedbank
- Absa
- TymeBank (future)

---

## Core Entities

- BankOnboardingCase
- BankAccount
- BankConsent
- Transaction
- IntegrationConnection

---

## Workflow: Account Onboarding

1. Trigger onboarding
2. Collect documents
3. Package bundle
4. Send referral
5. Track status
6. Receive outcome
7. Link account

---

## Referral Models

### Manual referral
Operator sends docs to banker.

### Partner API referral
POST onboarding payload to partner endpoint.

---

## Account Linking

### Aggregator method
Use provider like Stitch.

Steps:
- User consents
- Redirect to provider
- Receive token
- Store connection

---

## Consent

Fields:
- provider
- scopes
- granted_at
- revoked_at
- status

No data sync without consent.

---

## Transaction Sync

Optional feature.

Process:
- Pull transactions via API
- Normalize
- Store or proxy
- Emit events

---

## Security

- Never store full account numbers
- Encrypt tokens
- Use vault for secrets
- Log access

---

## Failure Modes

### Bank rejects onboarding
Update case.
Create task.

### Consent revoked
Disable sync.

### API error
Retry with backoff.

---

## Events

- BankReferralCreated
- BankOnboardingApproved
- BankOnboardingRejected
- BankAccountLinked
- BankAccountDisconnected
- TransactionsSynced

---

## Future Extensions

- Payment initiation
- Lending integrations
- Cash flow analytics
- Reconciliation engine

---

## Minimum Guarantees

- Every onboarding tracked.
- No linking without consent.
- All changes audited.
