# Event Model

## Purpose
Defines canonical domain events.

All events must be immutable.

---

## Core Events

CompanyCreated
CompanyRegistered
RegistrationSubmitted
RegistrationFailed

ObligationCreated
ObligationDue
FilingSubmitted
FilingCompleted

BankOnboardingStarted
BankOnboardingApproved
BankAccountLinked

DocumentUploaded
MandateSigned

---

## Event Structure

event_id  
event_type  
timestamp  
tenant_id  
workspace_id  
actor  
payload  

---

## Guarantees

- Idempotent consumers
- Ordered per workspace
- Stored for replay
