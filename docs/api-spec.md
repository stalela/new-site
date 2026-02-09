# Internal API Spec

## Company Service

POST /companies
Input:
- workspace_id
- company_profile

Output:
- company_profile_id

GET /companies/{id}
Output:
- company_profile

---

## Document Service

POST /documents
Input:
- workspace_id
- file
- document_type

Output:
- document_id

GET /documents/{id}
Returns metadata + signed URL

---

## Compliance Service

GET /obligations/{workspace_id}
Returns obligations list

POST /filings
Input:
- obligation_id
- evidence_docs

Output:
- filing_id

---

## Banking Service

POST /bank-onboarding
Input:
- workspace_id
- bank_name

Output:
- onboarding_case_id

POST /bank-link
Input:
- workspace_id
- provider_token

Output:
- bank_account_id

---

## Events

Services must emit events defined in event-model.md
