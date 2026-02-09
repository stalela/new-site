# Stalela Domain Model

## Purpose

This document defines the canonical domain model for Stalela, an operating system for South African SMEs and other registered businesses. It is designed to:

- Provide a single source of truth for core business identity, compliance obligations, filings, documents, and integrations.
- Enable deterministic workflow orchestration and reliable auditability.
- Support multi-tenant usage by direct SMEs, accounting practices, and internal operations teams.
- Enable future public APIs and partner integrations.

This model is written as a conceptual schema. Implementation may map these entities into relational tables, event streams, and derived views.

## Modeling Conventions

- **Tenant** is the billing and ownership boundary.
- **Workspace** is the business context. A tenant may have multiple workspaces.
- **Person** represents an individual and may have roles across workspaces.
- All entities include **metadata** such as `created_at`, `updated_at`, `created_by`, `source`, and `version` where relevant.
- Sensitive fields should be stored encrypted, and access must be logged.

## Multi-Tenancy and Identity Boundaries

### Tenant
Represents a paying customer or practice.

Key properties:
- `tenant_id`
- `tenant_type`: `SME`, `PRACTICE`, `ENTERPRISE`, `INTERNAL`
- `legal_name`
- `billing_email`
- `subscription_plan_id`
- `status`: `ACTIVE`, `SUSPENDED`, `CLOSED`
- `default_currency`
- `country`: default `ZA`

Relationships:
- Tenant has many Users
- Tenant has many Workspaces
- Tenant has many BillingArtifacts

### User
Represents a login identity.

Key properties:
- `user_id`
- `tenant_id`
- `email`
- `phone`
- `mfa_enabled`
- `status`: `ACTIVE`, `INVITED`, `DISABLED`
- `last_login_at`

Relationships:
- User has many WorkspaceMemberships
- User has many AuditEvents as actor

### Workspace
Represents a business context. A workspace is the canonical "company object container".

Key properties:
- `workspace_id`
- `tenant_id`
- `display_name`
- `workspace_type`: `COMPANY`, `SOLE_PROP`, `NON_PROFIT`, `TRUST` (extend as needed)
- `status`: `ACTIVE`, `ARCHIVED`
- `primary_company_profile_id`
- `default_timezone`
- `industry_code` (optional)

Relationships:
- Workspace has one or more CompanyProfiles (versioned)
- Workspace has many Persons in roles (Directors, Shareholders, Representatives)
- Workspace has many Documents
- Workspace has many Workflows, Tasks, Obligations, Filings
- Workspace has many IntegrationConnections
- Workspace has many BankAccounts (linked)
- Workspace has many Mandates

### WorkspaceMembership
Connects users to workspaces with roles.

Key properties:
- `workspace_membership_id`
- `workspace_id`
- `user_id`
- `role`: `OWNER`, `ADMIN`, `ACCOUNTANT`, `OPERATOR`, `VIEWER`
- `scopes` (fine-grained permissions)
- `status`: `ACTIVE`, `INVITED`, `REVOKED`

## Core Business Identity Model

### CompanyProfile
Versioned snapshot of a workspace’s legal identity and registrations.

Key properties:
- `company_profile_id`
- `workspace_id`
- `version`
- `legal_name`
- `trading_name`
- `company_type`: `PTY_LTD`, `NPC`, `CC`, `SOLE_PROP`, `TRUST` (extend)
- `registration_number` (CIPC)
- `registered_address`
- `business_address`
- `postal_address`
- `incorporation_date`
- `status`: `DRAFT`, `REGISTERED`, `DEREGISTERED`, `IN_LIQUIDATION`
- `source_of_truth`: `CLIENT`, `CIPC`, `BIZPORTAL`, `OPERATOR`

Registrations (embedded or linked):
- `sars_income_tax_number`
- `vat_registration` (link to VatRegistration)
- `paye_registration` (link to PayeRegistration)
- `uif_registration` (link to UifRegistration)
- `other_registrations` (extensible list)

Relationships:
- CompanyProfile links to Persons in roles (directors, shareholders, signatories)
- CompanyProfile links to core legal documents (certificate, MOI, resolutions)

### Address
Reusable value object.

Properties:
- `line1`, `line2`
- `suburb`
- `city`
- `province`
- `postal_code`
- `country` default `ZA`
- `address_type`: `REGISTERED`, `BUSINESS`, `POSTAL`, `RESIDENTIAL`
- `verified`: boolean
- `verification_source`

### Person
Represents an individual involved with one or more workspaces.

Key properties:
- `person_id`
- `full_name`
- `date_of_birth` (sensitive)
- `nationality`
- `id_type`: `SA_ID`, `PASSPORT`, `ASYLUM`, `OTHER`
- `id_number` (sensitive)
- `phone`
- `email`
- `residential_address`
- `pep_status` (politically exposed person flag, optional)
- `sanctions_screening_status` (optional)
- `status`: `ACTIVE`, `INACTIVE`

Relationships:
- Person has many PersonRoles
- Person has many IdentityDocuments
- Person may be linked to a User (optional)

### PersonRole
Assigns a Person a role in a workspace and optionally a company profile version.

Key properties:
- `person_role_id`
- `workspace_id`
- `company_profile_id` (optional, for version scoping)
- `person_id`
- `role_type`: `DIRECTOR`, `SHAREHOLDER`, `AUTHORIZED_REP`, `BENEFICIAL_OWNER`, `CONTACT_PERSON`
- `start_date`
- `end_date`
- `status`: `ACTIVE`, `ENDED`

Role-specific attributes:
- For `SHAREHOLDER`: share class, share count, percentage
- For `DIRECTOR`: appointment date, resignation date
- For `AUTHORIZED_REP`: authority level, mandate link

### Mandate
Evidence that Stalela or a practice is authorized to act on behalf of a workspace.

Key properties:
- `mandate_id`
- `workspace_id`
- `mandate_type`: `CIPC_AGENT`, `SARS_PRACTITIONER`, `BANKING_REFERRAL`, `GENERAL_ADMIN`
- `signed_at`
- `expires_at` (optional)
- `status`: `ACTIVE`, `EXPIRED`, `REVOKED`
- `evidence_document_id`
- `signatory_person_id`
- `notes`

## KYC, Identity, and Screening

### KycProfile
Tracks KYC status for a Person or Workspace.

Key properties:
- `kyc_profile_id`
- `subject_type`: `PERSON`, `WORKSPACE`
- `subject_id`
- `kyc_level`: `BASIC`, `STANDARD`, `ENHANCED`
- `status`: `NOT_STARTED`, `IN_PROGRESS`, `PASSED`, `FAILED`, `REVIEW_REQUIRED`
- `last_checked_at`
- `provider`: `MANUAL`, `VERIFF`, `OTHER`
- `failure_reason_code` (optional)

### IdentityDocument
Represents a specific identity document for a person.

Key properties:
- `identity_document_id`
- `person_id`
- `document_type`: `SA_ID_FRONT`, `SA_ID_BACK`, `PASSPORT`, `PROOF_OF_ADDRESS`, `SELFIE`, `OTHER`
- `document_id` (link to Document vault)
- `issue_date`, `expiry_date`
- `status`: `VALID`, `EXPIRED`, `REJECTED`
- `verification_status`: `NOT_VERIFIED`, `VERIFIED`, `FAILED`
- `verification_details` (structured)

### ScreeningResult
Optional entity to record PEP/sanctions/adverse media checks.

Key properties:
- `screening_result_id`
- `person_id`
- `screening_type`: `PEP`, `SANCTIONS`, `ADVERSE_MEDIA`
- `status`: `CLEAR`, `POSSIBLE_MATCH`, `MATCH`
- `checked_at`
- `provider`
- `evidence`

## Document Vault Model

### Document
Represents a stored file and its metadata.

Key properties:
- `document_id`
- `workspace_id`
- `owner_type`: `WORKSPACE`, `PERSON`
- `owner_id`
- `document_type`: `CIPC_CERTIFICATE`, `MOI`, `RESOLUTION`, `TAX_NOTICE`, `VAT_CERTIFICATE`, `ID_DOC`, `PROOF_OF_ADDRESS`, `BANK_LETTER`, `OTHER`
- `file_name`
- `mime_type`
- `size_bytes`
- `storage_uri`
- `sha256_hash`
- `sensitivity_level`: `LOW`, `MEDIUM`, `HIGH`, `RESTRICTED`
- `status`: `ACTIVE`, `REPLACED`, `DELETED`
- `issued_at` (optional)
- `expires_at` (optional)
- `retention_policy_id`
- `access_policy_id`

Relationships:
- Document may be referenced by Workflows, Filings, Mandates, KYC profiles
- Document may have derived artifacts (OCR text, thumbnails) stored separately

### DocumentArtifact
Derived data from a document.

Key properties:
- `document_artifact_id`
- `document_id`
- `artifact_type`: `OCR_TEXT`, `THUMBNAIL`, `REDACTED_COPY`, `INDEXED_TEXT`
- `storage_uri`
- `created_at`
- `status`

### RetentionPolicy
Defines how long to keep documents.

Key properties:
- `retention_policy_id`
- `name`
- `min_retention_days`
- `legal_basis` (optional)
- `delete_behavior`: `HARD_DELETE`, `SOFT_DELETE`, `ARCHIVE_ONLY`

### AccessPolicy
Defines allowed access patterns.

Key properties:
- `access_policy_id`
- `name`
- `allowed_roles`
- `purpose_limitations` (e.g. `COMPLIANCE`, `BANKING`, `KYC`)
- `requires_step_up_auth`: boolean

## Workflow and Operations Model

### WorkflowDefinition
A template for a state machine.

Key properties:
- `workflow_definition_id`
- `name`
- `version`
- `category`: `FORMATION`, `COMPLIANCE`, `BANKING`, `CHANGES`, `INTEGRATION`
- `input_schema` (json schema or equivalent)
- `states` (declarative representation)
- `transitions`
- `risk_level`: `LOW`, `MEDIUM`, `HIGH`
- `requires_human_approval`: boolean

### WorkflowInstance
A running workflow.

Key properties:
- `workflow_instance_id`
- `workflow_definition_id`
- `workspace_id`
- `status`: `NOT_STARTED`, `RUNNING`, `WAITING_APPROVAL`, `PAUSED`, `COMPLETED`, `FAILED`, `CANCELLED`
- `current_state`
- `input_payload` (validated)
- `context` (runtime data)
- `idempotency_key`
- `started_at`, `completed_at`
- `failure_reason_code` (optional)

Relationships:
- WorkflowInstance has many WorkflowEvents
- WorkflowInstance has many Tasks
- WorkflowInstance references Documents and external SubmissionRecords

### WorkflowEvent
Immutable record of state transitions and actions.

Key properties:
- `workflow_event_id`
- `workflow_instance_id`
- `event_type`: `STATE_TRANSITION`, `ACTION`, `APPROVAL_REQUESTED`, `APPROVED`, `REJECTED`, `ERROR`
- `from_state`, `to_state`
- `actor_type`: `USER`, `OPERATOR`, `SYSTEM`, `RPA`
- `actor_id`
- `timestamp`
- `details` (structured)
- `evidence_document_id` (optional)

### Task
Human work item.

Key properties:
- `task_id`
- `workspace_id`
- `workflow_instance_id` (optional)
- `task_type`: `REVIEW_DOCS`, `SUBMIT_CIPC`, `SUBMIT_SARS`, `FOLLOW_UP_BANK`, `CALL_CLIENT`, `FIX_DATA`
- `status`: `OPEN`, `IN_PROGRESS`, `BLOCKED`, `DONE`, `CANCELLED`
- `priority`: `LOW`, `NORMAL`, `HIGH`, `URGENT`
- `assigned_to_user_id` (optional)
- `due_at`
- `notes`
- `resolution_code` (optional)

### SubmissionRecord
Tracks an external submission attempt.

Key properties:
- `submission_record_id`
- `workspace_id`
- `workflow_instance_id`
- `target_system`: `CIPC`, `BIZPORTAL`, `SARS`, `BANK`, `OTHER`
- `submission_type`: `REGISTRATION`, `FILING`, `UPDATE`
- `status`: `PREPARED`, `SUBMITTED`, `ACKNOWLEDGED`, `REJECTED`, `FAILED`
- `submitted_at`
- `external_reference` (tracking number)
- `payload_hash`
- `evidence_document_id` (screenshots, confirmation PDFs)
- `error_details` (structured)

## Compliance Model

### ObligationDefinition
Defines a compliance obligation type.

Key properties:
- `obligation_definition_id`
- `name`
- `authority`: `CIPC`, `SARS`, `UIF`, `OTHER`
- `frequency`: `ONCE`, `MONTHLY`, `BI_MONTHLY`, `QUARTERLY`, `ANNUAL`, `AD_HOC`
- `rule_version`
- `required_evidence_types`
- `risk_level`

### Obligation
Concrete instance for a workspace.

Key properties:
- `obligation_id`
- `workspace_id`
- `obligation_definition_id`
- `period_start`, `period_end`
- `due_date`
- `status`: `PLANNED`, `DUE_SOON`, `DUE`, `IN_PROGRESS`, `SUBMITTED`, `COMPLETED`, `FAILED`, `WAIVED`
- `computed_at`
- `rule_inputs` (snapshot)
- `waiver_reason` (optional)
- `linked_filing_id` (optional)

### Filing
Represents an attempt to satisfy an obligation.

Key properties:
- `filing_id`
- `workspace_id`
- `obligation_id`
- `filing_type`: `ANNUAL_RETURN`, `VAT201`, `EMP201`, `UIF_DECLARATION`, `OTHER`
- `status`: `DRAFT`, `READY`, `SUBMITTED`, `ACCEPTED`, `REJECTED`, `FAILED`
- `prepared_by_user_id`
- `submitted_by_user_id` (or operator)
- `submitted_at`
- `external_reference`
- `amount_payable` (optional)
- `payment_status`: `NOT_REQUIRED`, `PENDING`, `PAID`, `FAILED`
- `evidence_document_ids` (confirmation, receipts)
- `error_reason_code` (optional)

### ComplianceCalendarEntry
Derived view for UI.

Key properties:
- `calendar_entry_id`
- `workspace_id`
- `obligation_id`
- `date`
- `label`
- `severity`
- `status`

## Tax and Registration Sub-Models

These may be embedded in CompanyProfile or stored as dedicated entities for richer history.

### VatRegistration
- `vat_registration_id`
- `workspace_id`
- `vat_number`
- `status`: `NOT_REGISTERED`, `PENDING`, `REGISTERED`, `DEREGISTERED`
- `effective_date`
- `filing_frequency`
- `certificate_document_id`

### PayeRegistration
- `paye_registration_id`
- `workspace_id`
- `paye_reference`
- `status`
- `effective_date`

### UifRegistration
- `uif_registration_id`
- `workspace_id`
- `uif_reference`
- `status`
- `effective_date`

## Banking Model

### BankAccount
Linked account metadata.

Key properties:
- `bank_account_id`
- `workspace_id`
- `bank_name`
- `account_type`: `BUSINESS_CURRENT`, `SAVINGS`, `OTHER`
- `masked_account_number`
- `account_holder_name`
- `status`: `NOT_LINKED`, `LINKED`, `DISCONNECTED`, `CLOSED`
- `linked_at`
- `link_provider`: `STITCH`, `DIRECT`, `MANUAL`
- `consent_id` (optional)

### BankOnboardingCase
Tracks the onboarding orchestration.

Key properties:
- `bank_onboarding_case_id`
- `workspace_id`
- `bank_name`
- `status`: `NOT_STARTED`, `REFERRED`, `PENDING_VERIFICATION`, `APPROVED`, `REJECTED`, `CLOSED`
- `started_at`
- `completed_at`
- `required_document_types`
- `submitted_bundle_document_id`
- `bank_reference_number`
- `notes`

### BankConsent
Records user consent for bank data access.

Key properties:
- `bank_consent_id`
- `workspace_id`
- `provider`
- `scopes`
- `granted_at`
- `revoked_at` (optional)
- `status`

### Transaction
Optional normalized transaction model (if storing).

Key properties:
- `transaction_id`
- `bank_account_id`
- `posted_at`
- `amount`
- `currency`
- `description`
- `category` (derived)
- `source_raw_payload` (optional)
- `dedupe_key`

## Integrations Model

### IntegrationDefinition
Catalog of supported integrations.

Key properties:
- `integration_definition_id`
- `name`
- `category`: `BANKING`, `ACCOUNTING`, `PAYROLL`, `PAYMENTS`, `KYC`, `NOTIFICATIONS`
- `auth_type`
- `supported_scopes`
- `webhook_supported`: boolean

### IntegrationConnection
A specific workspace connection.

Key properties:
- `integration_connection_id`
- `workspace_id`
- `integration_definition_id`
- `status`: `CONNECTED`, `DISCONNECTED`, `ERROR`
- `scopes_granted`
- `auth_secret_ref` (vault reference)
- `connected_at`
- `last_sync_at`
- `error_details` (optional)

### WebhookSubscription
Outbound webhook configuration for partners (phase later).

Key properties:
- `webhook_subscription_id`
- `tenant_id`
- `status`
- `target_url`
- `secret`
- `event_types`
- `created_at`

## Billing Model

### SubscriptionPlan
- `subscription_plan_id`
- `name`
- `monthly_price`
- `features`
- `limits`

### Subscription
- `subscription_id`
- `tenant_id`
- `subscription_plan_id`
- `status`
- `started_at`
- `cancelled_at` (optional)

### Invoice
- `invoice_id`
- `tenant_id`
- `period_start`, `period_end`
- `amount`
- `status`
- `payment_reference`

## Audit and Governance

### AuditEvent
Immutable log record.

Key properties:
- `audit_event_id`
- `tenant_id`
- `workspace_id` (optional)
- `actor_type`: `USER`, `SYSTEM`, `OPERATOR`, `RPA`
- `actor_id`
- `action`
- `target_type`
- `target_id`
- `timestamp`
- `ip_address` (optional)
- `user_agent` (optional)
- `details` (structured)
- `risk_level`

### DataExport
Tracks exports of sensitive data.

Key properties:
- `data_export_id`
- `tenant_id`
- `requested_by_user_id`
- `status`
- `created_at`
- `completed_at`
- `export_uri`
- `scope`
- `audit_event_id`

## Domain Events (Canonical)

Events are used for async processing and integrations. Key events include:

- `TenantCreated`
- `WorkspaceCreated`
- `CompanyProfileUpdated`
- `PersonAddedToWorkspace`
- `DocumentUploaded`
- `MandateSigned`
- `WorkflowStarted`
- `WorkflowStateChanged`
- `SubmissionRecorded`
- `ObligationCreated`
- `ObligationStatusChanged`
- `FilingCreated`
- `FilingSubmitted`
- `FilingOutcomeRecorded`
- `BankOnboardingCaseCreated`
- `BankAccountLinked`
- `IntegrationConnected`
- `PermissionGranted`
- `AuditEventRecorded`

Each event payload must include:
- `event_id`
- `event_type`
- `timestamp`
- `tenant_id`
- `workspace_id` (optional if tenant-only)
- `actor` (optional)
- `payload` (versioned)

## Minimum Invariants

- Every workspace has at least one CompanyProfile.
- Sensitive personal identifiers are encrypted and never exposed by default.
- Document binaries are never stored in the relational database.
- Every external submission has a SubmissionRecord and evidence artifact.
- Every compliance obligation has a definition and versioned rule inputs.
- Every high-risk action emits an AuditEvent.
