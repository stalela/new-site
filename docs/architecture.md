# Stalela Architecture

## Purpose

Stalela is an operating system for South African SMEs and other registered businesses. It provides a single control plane for the administrative lifecycle of a business across formation, statutory maintenance, tax compliance, payroll compliance, banking orchestration, and document governance.

The system is built to support a phased delivery path:

- Phase 0: Data capture, document vault, manual operations with strong audit trails.
- Phase 1: Workflow orchestration and deadline automation, still human executed on government portals.
- Phase 2: Assisted automation using RPA for repetitive steps, with human approval gates.
- Phase 3: Partner and platform integrations (banks, payroll providers, accounting platforms).
- Phase 4: Domain APIs enabling third parties (accountants, fintechs, marketplaces) to build on top.

The architecture treats government portals as constrained, non-API systems. It optimizes for correctness, traceability, and controlled automation over raw speed.

## Guiding Principles

### Correctness over autonomy
Legal filings and registrations have real-world consequences. Automation is allowed only when inputs are validated and actions are logged. Human approval gates are default for high-risk actions.

### Deterministic workflows, optional intelligence
Workflows are encoded as explicit state machines. AI is used as an assistant for parsing, mapping, and recovery, not as the source of truth for business logic.

### Canonical company object
Stalela maintains a persistent, versioned representation of a business and its obligations. External systems are synchronized into this model.

### Auditability is a first-class feature
Every significant action must be attributable, replayable, and explainable with timestamps, actor identity, artifacts, and outcome.

### Privacy and least privilege
The platform handles sensitive personal and corporate records. Access is scoped to roles, purpose, and time.

## Primary User Segments

### SMEs and business owners
- Want a single place to see compliance status and outstanding actions.
- Want guided flows for registrations and filings.
- Want document storage and retrieval.

### Accountants and compliance practitioners
- Manage multiple clients.
- Need standardized intake, document collection, and reporting.
- Need activity logs and evidence of filings.

### Internal operations team
- Executes manual and semi-automated steps on government portals.
- Needs queue management, checklists, and exception handling.
- Needs visibility into SLA and error patterns.

### Integration partners
- Payroll, accounting, banking, payments, and lending partners.
- Need stable APIs, events, and data access controls.

## Core Product Capabilities

- Business intake and identity verification (KYC for directors and authorized reps).
- Document vault with structured metadata and lifecycle policies.
- Company formation workflow orchestration (manual first).
- Compliance calendar and obligation engine (annual returns, VAT, PAYE, UIF, etc.).
- Filing preparation, submission tracking, and evidence capture.
- Banking orchestration (referrals and account linking, not account creation).
- Multi-tenant experience for accountants and agencies.
- Notifications and task management.
- Integration and event platform.

## Non-Goals

- Replacing SARS eFiling, CIPC, or bank onboarding systems.
- Providing legal advice. The platform provides workflow and operational support.
- Fully autonomous agent-based filing without oversight.
- Real-time guarantees from government systems. The platform records and reconciles.

## High-Level Architecture

Stalela is a multi-tenant SaaS platform with a modular service architecture. It consists of:

- Web application (SME portal, accountant portal, operations console).
- API gateway (public and internal APIs).
- Domain services (company, documents, workflows, compliance, banking).
- Job system (queues, schedulers, workers).
- Automation runner (Playwright-based RPA with guardrails).
- Event bus (domain events for integrations and internal async processing).
- Data stores (relational for core state, object storage for files, search for discovery).
- Observability stack (logs, metrics, traces, audit trail).

### Component Diagram (conceptual)

- Clients
  - SME Web App
  - Accountant Web App
  - Operations Console
  - Partner API Clients

- Edge
  - CDN and WAF
  - API Gateway
  - Auth Service

- Core Platform
  - Company Service
  - Identity and KYC Service
  - Document Service
  - Workflow Orchestrator
  - Compliance Engine
  - Billing and Subscription Service
  - Notification Service
  - Banking Orchestration Service
  - Integration Service (connectors and webhooks)

- Automation
  - RPA Runner (Playwright)
  - Agent Assistant (optional AI tool, constrained)
  - Human Approval Service

- Data
  - Postgres (canonical business state)
  - Object Storage (documents)
  - Redis (caching and short-lived state)
  - Search Index (company and document search)
  - Data Warehouse (analytics, optional phase)

- Messaging
  - Event Bus (Kafka, NATS, or managed equivalent)
  - Job Queue (SQS/RabbitMQ, or managed equivalent)

## Tenancy and Data Partitioning

Stalela is multi-tenant. Tenants are either:

- A single business (direct SME customer).
- A practice or agency managing multiple businesses (accountant tenant).

Each business is a workspace entity. A tenant can have many workspaces. Permissions are granted per workspace.

Data partitioning requirements:

- Tenant-level isolation at the application layer is mandatory.
- Row-level security or tenant_id scoping is mandatory in the primary relational store.
- Document storage uses a tenant and workspace prefix plus envelope encryption.
- Audit logs are immutable and separated by tenant and workspace.

## Identity, Authentication, and Authorization

### Authentication
- Email and password with MFA.
- Optional SSO for enterprise practices (phase later).
- Session and token-based auth (JWT or opaque tokens via auth server).
- Device and session management for high-privilege operations users.

### Authorization
Role-based access control with workspace scoping. Example roles:

- Owner: full control in a workspace.
- Admin: manage users and settings, limited billing.
- Accountant: access assigned clients and compliance tools.
- Operator: internal operations staff, restricted to assigned tasks.
- Viewer: read-only access.

Policies:

- Least privilege by default.
- High-risk actions require step-up authentication and explicit confirmation.
- All access to documents and personal data is logged.

## Domain Model Overview

This is a simplified view. The full model belongs in `domain-model.md`.

### Core entities
- Tenant
- Workspace (Business)
- Person (Director, Shareholder, Authorized Representative)
- CompanyProfile (registration numbers, addresses, industry, tax status)
- Document (file metadata, classification, retention, hash)
- Mandate (authorization to act as agent on behalf of business)
- WorkflowInstance (state machine for a process)
- Task (human work item)
- Obligation (recurring or one-time compliance requirement)
- Filing (a concrete attempt to satisfy an obligation)
- BankAccount (linked account metadata)
- IntegrationConnection (tokens, consents, scopes)

### Canonical company object
A workspace has a canonical company object that aggregates:

- Legal identity: company name, registration number, type, registered address.
- People and roles: directors, shareholders, signatories.
- Registrations: tax number, VAT status, UIF, PAYE.
- Compliance status: obligations due, filings submitted, evidence links.
- Financial connections: bank links, accounting links, payroll links.
- Documents: MOI, certificates, proofs, resolutions.

## Workflow Orchestration

Workflows are explicit state machines. Each workflow has:

- Input schema with validations.
- States and transitions.
- Side effects (document generation, notifications, external submissions).
- Human checkpoints.
- Idempotency keys and retry behavior.
- Observability hooks.

### Examples of workflows
- Company registration (CIPC and BizPortal pathways).
- Annual return submission.
- VAT registration and filing.
- PAYE and UIF setup.
- Director changes.
- Bank account onboarding referral and linking.

### Workflow engine
Implementation options:

- A dedicated workflow engine (Temporal, Cadence, or similar).
- Or an internal orchestrator using a job queue and persisted state.

Design requirement:

- Workflow state must be persisted in Postgres.
- Every transition emits an event.
- All side effects are retried safely using idempotency keys.

## Compliance Engine

The compliance engine produces obligations for each workspace.

It has three layers:

1. Rules: Encodes triggers and schedules.
2. Evidence: Defines required artifacts and fields.
3. Execution: Creates tasks and filing attempts, then monitors outcomes.

### Rule examples
- Annual returns: due based on registration anniversary and company type.
- VAT: due based on registration, turnover threshold, and filing frequency.
- PAYE: due monthly once employer registered and payroll exists.
- UIF: due monthly with payroll data.

Rules are versioned. When rules change, the engine re-evaluates future obligations and records diffs.

### Obligation lifecycle
- Planned: computed and scheduled.
- DueSoon: within notification window.
- Due: now actionable.
- InProgress: filing being prepared or submitted.
- Submitted: evidence captured, awaiting confirmation.
- Completed: confirmed.
- Failed: error state with reason codes.
- Waived: manual override with justification.

## Document Vault

Documents are treated as regulated assets.

### Storage
- Files stored in object storage with server-side encryption and per-tenant keys.
- Metadata in Postgres including hashes for tamper detection.
- Optional content extraction for search, with redaction controls.

### Classification
Each document has:
- Type (ID, proof of address, company certificate, MOI, resolution, tax notice, etc.).
- Owner (person or workspace).
- Sensitivity level.
- Validity dates (issue, expiry).
- Use policy (which workflows may access it).

### Retention
Retention policy is configurable per document type and legal requirements. Deletion is controlled and logged. Some evidence must be retained for a minimum period.

## Banking Orchestration

Stalela does not create bank accounts via public APIs. It orchestrates onboarding.

### Capabilities
- Packaged onboarding bundle per bank: required docs, forms, and metadata.
- Referral flow: handoff to bank onboarding, tracked as a workflow.
- Account linking post-creation using open banking aggregators or partner APIs.
- Transaction sync for compliance and analytics, with consent.

### State model
- NotStarted
- Referred
- PendingVerification
- Approved
- Active
- Rejected
- Closed

Each transition requires evidence, such as bank confirmation or account linking success.

## RPA and Assisted Automation

Government portals may not provide APIs. Automation uses Playwright as a controlled runner.

### RPA runner design
- Runs in isolated, ephemeral environments.
- Uses a credential vault for portal logins.
- Captures screenshots and HTML snapshots on each step.
- Writes an execution trace as an immutable audit artifact.
- Uses deterministic scripts for the main path.

### AI-assisted layer
Optional, bounded tool usage:

- Reads page content.
- Maps form labels to fields.
- Interprets error messages.
- Suggests next steps within a predefined action set.

AI never invents input values. It can only choose from validated data already stored.

### Human approvals
High-risk points require human approval:
- Final submission actions.
- Any step involving ambiguous portal messages.
- Any upload of identity documents where portal feedback is inconsistent.

### Failure handling
- Retry only for safe, idempotent steps.
- Escalate to human operator for unknown errors.
- Record a structured failure reason code for analytics.

## Integration Layer

Integrations are built as connectors with a consistent contract:

- Authentication: OAuth, API keys, or managed credentials.
- Scopes: explicit data access scopes per connection.
- Data sync: pull, push, or webhook driven.
- Mapping: external data mapped into canonical model with provenance.

### Event-driven design
Domain events drive both internal processing and partner integrations.

Examples:
- CompanyCreated
- RegistrationSubmitted
- RegistrationApproved
- ObligationCreated
- ObligationDueSoon
- FilingSubmitted
- FilingFailed
- BankReferralCreated
- BankAccountLinked
- DocumentUploaded
- PermissionGranted

Events are immutable. Consumers are idempotent.

## API Design

Two API surfaces:

- Internal APIs for the web apps and internal services.
- Public APIs for partners and accountants (phase later).

Design requirements:
- Explicit versioning.
- Strong input validation.
- Fine-grained scopes.
- Webhook signatures and replay protection.
- Rate limiting and abuse detection.

## Data Stores and Consistency

### Primary relational store
Postgres holds:
- Canonical business state.
- Workflow state.
- Compliance obligations and filings.
- Permissions and audit indices.

### Object storage
- Document binaries.
- Evidence artifacts.
- RPA screenshots and traces.

### Cache
Redis for:
- Session caches.
- Short-lived workflow tokens.
- Rate limiting counters.

### Search
Search index for:
- Companies, directors, and documents.
- Full-text search on extracted content if enabled.

### Consistency model
- Strong consistency for core writes in Postgres.
- Eventual consistency for derived views, search, and analytics.
- Idempotent consumers for event processing.

## Security Architecture

### Data protection
- Encryption in transit everywhere.
- Encryption at rest for all data stores.
- Envelope encryption for documents and sensitive fields.
- Secrets stored in a managed secret vault.
- Document access via short-lived signed URLs.

### Audit logging
Immutable audit entries for:
- Authentication events.
- Permission changes.
- Document access and downloads.
- Workflow submissions and approvals.
- Data exports.

Audit logs include actor, tenant, workspace, action, target, timestamp, and context.

### Threat model highlights
- Credential compromise for government portals.
- Insider threat in operations workflows.
- Data leakage via document downloads.
- Automation misuse leading to fraudulent registrations.

Mitigations:
- MFA and step-up auth.
- Strict operator permissions and task scoping.
- Just-in-time credential access for RPA.
- Comprehensive monitoring and anomaly detection.

## Legal and Regulatory Considerations

This section defines architectural implications, not legal advice.

### POPIA
- Purpose limitation for personal data usage.
- Consent tracking for data processing and sharing.
- Data subject access and deletion workflows with legal retention constraints.

### FICA and mandates
- Signed mandates for acting on behalf of clients.
- Evidence storage for mandates.
- Role-based restrictions for who may submit filings.

### Liability boundaries
- Clear separation between platform guidance and professional advice.
- Record of client-provided inputs versus operator actions.
- Human approval for high-risk filings to reduce error impact.

## Observability and Operations

### Metrics
- Workflow success rates by type.
- Average time per registration and filing.
- Failure codes and portal error patterns.
- Operator throughput and backlog.
- Notification delivery rates.

### Logging and tracing
- Structured logs with correlation ids.
- Distributed tracing across services and workflows.
- RPA session logs and artifacts.

### Runbooks
- Portal outage handling.
- Credential rotation.
- Incident response for data exposure.
- Replay and recovery procedures for failed workflows.

## Deployment Architecture

Default deployment targets a cloud environment.

- Web frontends served via CDN.
- API gateway routing to services.
- Services run as containers (Kubernetes or managed container platform).
- Job workers scale independently.
- RPA runners executed in isolated job environments.
- Managed Postgres and object storage.
- Managed message bus or queue.

Environment tiers:
- Dev: mocked integrations.
- Staging: sandbox credentials and test tenants.
- Prod: hardened security, separate keys, strict access.

## Scalability Strategy

### Scale axes
- Number of workspaces (businesses).
- Number of documents and evidence artifacts.
- Number of workflow executions and RPA sessions.
- Number of partner sync events.

### Techniques
- Queue-based processing for heavy workflows.
- Horizontal scaling of stateless services.
- Partitioning by tenant for high-volume practices.
- Asynchronous document processing for indexing and extraction.

## Roadmap for Architecture Evolution

### Phase 0
- Web app, core services, document vault, manual workflow tracking, audit logs.

### Phase 1
- Workflow engine, compliance calendar, notifications, operations console, task queues.

### Phase 2
- Playwright RPA runner with human approvals, error taxonomy, evidence capture.

### Phase 3
- Banking and accounting integrations, account linking, transaction sync.

### Phase 4
- Public API, partner marketplace, deeper automation, analytics and benchmarking.

## Open Design Decisions

These decisions should be recorded and resolved in `architecture-decisions.md`.

- Workflow engine choice: Temporal vs internal.
- Event bus choice: Kafka vs NATS vs managed.
- Document content extraction strategy and redaction.
- Multi-region strategy and data residency requirements.
- Partner strategy for bank account linking and payments.

## Appendix: Minimum Architectural Guarantees

- Every workflow is a state machine with persisted state.
- Every submission has evidence and audit records.
- No high-risk action runs without validated inputs and approval policy.
- Tenancy isolation is enforced at the data layer.
- Documents are encrypted, access-controlled, and logged.
