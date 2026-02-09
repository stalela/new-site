# UI Spec

## Purpose

Defines the user interface structure for Stalela's three portals. Each portal serves a distinct user segment with role-scoped views. All screens must enforce the RBAC model from `security.md`, emit audit events for sensitive actions, and respect the multi-tenant workspace boundary.

This spec references domain entities from `domain-model.md` and workflow states from `workflows/`.

---

## Portals

### SME Portal
Primary users: Business owners, directors.
Roles: Owner, Admin, Viewer.
Purpose: Business setup, compliance visibility, document management, banking.

### Accountant Portal
Primary users: Accounting practices managing multiple clients.
Roles: Accountant, Admin.
Purpose: Multi-workspace management, bulk compliance tracking, client intake.

### Operations Console
Primary users: Internal Stalela staff.
Roles: Operator.
Purpose: Task queues, RPA monitoring, manual submissions, exception handling.

---

## Shared Patterns

### Workspace Switcher
All portals display the active workspace context. Accountant and Operator portals support switching between workspaces. The active `workspace_id` scopes all data queries and actions.

### Auth Gates
- Login: email/password + MFA (mandatory for Operators).
- Step-up auth: prompted before high-risk actions (filings, ID document downloads, bank linking, director changes). See `security.md` → Authorization Model.
- Session timeout with re-auth.

### Notification Center
In-app notifications for:
- Obligation due dates (30d, 7d, due, overdue).
- Workflow state changes.
- Task assignments.
- Filing outcomes.

### Audit Trail Viewer
Read-only view of `AuditEvent` records for the current workspace. Filterable by action, actor, target, and date range. Available to Owner and Admin roles.

### Empty States
Every list and dashboard widget must have a meaningful empty state that guides the user to the next action (e.g., "No company registered yet — Start registration").

---

## SME Portal

### Onboarding Flow

Multi-step wizard mapping to the `company_registration` workflow states. Progress is persisted server-side as a `WorkflowInstance`. Users can leave and resume.

**Steps:**

1. **Create workspace**
   - Collect: business display name, workspace type (Pty Ltd, NPC, Sole Prop, Trust).
   - Creates: Tenant (if new), Workspace, draft CompanyProfile v1.

2. **Enter company info**
   - Fields: proposed company names (1–4), registered address, business address, financial year end, share structure.
   - Validation: address format, share totals = 100%, name length per CIPC rules.

3. **Add directors and shareholders**
   - Repeatable section per person.
   - Fields: full name, ID type (SA ID / Passport), ID number, residential address, email, phone.
   - Creates: Person, PersonRole records.
   - ID numbers masked after entry (display last 4 only).

4. **Upload documents**
   - Required: ID documents (front/back) per director, proof of address, incorporation resolution.
   - Upload widget: drag-and-drop, type selection from `document_type` enum, file size/type validation.
   - Creates: Document records in vault with `sha256_hash`.

5. **Sign mandate**
   - Display mandate text for `CIPC_AGENT` type.
   - Capture: digital signature or signed upload.
   - Creates: Mandate record with `evidence_document_id`.
   - Workflow cannot proceed without active mandate.

6. **Review and submit**
   - Summary screen: all entered data, documents, mandate status.
   - Confirmation checkbox.
   - Submit creates SubmissionRecord (status = `PREPARED`), transitions workflow to `READY_FOR_SUBMISSION`.
   - Human operator approval required before portal submission.

**State display:** Progress bar reflecting workflow states: `NOT_STARTED` → `INTAKE_IN_PROGRESS` → `AWAITING_KYC` → `AWAITING_MANDATE` → `DATA_VALIDATION` → `READY_FOR_SUBMISSION` → `SUBMITTED` → `REGISTERED` → `COMPLETED`.

**Error recovery:** If validation fails or KYC is rejected, show specific error with link back to the relevant step. Create Task for operator if manual intervention needed.

---

### Dashboard

Primary landing screen after onboarding.

**Widgets:**

1. **Company Status Card**
   - CompanyProfile summary: legal name, registration number, company type, status.
   - Registration workflow status if still in progress.

2. **Compliance Overview**
   - Count of obligations by status: due soon, overdue, in progress.
   - Next 3 upcoming obligations with due dates.
   - Link to full compliance timeline.

3. **Recent Activity**
   - Last 10 AuditEvent entries for the workspace.
   - Filterable by category (filings, documents, banking).

4. **Banking Status**
   - BankOnboardingCase status if active.
   - Linked BankAccount summary (bank name, masked account, status).

5. **Quick Actions**
   - Upload document.
   - Start bank onboarding.
   - View tasks.

---

### Compliance Timeline

Chronological view of obligations from `ComplianceCalendarEntry`.

**Layout:** Calendar or list view toggle.

**Per obligation row:**
- Obligation name (from ObligationDefinition).
- Authority: CIPC, SARS, UIF.
- Period: start–end.
- Due date.
- Status badge: `PLANNED` | `DUE_SOON` | `DUE` | `IN_PROGRESS` | `SUBMITTED` | `COMPLETED` | `FAILED` | `WAIVED`.
- Evidence indicator (documents attached or missing).

**Actions:**
- View filing details.
- Upload evidence (creates/links Document to Filing).
- Download confirmation (signed URL, step-up auth required, access logged).

**Filters:** Status, authority, date range, overdue only.

---

### Document Vault

Workspace-scoped document list from the Document entity.

**List columns:** File name, document type, owner (workspace or person name), sensitivity level, uploaded date, status.

**Upload flow:**
1. Select document type from enum (`CIPC_CERTIFICATE`, `MOI`, `RESOLUTION`, `TAX_NOTICE`, `ID_DOC`, `PROOF_OF_ADDRESS`, `BANK_LETTER`, etc.).
2. Select owner (workspace or person from workspace).
3. Drag-and-drop or file picker. Validate mime type and size.
4. System computes `sha256_hash`, stores in object storage, creates Document record.

**Download:** Generates time-limited signed URL. Step-up auth for `RESTRICTED` or `HIGH` sensitivity documents. Access logged as AuditEvent.

**Metadata view:** Classification, validity dates, retention policy, linked workflows/filings.

---

### Bank Onboarding

Maps to `banking.md` workflow.

**Flow screens:**

1. **Choose bank**
   - Select from supported banks (FNB, Standard Bank, Nedbank, Absa).
   - Display required documents checklist per bank.

2. **Document readiness**
   - Show required documents with status (uploaded / missing).
   - Link to upload for missing items.
   - System auto-bundles from existing vault documents.

3. **Consent**
   - Display data sharing scope.
   - Capture explicit consent. Creates BankConsent record.
   - POPIA-compliant disclosure: what data, why, how long.

4. **Referral**
   - For integrated banks: redirect to partner flow.
   - For manual: confirmation that bundle sent, tracking reference displayed.
   - Creates BankOnboardingCase (status = `REFERRED_TO_BANK`).

5. **Status tracking**
   - Display BankOnboardingCase status: `PREPARING_PACKAGE` → `REFERRED_TO_BANK` → `PENDING_VERIFICATION` → `APPROVED` / `REJECTED`.
   - Notifications on status changes.

6. **Account linking** (post-approval)
   - Aggregator flow (e.g., Stitch): redirect → consent → token return.
   - Manual: enter masked account number for linking.
   - Creates BankAccount record.

---

### Settings

- **Profile:** User email, phone, MFA configuration.
- **Workspace:** Display name, timezone, industry code.
- **Team:** Manage WorkspaceMemberships. Invite users, assign roles (Owner/Admin/Viewer). Revoke access.
- **Integrations:** View IntegrationConnections. Connect/disconnect. Manage scopes.
- **Data & Privacy:** Download personal data. Request deletion (where legally permitted). View/revoke consents.

---

## Accountant Portal

### Client List

Table of workspaces the accountant has access to via WorkspaceMembership.

**Columns:** Business name, company type, registration status, compliance status summary (count of overdue), last activity date.

**Actions:** Open workspace (switches context), create new client workspace, archive workspace.

**Search:** Full-text search across client business names and registration numbers.

---

### Client Intake

Streamlined onboarding for accountants adding new clients. Same wizard as SME onboarding but:
- Mandate type defaults to `SARS_PRACTITIONER` + `CIPC_AGENT`.
- Accountant marked as Authorized Representative.
- Bulk document upload for multiple directors.
- Pre-populated fields from accountant's existing client data if available.

---

### Compliance Dashboard (Multi-Client)

Aggregated compliance view across all workspaces.

**Views:**
- **Due this week/month:** Obligations due soon across all clients, sorted by due date.
- **Overdue:** All overdue obligations, ordered by severity.
- **By client:** Expandable rows per workspace showing obligation summary.
- **By type:** Group obligations by authority (CIPC, SARS, UIF) across clients.

**Bulk actions:**
- Assign to team member.
- Export compliance report.

---

### Filing Queue

List of Filings across managed workspaces.

**Columns:** Client name, filing type, obligation period, status, assigned to, due date.

**Actions:** Open filing detail, upload evidence, mark submitted.

**Filters:** Status, filing type, client, date range, assigned user.

---

## Operations Console

### Task Queue

Primary operator work surface. Lists Tasks sorted by priority and due date.

**Columns:** Task type, workspace name, priority badge (`LOW` / `NORMAL` / `HIGH` / `URGENT`), status, assigned operator, due date, workflow link.

**Actions:**
- Claim task (assigns to current operator).
- Open associated WorkflowInstance.
- Mark status: `IN_PROGRESS`, `BLOCKED`, `DONE`.
- Add notes and resolution code.
- Escalate (reassign + increase priority).

**Filters:** Task type, status, priority, assigned, workspace.

---

### Workflow Monitor

View all WorkflowInstances across tenants.

**Columns:** Workflow type, workspace, current state, started date, last activity, assigned operator.

**Detail view:**
- State machine visualization showing current state highlighted.
- WorkflowEvent timeline (state transitions, approvals, errors).
- Linked documents and SubmissionRecords.
- Input payload (read-only, sensitive fields masked).

**Actions:**
- Approve (for `WAITING_APPROVAL` state). Requires step-up auth.
- Reject with reason.
- Cancel with reason.
- Retry failed step (idempotent, creates new SubmissionRecord).

---

### Submission Console

For manual and RPA-assisted government portal submissions.

**Pre-submission view:**
- SubmissionRecord details: target system (CIPC / SARS / BizPortal), payload summary, documents bundle.
- Checklist: all required fields present, documents valid, mandate active.
- Human approval confirmation (checkbox + step-up auth).

**During submission (RPA mode):**
- Live status: step-by-step progress from RPA runner.
- Screenshot viewer: latest screenshot from Playwright session.
- Pause / abort controls.

**Post-submission:**
- Capture external reference number.
- Upload confirmation document / screenshot.
- Update SubmissionRecord status.
- Outcome selection: Approved / Rejected / Pending.

---

### Compliance Operations

Operator view of the compliance engine output.

**Views:**
- Obligations requiring action (overdue, failed, blocked).
- Waiver management: review waiver requests, approve with reason code (audit logged).
- Filing recording: manually record a filing for an obligation with evidence upload.

---

### RPA Session Viewer

View history and live status of RPA automation runs.

**Per session:**
- Target system.
- Workflow instance link.
- Step log with timestamps.
- Screenshot gallery (redacted per `security.md` — no full IDs visible).
- Outcome: success / failure / escalated.
- Execution trace (immutable audit artifact).

---

## Cross-Cutting UI Requirements

### Data Display Rules
- ID numbers: display last 4 digits only, masked prefix (e.g., `●●●●●●●1234`).
- Bank account numbers: masked, last 4 digits only.
- Tax numbers: masked unless step-up auth confirmed.
- Follows data classification from `security.md`: Restricted fields always masked by default.

### Form Validation
- Client-side validation for format and required fields.
- Server-side validation is authoritative — UI must handle and display server validation errors.
- Share allocation must total 100%.
- Address fields follow South African format (province from fixed list, postal code format).

### Loading and Error States
- Every async operation shows a loading indicator.
- Network errors display retry option.
- Server validation errors shown inline per field.
- Unexpected errors show generic message + correlation ID for support.

### Accessibility
- Semantic HTML.
- Keyboard navigation for all actions.
- ARIA labels for status badges and icons.
- Colour is never the sole indicator of status.

### Responsive Design
- Primary target: desktop (operators, accountants).
- SME portal must be functional on tablet and mobile for document uploads and status checks.

### Confirmation Dialogs
Required before:
- Submitting for registration.
- Approving a workflow.
- Deleting/revoking access.
- Waiving an obligation.
- Revoking consent.

---

## Minimum UI Guarantees

- No filing or submission without visible mandate status.
- No document download without access logging.
- No approval without step-up authentication.
- Sensitive fields masked by default in all views.
- Every state change reflected in real-time or on next refresh.
- Empty states guide users to the next action.
