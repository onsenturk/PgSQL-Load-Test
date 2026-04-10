---
description: "Reviews architecture, security, infrastructure, and coding compliance. Use to validate designs or check standards."
---

# Engineering Standards

This document defines non-negotiable engineering standards for this repository.
All code, documentation, infrastructure, and deployments must comply.

---

## 1. Platform & Deployment Strategy

### Azure Safety

- **Copilot and agents MUST NOT create, update, or delete Azure resources autonomously.**
- Infrastructure changes must be authored as IaC (Bicep/Terraform) locally — never applied without explicit user approval.
- Read-only Azure operations (queries, documentation lookups, cost estimation) are permitted.
- All Azure work must reference `azure.md` in the repository root for tenant, subscription, and resource group context.

### Backend

- Azure is the default and only cloud provider unless explicitly approved.
- Prefer managed services over self-hosted infrastructure.
- IaC standards (Bicep, Terraform, AVM) are defined in the **Infrastructure as Code** section below.
- No AWS/GCP services unless explicitly required and documented.

Default service choices:

- Web/API: Azure App Service or Azure Container Apps
- Containers: Azure Container Apps
- Database: PostgreSQL Flexible Server or CosmosDB
- Storage: Azure Storage (Blob)
- Secrets: Azure Key Vault

### Infrastructure as Code (IaC)

#### General Principles

- **Bicep is the primary IaC language.** Terraform is acceptable when Bicep is not feasible or when the team has existing Terraform investment.
- Prefer modules over large, flat configurations. Use one module per folder.
- Use an environment-based layout to separate configuration per stage (dev, prd, etc.).
- Never hardcode environment-specific values in modules — pass them via parameters or variables.

#### Azure Verified Modules (AVM)

- **AVM modules are the default choice.** When an AVM module exists for a resource type, use it instead of writing custom resource definitions.
- Query available AVM modules before writing new resource code. AVM modules are published on the [Public Bicep Registry](https://azure.github.io/Azure-Verified-Modules/) and [Terraform Registry](https://registry.terraform.io/).
- Reference Bicep AVM modules via the public registry: `br/public:avm/res/<provider>/<resource>:<version>` (resource modules) or `br/public:avm/ptn/<pattern>:<version>` (pattern modules).
- Reference Terraform AVM modules via the Terraform Registry: `source = "Azure/avm-res-<provider>-<resource>/azurerm"`.
- **Pin exact versions** (e.g., `0.4.0`). Do not use floating version ranges.
- When using AVM modules, configure the following interfaces where applicable:
  - **Diagnostics** — send logs/metrics to Log Analytics or Application Insights.
  - **RBAC** — assign least-privilege roles via the module's `role_assignments` parameter.
  - **Locks** — apply resource locks for production resources.
  - **Private Endpoints** — use when the resource supports it and the architecture requires private networking.
  - **Managed Identity** — prefer system-assigned or user-assigned managed identity over connection strings.
  - **Tags** — always pass a standard set of tags (environment, project, owner).
- **Fallback:** If no AVM module exists for a resource type, write custom Bicep/Terraform following AVM interface conventions (parameters for diagnostics, RBAC, tags, locks). Document the gap so it can be replaced when an AVM module becomes available.

#### Bicep Standards

- Use `main.bicep` as the entrypoint per environment/module.
- Use `.bicepparam` files for parameter values — not JSON parameter files.
- Prefer User-defined types over open types (`object`, `array`).
- Use `@secure()` for sensitive parameters.
- Use symbolic references (`resource.id`) instead of `resourceId()` / `reference()`.
- Use the `parent` property for child resources — not `/` in the name.

Recommended Bicep project layout:

```
infra/
├── env/
│   ├── dev/
│   │   ├── main.bicep
│   │   └── main.dev.bicepparam
│   └── prd/
│       ├── main.bicep
│       └── main.prd.bicepparam
└── modules/
    ├── <module1>/
    │   └── main.bicep
    └── <module2>/
        └── main.bicep
```

#### Terraform Standards

- Follow standard file conventions per environment: `main.tf`, `variables.tf`, `outputs.tf`, `providers.tf`, `backend.tf`, `locals.tf`, `datasource.tf` (when relevant).
- Use `snake_case` for all variable, output, and resource names.
- Use AzureRM provider best practices — avoid deprecated arguments, optimize for reusability and idempotency.
- Show module usage from `env/<env>/main.tf` and define reusable modules under `modules/`.

Recommended Terraform project layout:

```
infra/
├── env/
│   ├── dev/
│   │   ├── backend.tf
│   │   ├── datasource.tf
│   │   ├── locals.tf
│   │   ├── main.tf
│   │   ├── providers.tf
│   │   └── variables.tf
│   └── prd/
│       ├── backend.tf
│       ├── datasource.tf
│       ├── locals.tf
│       ├── main.tf
│       ├── providers.tf
│       └── variables.tf
└── modules/
    ├── <module1>/
    └── <module2>/
```

### Web Frontend

- React-based architecture preferred unless the project specifies otherwise.
- Tailwind CSS is the default styling system unless the project specifies otherwise.
- Avoid custom CSS unless strictly necessary.
- No additional CSS frameworks unless justified.

### Mobile

- Expo (React Native + Expo) is the default mobile platform unless the project specifies otherwise.
- Single codebase for iOS and Android.
- Native modules only when absolutely required.
- OTA updates preferred when possible.
- No secrets stored in mobile applications.
- Sensitive logic must reside in backend.

---

## 2. Architecture Principles

- Use layered architecture (API / Domain / Infrastructure).
- No business logic inside controllers.
- No cross-layer imports.
- External integrations must be isolated in infrastructure layer.
- Public interfaces must be explicit and stable.
- Avoid tight coupling and hidden global state.
- Prefer clarity over cleverness.
- Do not refactor unrelated code during feature implementation.

### Web & Mobile Architecture

- UI, business logic, and API integration must be separated.
- API communication must go through a dedicated service layer.
- No direct backend calls inside UI components.
- State management must be centralized and consistent.
- No ad-hoc global state.

### AI & Agent Architecture

- AI service calls must be isolated behind a service/adapter layer.
- Prompts and system instructions must be stored as versioned assets, not hardcoded in application logic.
- Model configuration (model name, temperature, tokens) must be externalized to configuration or environment variables.
- Agent workflows must have observability: trace IDs, token usage logging, and latency metrics.
- Use Azure AI Foundry / Microsoft Foundry for managed agent deployments when applicable.
- AI responses must be validated before passing to downstream systems (guard against hallucination, injection, and malformed output).

### API Versioning

- Public APIs must use URL path versioning (e.g., `/api/v1/resource`).
- Breaking changes require a new version — never modify an existing version's contract.
- Deprecated versions must return a `Sunset` header with a removal date.
- Internal APIs may use header-based versioning if justified.

### Error Handling

- All APIs must return a consistent error response format: `{ "error": { "code": "...", "message": "..." } }`.
- Do not expose stack traces or internal details in production error responses.
- Use typed exceptions in domain/service layers — catch and map at the API boundary.
- Transient failures to external services must use retry with exponential backoff (Polly, tenacity, or equivalent).
- Log errors with full context (correlation ID, operation, input summary) but never log sensitive data.

---

## 3. Documentation Requirements

Every project must contain:

- README.md
- requirements.md
- architecture.md (must include a Mermaid diagram illustrating the current architecture)
- deployment.md
- .env.example
- Infrastructure-as-Code (Bicep/Terraform)
- *(Optional)* API documentation (OpenAPI/Swagger) — required only when the project exposes public API endpoints

Documentation must reflect current behavior.

If any required documentation is missing, the project is incomplete.

---

## 4. Security Requirements

- No hardcoded secrets.
- Secrets stored in Azure Key Vault or environment variables.
- Managed Identity preferred over connection strings.
- Input validation required for all external inputs.
- Parameterized queries only (no string-built SQL).
- CORS must not be overly permissive.
- HTTPS enforced.
- Dependencies must be scanned for vulnerabilities.
- Logs must not expose sensitive data.
- No public storage access unless explicitly required.
- Secure token storage required for mobile.
- All API calls must use HTTPS.
- Rate limiting required on all public APIs.
- Must follow OWASP Top 10 security principles.

---

## 5. Dependency Management

- Minimize dependencies.
- Do not introduce new dependencies without justification.
- Prefer mature, well-maintained libraries.
- Remove unused dependencies immediately.
- Lock dependency versions.

---

## 6. Non-Functional Requirements

- Services must include structured logging.
- Errors must be logged with context (no silent failures).
- Health endpoint required for services.
- Application Insights enabled for production deployments.
- Correlation IDs required for request tracing.
- Resource usage must be reasonable for expected traffic.
- Avoid over-provisioned Azure resources.
- Prefer consumption/serverless tiers where possible.
- Document estimated monthly cost for new infrastructure.
- Public API changes must maintain backward compatibility or bump version.
- Database schema changes require migration script.
- No destructive schema changes without migration plan.
- Default to simplest viable solution.
- Do not introduce microservices without strong justification.

### Accessibility

- Web applications must meet WCAG 2.1 Level AA.
- All interactive elements must be keyboard-navigable.
- Images and icons must have appropriate alt text.
- Color must not be the sole means of conveying information.
- Mobile applications must support platform accessibility features (VoiceOver, TalkBack).

---

## 7. Change Discipline

If a change affects:

- Architecture
- Data model
- Public API
- Infrastructure

Then:

- Update architecture.md
- Update deployment.md
- Document rationale in requirements.md

---

## 8. Definition of Done

A feature is not complete unless it passes all checks in dod.md.

All features must satisfy Definition of Done requirements before merge.