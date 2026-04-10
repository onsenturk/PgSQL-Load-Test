---
description: "Template for all implementation work — new features, bug fixes, refactors, performance improvements, or breaking changes."
---

# Implementation Request

## 1. Summary

Describe the work clearly and concisely.

**Type** (select one):
- [ ] New feature
- [ ] Bug fix
- [ ] Behavior modification
- [ ] Performance improvement
- [ ] Refactor
- [ ] Breaking change

---

## 2. Requirements & Expected Behavior

### For new features:

List explicit, testable requirements.

- FR1:
- FR2:
- FR3:

### For changes to existing behavior:

**Current Behavior:**
Describe how the system currently behaves.

**Desired Behavior:**
Describe expected behavior after the change.

---

## 3. Acceptance Criteria

Define testable conditions that confirm the work is correct.

- **Given** [precondition], **when** [action], **then** [expected result].
- **Given** [precondition], **when** [action], **then** [expected result].

---

## 4. Non-Functional Requirements

- Performance expectations:
- Security requirements:
- Expected load:
- Availability requirements:
- Cost constraints (if applicable):

---

## 5. Constraints

- Must comply with agents/engineering-standards.md
- Must satisfy agents/dod.md
- Must not refactor unrelated code
- Must not expand scope beyond defined requirements
- Must not create, update, or delete Azure resources without explicit user approval
- Must reference `azure.md` for Azure environment context before any Azure-related work

---

# Copilot Mandatory Workflow

You MUST follow this structure in your response:

## A. Impacted Layers
(API / Domain / Infrastructure / Web / Mobile)

## B. Architecture Plan
- Describe how the work fits current architecture
- Justify design decisions
- Confirm layered architecture compliance

## C. Security Considerations
- Authentication/authorization impact
- Does this alter existing auth flows?
- Input validation requirements
- Are new inputs introduced?
- OWASP Top 10 review — does the risk profile change?
- Rate limiting impact
- Sensitive data handling

## D. Infrastructure Impact

> **Azure Safety Rule:** Do NOT create, modify, or delete Azure resources autonomously.
> Generate IaC files locally. Deployment requires explicit user approval.
> Reference `azure.md` for tenant/subscription/resource group context.

- New Azure resources required? (document in Bicep/Terraform — do not deploy)
- Changes to existing services? (document only — do not apply)
- Cost impact?
- Bicep/Terraform updates required?

## E. Impact Analysis

### Backward Compatibility
- Does this change API contracts?
- Does this affect existing users?
- Is a version bump required?

### Data Impact
- Schema changes?
- Migration required?
- Risk of data loss?

### Performance Impact
- Latency impact?
- Load implications?
- Resource usage changes?

## F. Documentation Updates Required
- README.md
- requirements.md
- architecture.md
- deployment.md
- OpenAPI/Swagger
- .env.example (if needed)

## G. Implementation

Provide implementation only after sections A–F are complete.

Do not modify unrelated files.

---

# Post-Implementation Verification

After implementation, explicitly:

1. Validate against agents/dod.md checklist.
2. Confirm no regression introduced.
3. Confirm documentation updates.
4. Confirm security review completion.
5. Confirm architectural compliance.
6. Confirm testing coverage added or updated.
7. Confirm infrastructure alignment.

If any section is incomplete, the task is NOT done.
