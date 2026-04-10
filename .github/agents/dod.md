---
description: "Validates completion of any task. Run this checklist before marking work as done."
---

# Definition of Done (DoD)

A feature, fix, or change is NOT complete unless ALL of the following are satisfied.

---

## 1. Engineering Standards Compliance

- Implementation complies with agents/engineering-standards.md.
- No architectural violations introduced.
- No cross-layer imports.
- No business logic inside controllers.
- No unnecessary refactoring of unrelated code.

---

## 2. Documentation

- README.md updated (if behavior changes).
- requirements.md updated (if scope changes).
- architecture.md updated (if structure changes), including a Mermaid diagram illustrating the current architecture.
- deployment.md updated (if infra changes).
- *(Optional)* API documentation (OpenAPI/Swagger) updated — required only when the task adds or changes public API endpoints.
- .env.example updated (if new environment variables introduced).

If documentation does not reflect reality, feature is incomplete.

---

## 3. Security Verification

- No hardcoded secrets.
- No secrets committed to repository.
- Inputs validated.
- Parameterized queries used.
- OWASP Top 10 risks reviewed.
- Public endpoints protected with authentication (if required).
- Rate limiting applied to public APIs.
- CORS configuration reviewed.
- Logs contain no sensitive data.

Security review must be explicit, not assumed.

---

## 4. Testing

- Unit tests added for new logic.
- Existing tests pass.
- Critical flows covered by integration tests.
- Edge cases considered.
- No flaky tests introduced.

If no tests exist, add minimal coverage for new logic.

### CI/CD Validation

- CI pipeline passes (build, lint, test).
- No new warnings introduced without justification.
- PR checks are green before merge.
- Branch protection rules respected.

---

## 5. Observability

- Errors logged with context.
- Structured logging used.
- Correlation IDs preserved.
- Health endpoint verified.
- Application Insights (or equivalent) enabled in production.

---

## 6. Infrastructure

- Infrastructure changes defined in Bicep or Terraform.
- No manual cloud configuration.
- **Copilot must NOT deploy infrastructure autonomously** — deployment requires explicit user approval.
- Deployment tested in staging before production (by the user or CI/CD pipeline).
- Estimated monthly cost documented for new resources.
- `azure.md` referenced for tenant/subscription/resource group context.

---

## 7. Performance & Stability

- No blocking synchronous calls in async flows.
- No obvious performance regressions.
- Resource usage reasonable.
- No unnecessary allocations or heavy loops.

---

## 8. Dependency Review

- No unnecessary dependencies introduced.
- Dependency versions locked.
- Vulnerability scan completed.
- No abandoned/unmaintained libraries added.

---

## 9. Change Discipline

If change affects:
- Architecture
- Data model
- Public API
- Infrastructure

Documentation updated accordingly.

---

## 10. Final Sanity Check

Ask:

- Would a senior engineer approve this?
- Is the solution the simplest viable implementation?
- Is anything hacky?
- Does this introduce future technical debt?

If yes — fix it before marking done.

---

## 11. Public Publishing Readiness

> This section applies **only** when publishing to a public web experience (e.g., Netlify, Azure Static Web Apps, Vercel). Skip if not applicable.

The following must be in place before release:

- Landing page includes basic SEO metadata (title, description, canonical URL, Open Graph/Twitter metadata where applicable).
- Landing page content clearly describes the product, owner, and intended use.
- Sitemap/robots handling reviewed for the intended indexing behavior.
- Public repository link included on the site when the repository is intended to be public.
- Privacy Policy published and linked from the landing page or footer.
- Privacy Policy clearly explains what data is collected, how it is used, how long it is retained, and any third-party processors/services involved.
- Privacy Policy includes appropriate disclaimer and limitation-of-liability language for the site/operator, with legal review recommended before production use.
- End User Agreement / Terms of Use published and linked from the site.
- End User Agreement explicitly requires user acceptance of the Privacy Policy and any data usage terms before use where legally or operationally required.
- Data usage disclosure is published in a user-facing form, covering analytics, cookies, telemetry, contact submissions, and any model/training-related usage if applicable.
- Platform-specific publishing settings reviewed (custom domain, HTTPS, redirects, headers, form handling, analytics, and environment variable exposure).
- All legal/documentation links are visible and functional in the deployed experience.

---

## 12. Accessibility

- Web UI meets WCAG 2.1 Level AA (if applicable).
- Interactive elements are keyboard-navigable.
- Screen reader compatibility verified for key flows.
- Color contrast ratios meet minimum thresholds.