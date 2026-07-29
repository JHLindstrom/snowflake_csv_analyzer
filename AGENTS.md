# Development workflow

When the user requests or proposes a change to the project, treat the request as authorization to complete the full delivery workflow.

For every requested project change:

1. Inspect the relevant code and determine the intended scope.
2. Implement the change on a dedicated branch.
3. Run all relevant tests, validation, linting, and build steps.
4. Create a pull request targeting `main`.
5. Review the complete pull-request diff.
6. Fix all actionable review findings and rerun affected checks.
7. Wait for required CI checks to pass.
8. Merge the pull request.
9. Monitor the deployment until validation, restart, and health checks succeed.
10. Report the pull request, merge commit, checks, deployment result, and any remaining concerns.

Do not ask for routine confirmation between these steps.

## High-risk changes

Treat a change as high risk when it may cause data loss, break irrigation, affect valve or pump safety, change deployment infrastructure, modify secrets or authentication, alter persisted Home Assistant entities, require migration, or otherwise have a meaningful chance of disrupting the running system.

For high-risk changes:

1. Complete implementation, tests, pull-request creation, review, and CI validation.
2. Do not merge or deploy yet.
3. Explain the identified risks and provide concrete verification steps.
4. Ask the user for explicit approval before merging or deploying.
5. After approval, merge and monitor the deployment through its health check.

If risk is uncertain, classify the change as high risk.

## Scope boundaries

- Questions, explanations, investigations, code reviews, and requests for advice do not authorize code changes or deployment.
- Only requests or proposals that clearly ask for a project change trigger the delivery workflow.
- Never include unrelated working-tree changes in a commit or pull request.
- Never bypass failing tests, required reviews, branch protection, deployment checks, or authentication controls.
- If deployment fails, inspect the failure and use the established rollback behavior. Report the result clearly.
