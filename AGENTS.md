
### Github Issue Workflow

When asked to resolve a Github issue:

1. **Inspect Issue (MCP):**
* Fetch issue details using the `Github` MCP server before starting work to confirm requirements and acceptance criteria.

2. **Branch & Implement (Local Shell):**
* Use the local terminal/git CLI to branch from the default branch:
```bash
git checkout -b <type>/issue-<id>-<slug>
```


*(Use `fix/` for bug fixes, `feat/` for features/enhancements).*
* Make edits locally, run required test suites and linters, and commit changes using standard git commits.


3. **Push to Remote (Local Shell via SSH/Git):**
* Do **not** attempt to push commits via MCP. Push directly using the local git tool:
```bash
git push -u origin <branch-name>
```

4. **Create Pull Request (MCP):**
* Open the Pull Request using the `Github` MCP server's pull request creation tool.
* Include `Resolves #<id>` in the PR description to trigger Github's automatic issue closing on merge.
* Capture the resulting PR number and URL.

5. **Update Issue Thread (MCP):**
* Post a comment on the original issue using `create_issue_comment` detailing:
* The branch name pushed.
* Summary of key architectural or code changes made.
* Link and reference to the created Pull Request.
