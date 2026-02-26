## Raise this change as a GitHub PR

You cannot open a PR directly from this environment unless your GitHub credentials are configured with push access.
Use this standard fork-and-PR flow against `https://github.com/Netflix/maestro`:

1. Fork `Netflix/maestro` on GitHub to your account.
2. Add your fork as `origin` and Netflix repo as `upstream`:
   ```bash
   git remote rename origin upstream
   git remote add origin git@github.com:<your-user>/maestro.git
   ```
3. Push your current branch:
   ```bash
   git push -u origin $(git branch --show-current)
   ```
4. Open a PR in GitHub UI:
   - Base repo: `Netflix/maestro`
   - Base branch: `main`
   - Compare repo: `<your-user>/maestro`
   - Compare branch: your pushed branch
5. Reuse this PR title/body:
   - **Title:** `Add architecture overview and diagram for the Maestro repository`
   - **Body:** summarize motivation, docs added, and testing notes.

### Optional: create PR via GitHub CLI

If you have `gh` authenticated:

```bash
gh pr create \
  --repo Netflix/maestro \
  --base main \
  --head <your-user>:$(git branch --show-current) \
  --title "Add architecture overview and diagram for the Maestro repository" \
  --body "Add architecture overview docs and Mermaid diagram; include validation notes."
```
