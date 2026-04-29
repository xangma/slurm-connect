# Release

Merges to `main` trigger the Marketplace release workflow. The workflow validates
the release candidate first, then waits for approval on the `vscode-marketplace`
GitHub Environment before publishing.

## GitHub Setup

Create a GitHub Environment named `vscode-marketplace` and add the reviewers who
may approve a production extension release.

Add a `VSCE_PAT` secret to either that environment or the repository. The token
must be an Azure DevOps Personal Access Token with Marketplace `Manage` scope
for the `xangma` publisher.

Ensure Actions has permission to push release commits and tags back to `main`.
The workflow uses `GITHUB_TOKEN` with `contents: write`.

## Versioning

On a merge to `main`, the workflow publishes a patch release by default.
Manual `workflow_dispatch` runs can choose `patch`, `minor`, or `major`.

The approved publish job runs `vsce publish`, which bumps `package.json` and
`package-lock.json`, creates a release commit, creates the matching `vX.Y.Z` tag,
publishes the extension to the Visual Studio Marketplace, and pushes the release
commit and tag back to GitHub.
