# Leader account activation

Implemented October 29, 2026.

## Behavior

Settings → Leader accounts & invites now shows searchable accounts grouped by unit, Active/Inactive/All filters with counts, activity badges, and labeled actions. Owners can deactivate/reactivate other accounts; leaders can still edit access within their existing scope. Removing membership remains a separate action under More.

Deactivation preserves the sign-in identity, unit assignment, role, subscription, and notification preferences. Organization APIs, account sync, platform administration, and organization notification delivery reject or exclude inactive accounts. An already signed-in user loses organization access on their next request. Their own account information and billing management/cancellation remain available.

The inactive screen explains the restriction, clears displayed organization data, and offers Check access again, Manage billing, and Sign out. A reactivated leader can retry without creating another account. Inactive leaders do not appear as active leaders on unit cards.

The server rejects self-deactivation, non-boolean activation values, cross-organization changes, and any change that removes the last active owner. Lifecycle changes are serialized and the acting user's permissions are rechecked after waiting, including competing owner actions.

## Database and backup compatibility

Startup adds `users.is_active BOOLEAN NOT NULL DEFAULT true`. Existing accounts remain active. The migration was tested against an actual table without the column.

Current backup exports include activation state. Older version-3 backups retain the activation state of existing users and default newly inserted accounts to active. Restore cannot remove the last active owner. Account activation changes are audited.

Docker build contexts now exclude database backups and local agent metadata.

## Validation

New PostgreSQL tests cover lifecycle changes, existing-session denial, invitation/legacy bypass attempts, billing access, backup compatibility, notification exclusion, owner races, and the upgrade migration. Browser tests cover mobile/desktop layouts, account filters and actions, pending and failed writes, inactive-session handling, late responses, billing, and reactivation.

The complete standalone suite is run against a clean snapshot of the release source, with disposable PostgreSQL roles and Chromium. This also avoids a pre-existing icon-test scan of ignored local review archives.

## Deployment and rollback

Production uses `/opt/homelab/platoon` on `prodsrv02`. Take and verify a database dump and retain the current app image before upgrading. Build the app, then recreate only its Compose service. Verify startup logs, the new column, source checksums, public app/config responses, and authentication protection. Deployment does not deactivate any real account.

Code predating this feature ignores inactive flags. After any account has been deactivated, a rollback must retain the activation enforcement or use a corrected forward release. Reverting only the Docker image would restore access to inactive accounts. Do not automatically restore a pre-deployment database over subsequent live changes.
