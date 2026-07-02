---
type: Journal
title: "conv-space-catchall-484: default-Space DM catch-all + spaceless group/topic isolation"
description: Close the two deterministically reproducible cross-Space paths in the recent-conversation list
tags: [space, isolation, message, conversation, sidebar]
timestamp: 2026-07-02T00:00:00Z
---

# conv-space-catchall-484

Branch `fix/conv-space-catchall-484` (based on origin/main 236b78b; stacked-free —
presence infra re-introduced byte-identical to the unmerged #484 branch by user
decision, so the eventual merge dedupes).

## What was done

1. **Default-Space DM catch-all tightened** (production-reproducible leak): the
   catch-all in `decideConvKeepInSpace` kept every bare DM in the default Space.
   Now a post-filter pass (`space_filter_default_catchall.go`) hides a bare DM
   from the default Space iff `dm_space_presence` has rows for the pair and none
   of them is the default Space and the Recents window carries no untagged /
   default-tagged counter-evidence. No presence rows → legacy catch-all kept.
   System bots exempt; bot visibility stays with the catch-all bot sub-check;
   `skipBotFilter` or any query failure disables the pass (never hide on doubt).
   Self-heals: one default-Space message re-adds visibility.
2. **Spaceless groups/topics attributed to the default Space only** (the exact
   path a production client diagnostic showed rendering in every Space): the
   `spaceID == "" → return true` fail-open in the group branch, the
   `parentSpaceID == ""` fail-open in `filterThreadConvCore`, and the legacy-keep
   in sidebar `filterThreadExtsBySpace` all now show the conversation only when
   `filterSpaceID == defaultSpaceID` — same policy as #337 bare DMs and #484
   untagged DM history.
3. Both v1 (`/v1/conversation/sync`) and v2 (`/v1/sidebar/sync`) paths switched
   to `GetUserDefaultSpaceIDE`; on lookup error the conv filters fall open for
   the request (defaultSpaceID=filterSpaceID, catch-all pass disabled) so a DB
   hiccup never hides more than today; `filterThreadExtsBySpace` keeps its
   fail-closed error contract.

## Verification

- Unit: new `space_filter_default_catchall_test.go` (evidence gates, fail-open
  gates, counter-evidence, bot exemptions); two legacy assertions deliberately
  flipped (`...ThreadChannelLegacyParent`, `...Group_LegacyNoSpace_*`).
- Integration (real handlers + webhook-written presence):
  `TestConvSpaceCatchall_DefaultSpaceHidesElsewhereOnlyDM`,
  `TestConvSpaceCatchall_SpacelessGroupAndTopicOnlyDefault` — both PASS; suite
  helpers are cs-prefixed to avoid collisions with the #484 branch harness.
- `go build ./...`, `go vet`, `make i18n-lint`, `make i18n-extract-check` green.

## Learnings

- Topic conversations must ALSO pass the thread liveness whitelist
  (`QueryActiveShortIDs`, status=1) — integration tests seeding topics need a
  `thread` row, not just `group_member`.
- The shared `test` DB across test binaries still causes cross-binary migration
  clashes; DROP/CREATE between package runs remains mandatory.
- `personConvHasSpaceMessages(conv, "")` does NOT match untagged messages (they
  lack the key) — untagged counter-evidence needs its own scan.
