# Taking changes from upstream

`OKI-Mesh/CoreScope` was forked from `Kpa-clawbot/CoreScope` on 2026-06-24 and **left the fork network on 2026-10-03** (#158, #161). There is no Sync fork button, no shared object storage, and no automatic path for upstream code to arrive.

That is deliberate. This file is how a wanted upstream change gets taken anyway, deliberately and one at a time.

## Why the old way is not an option

The fork link offered one button that merged everything upstream had done since the last sync. The last time it was used, #152, that was **583 files, 100+ commits, 20 authors** in a single PR.

It carried a defect nobody caught: upstream had moved the Carto tile key from config field `carto.token` to `carto.key`. Our config still said `token`, so the new code appended no API key and the map went blank — on dev immediately, and on prod at its next deploy. Found by the owner noticing dead tiles, not by review and not by CI (#156).

Upstream runs at roughly **90 commits a month**. A batch sync is unreviewable at that volume, and one unreviewed config-contract change is enough to break production.

## Watch releases, not commits

Upstream tags releases: `v3.12.0` 2026-09-26, `v3.11.0` 2026-09-16, `v3.10.1` 2026-09-04. Three in a month against ninety commits.

**Read their release notes, not their commit log.** The notes name the changes worth considering; the commit log is noise at this volume.

```bash
gh release list -R Kpa-clawbot/CoreScope --limit 5
gh release view v3.12.0 -R Kpa-clawbot/CoreScope
```

Nothing about reading upstream requires a fork relationship — a public repo is readable by anyone, confirmed post-detach (#162).

## Mechanism: a plain remote and a cherry-pick

Add upstream as an ordinary read-only remote. Never push to it, never merge a branch of theirs wholesale.

```bash
git remote add upstream https://github.com/Kpa-clawbot/CoreScope.git
git remote set-url --push upstream DISABLED
git fetch upstream
```

Take **one change at a time** by cherry-pick:

```bash
git worktree add /c/Dev/.worktrees/corescope-<issue#> -b feat/<issue#>-<slug> origin/master
cd /c/Dev/.worktrees/corescope-<issue#>
git cherry-pick -x <upstream-sha>
```

`-x` records the source commit in the message, so the provenance lives in the history rather than in someone's memory.

**Cherry-pick, not reimplementation.** Cherry-pick preserves the original author's commit authorship, which is correct: it is their work. A squashed reimplementation silently reassigns credit to us. If a change genuinely cannot be cherry-picked and has to be rewritten, say so in the commit body and name the upstream commit it derives from.

## Review checklist for an incoming upstream change

Every item is here because something went wrong without it.

1. **Does it change a config field our deployments supply?** This is the #156 class and the most dangerous one. Check both boxes' `config.json` against the fields the incoming code reads — including the frontend, where `map` and similar blocks are pass-through and no Go type references them. #157 tracks making this mechanical instead of remembered; until it lands, check by hand.
2. **Does it change a required status check's name?** `Protect Main` requires the context `✅ Go Build & Test` as an exact string, emoji included, matched against the `name:` of a job in `deploy.yml`. A rename on either side leaves PRs waiting forever on a context that never reports.
3. **Does it hardcode a registry, repo or org path?** The pre-handover registry survived in live CI for ten weeks in `release-fast-path.yml` because the value was written out four times and only two were updated (#164). Single-source anything like it.
4. **Does it reach `Kpa-clawbot` or `meshcore-analyzer` at runtime?** Issue links in comments are harmless. A User-Agent header, a release URL, a `gh run list -R`, or an image path is not.
5. **Does it assume a fork relationship?** Anything keyed on `github.event.pull_request.head.repo`, `.fork`, or a Sync-fork workflow is dead here.
6. **Does it touch the schema?** We run goose migrations (#131). An upstream schema change has to arrive as a numbered migration that applies cleanly to a baselined prod database, not as an in-place DDL edit.
7. **Does it bring back anything #144 removed?** See below.

Run the pre-PR adversarial review as for any change. Note the reviewer tool is installed locally by the governance preflight and is deliberately not in the committed tree, so it will not be present in a clean checkout — if `scripts/llm-consult.py` is missing, run the preflight rather than writing a replacement.

## What we do not take, ever

Epic #144 removed an imported agent surface of ~143 files that upstream still maintains. Those paths must not return through this route:

- `.squad/` — agent charters, casting, decision logs
- `.github/agents/`, `.github/instructions/` — Copilot and PR-reviewer agent definitions
- `docs/agents/` — imported runbooks and skills, including a release runbook that aimed release operations at the pre-handover repo (removed in #151)

Upstream last modified all of these on 2026-09-17 and continues to. A cherry-pick that touches any of them is wrong by default. If an upstream change is entangled with those paths, take the part you want and drop the rest — that is the whole point of per-change over per-batch.

## Cadence and ownership

**Unassigned.** Who reads upstream releases, and how often, is an owner and team decision; this document does not settle it. Until it is settled, upstream is read **on demand** — when someone wants a specific fix, or when a release note mentions something that matters to us.

The failure mode to avoid is a standing obligation nobody actually performs, which is how the imported agent surface sat untouched for four months while appearing to be maintained.

## Recording the decision

A change worth taking gets an issue first, like any other work, naming the upstream commit or release it comes from and why we want it.

A change considered and **rejected** is worth one line on that issue too — otherwise the same evaluation gets redone by the next person.
