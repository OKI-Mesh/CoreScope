# Taking changes from upstream

`OKI-Mesh/CoreScope` was forked from `Kpa-clawbot/CoreScope` on 2026-06-24 and **left the fork network on 2026-10-03** (#158, #161). There is no Sync fork button, no shared object storage, and no automatic path for upstream code to arrive.

That is deliberate. This file is how a wanted upstream change gets taken anyway, deliberately and one at a time.

## Why the old way is not an option

The fork link offered one button that merged everything upstream had done since the last sync. The last time it was used, #152, that was **583 files, 100+ commits, 20 authors** in a single PR.

It carried a defect nobody caught: upstream had moved the Carto tile key from config field `carto.token` to `carto.key`. Our config still said `token`, so the new code appended no API key and the map went blank — on dev immediately, and on prod at its next deploy. Found by the owner noticing dead tiles, not by review and not by CI (#156).

Upstream runs at roughly **90 commits a month**. A batch sync is unreviewable at that volume, and one unreviewed config-contract change is enough to break production.

## Releases are the default lens — and what that costs

Upstream tags releases, and reading release notes rather than the commit log is what makes review tractable. Measured on 2026-10-07: **88 commits in the preceding thirty days against four releases** in the same window. Roughly twenty commits per release, so the release is the unit that a person can actually read.

Do not trust that ratio, or any tag named in this file, to still be current. It was four releases in thirty days when this was written and it will drift. Read it live:

```bash
gh release list -R Kpa-clawbot/CoreScope --limit 5
gh release view <tag> -R Kpa-clawbot/CoreScope
```

**State the tradeoff rather than pretending there isn't one.** Watching releases means a fix that lands on upstream's `master` and is not yet tagged is invisible to us until it is released, or until someone goes looking. For an ordinary improvement that latency is fine. For a security fix or a crash fix it is not.

So releases are the default, with one exception: **when we have reason to believe upstream has fixed something urgent, search their commits directly rather than waiting for a tag.**

```bash
# targeted, not a 90-commit read
gh search commits --repo Kpa-clawbot/CoreScope "fix" --limit 20
gh api repos/Kpa-clawbot/CoreScope/commits --paginate -q \
  '.[] | select(.commit.message | test("(?i)security|CVE|panic|data race|corrupt")) | "\(.sha[0:8]) \(.commit.message | split("\n")[0])"'
gh issue list -R Kpa-clawbot/CoreScope --label security --state all
```

Nothing about reading upstream requires a fork relationship — a public repo is readable by anyone, confirmed post-detach (#162).

## Mechanism: a plain remote and a cherry-pick

Add upstream as an ordinary read-only remote, once:

```bash
git remote add upstream https://github.com/Kpa-clawbot/CoreScope.git
git remote set-url --push upstream DISABLED
```

**Fetch before every pick.** A cherry-pick needs the commit object present locally; detaching did not change that, but it does mean nothing arrives until you fetch. A stale remote is the usual reason a pick fails with "bad object".

```bash
git fetch upstream                       # every time, not just the first
# $WORKTREES is wherever you keep worktrees. On the owner's Windows box that is
# C:/Dev/.worktrees; set it to suit your machine rather than copying a path.
git worktree add "$WORKTREES/corescope-<issue#>" -b feat/<issue#>-<slug> origin/master
cd "$WORKTREES/corescope-<issue#>"
git cherry-pick -x <upstream-sha>
```

`-x` records the source commit in the message, so provenance lives in the history rather than in someone's memory.

**Cherry-pick, not reimplementation.** Cherry-pick preserves the original author's authorship, which is correct: it is their work. A squashed reimplementation silently reassigns credit to us.

### When the pick conflicts

A conflict is information, not just an obstacle. Our tree has diverged a long way, so a conflict usually means the pick has landed in something we customised.

- **Resolve by hand.** Do not take `-X ours` or `-X theirs` wholesale; both discard one side without reading it.
- **Treat a conflict in heavily-customised code as a reason to reconsider.** If resolving it means rewriting most of the change, the honest outcome may be to reimplement deliberately — or to decide we don't want it.
- **Say so in the commit.** `-x` alone implies a clean pick. If you resolved conflicts, add a line naming the files and what was preserved, e.g. *"conflict resolved in `cmd/server/routes.go` to keep our RLock snapshot from #141"*. A reader six months out needs to know the pick was not clean.
- **Find out why our side looks like that before you overwrite it.** Resolving by hand still needs you to understand both sides, and the local side usually exists for a reason that is not visible in the diff. `git log -L` on the conflicting range, or the issue named in the blamed commit, will say who changed it and why. If that reason is still load-bearing and you cannot tell, ask whoever owns it rather than guessing — a hand-resolved conflict that quietly drops a fix is worse than a pick we declined.

```bash
git log -L <start>,<end>:<path>          # history of just the conflicting lines
git log -1 --format='%h %an %s' -S'<a line from our side>' -- <path>
```

## Review checklist for an incoming upstream change

Every item is here because something went wrong without it.

1. **Does it change a config field our deployments supply?** This is the #156 class and the most dangerous one, because nothing in CI can catch it: every CI artifact derives from the code, so code and fixtures move together and stay self-consistent. The only disagreeing artifact is the config deployed on the boxes, which CI has never seen. Compare the fields the incoming code reads — including frontend code, where blocks like `map` are pass-through and no Go type references them — against the live prod and dev config.

   Those config files live on the boxes, outside this repo; the private `OKI-Mesh/roadmap` infrastructure inventory says which box and which path. You do not need that access to do the comparison, because a running instance reports its own effective config:

   ```bash
   # what the server actually resolved, including defaults it filled in
   curl -s <instance>/api/config/client | jq 'keys'
   curl -s <instance>/api/config/client | jq '.<block-the-change-touches>'
   ```

   If you have neither box access nor a reachable instance, **you cannot complete this item** — say so on the issue and get someone who can. Do not merge an upstream change having skipped it. #157 tracks making this mechanical so it stops depending on who is holding it.
2. **Does it change a required status check's name?** Merge protection matches the check context as an exact string, emoji included, against a job `name:` in `deploy.yml`. A rename on either side leaves PRs waiting forever on a context that never reports. Do not trust the value written in any document, including this one — read the live rule:
   ```bash
   gh api repos/OKI-Mesh/CoreScope/rulesets -q '.[].id' \
     | xargs -I{} gh api repos/OKI-Mesh/CoreScope/rulesets/{} \
     -q '.rules[]|select(.type=="required_status_checks")|.parameters.required_status_checks[].context'
   ```
3. **Does it hardcode a registry, repo or org path?** The pre-handover registry survived in live CI for ten weeks because the value was written out four times and only two were updated (#164). Prove you checked:
   ```bash
   grep -rniE "kpa-clawbot|meshcore-analyzer" --exclude-dir=.git .
   ```
   Anything found at runtime — a User-Agent header, a release URL, a `gh run list -R`, an image path — is a defect. An issue link in a comment is not.

   **A clean grep is not proof.** It only finds the name written out literally. A path assembled at runtime — `fmt.Sprintf("%s/%s", org, repo)`, an org held in a const or an env default, a workflow `${{ env.UPSTREAM }}` — passes this grep and still points at upstream. That is exactly how #164 survived: the value was written four times and only two were changed. So also read how the change *builds* any repo, registry or URL string, not just whether it spells the old name.
4. **Does it change Go dependencies?** An upstream change can move `go.mod` / `go.sum`, pulling a new module, a breaking API, a new vulnerability, or a licence we would not choose. Diff both files, and run `govulncheck ./...` if the module set changed.

   What to do with the answer, so this is a decision and not just a reading: a **new direct dependency** is the owner's call, not the reviewer's — say what it is and what it replaces. A **vulnerability `govulncheck` reports as affecting a path we call** blocks the pick until upstream or the module fixes it. A copyleft licence (GPL, AGPL, SSPL) on anything we link blocks it outright. Anything else — a transitive bump, a vuln in a code path we do not reach — goes in the PR description and proceeds.
5. **Does it change frontend assets or vendored libraries?** The frontend is vanilla JS with no bundler, so a vendored `*.min.js`, a new CDN URL, or a changed `<script src>` lands straight in production with no build step to notice it. Treat a new external origin as a decision, not a detail.
6. **Does it touch the schema?** We run goose migrations (#131). A schema change has to arrive as a numbered migration that applies cleanly to a baselined prod database, not as an in-place DDL edit.
7. **Does it assume a fork relationship?** Anything keyed on `github.event.pull_request.head.repo`, `.fork`, or a Sync-fork workflow is dead here.
8. **Does it bring back anything #144 removed?** See below.

Run the pre-PR adversarial review as for any change. The reviewer tool lives at `scripts/llm-consult.py`. It is installed there by the governance preflight (`.claude/hooks/preflight.sh`, §5b) from the canonical copy in `standards`, and is deliberately kept out of the committed tree, so **it will not be present in a fresh clone or a new worktree.** If it is missing, run the preflight rather than writing a replacement:

```bash
bash .claude/hooks/preflight.sh
```

The hooks themselves are also local-untracked, so a new worktree has neither until the preflight has run in it.

## What we do not take, ever

**The principle: we do not re-import an agent instruction surface that we did not write.** Epic #144 removed roughly 143 such files. The list below is what existed when this was written; it is **not exhaustive**, and upstream may add more. A new directory of agent charters, personas, prompts or instructions is covered by the principle whether or not it appears here.

- `.squad/` — agent charters, casting, decision logs
- `.github/agents/`, `.github/instructions/` — Copilot and PR-reviewer agent definitions
- `docs/agents/` — imported runbooks and skills, including a release runbook that aimed release operations at the pre-handover repo (removed in #151)

Upstream last modified all of these on 2026-09-17 and continues to. A cherry-pick that touches any of them is wrong by default. If a change is entangled with those paths, take the part you want and drop the rest — that is the whole point of per-change over per-batch.

**The list above is examples, not the test.** Do not read it as a denylist and wave through a path that is missing from it. The test is the question: *is this a file that tells an agent how to behave, that we did not write?* `.prompts/`, `docs/ai/`, `.cursor/`, `CLAUDE.md`, a new `*.chatmode.md` — none of those are listed above and all of them are covered. If you are unsure whether a path is in scope, it is, and the decision to take it anyway is the owner's rather than yours. When you find a new one, add it here so the next reader has a shorter walk.

## Cadence and ownership

**Not settled by this document.** Who reads upstream releases, and how often, is an owner and team decision. Until it is decided, upstream is read **on demand** — when someone wants a specific fix, or when a release note happens to mention something that matters to us.

Be clear about what that costs: on-demand reading is reactive, so an upstream security or crash fix can sit unnoticed until it bites us. The argument for leaving it unassigned is that a named obligation nobody performs is worse than an honest gap — the imported agent surface sat untouched for four months while appearing maintained. The counter-argument, which is worth taking seriously, is that this codifies the neglect rather than fixing the accountability.

If and when a role and a minimum frequency are agreed, they belong here, replacing this section.

## When the change spans more than one commit

The per-change rule is not a one-commit rule. An upstream feature that genuinely needs six commits is still one change; a 100-commit sync is not.

The test is whether you can review it as a unit and say what it does. If you can, take the range in order and keep each commit:

```bash
git cherry-pick -x <oldest-sha>^..<newest-sha>
```

If you cannot — if the only way to describe it is "everything they did to the map code since August" — then it is a batch, and batches are what this document exists to prevent. Split it into the parts you can evaluate, or decline it and open an issue for implementing the behaviour ourselves. **Do not let a large wanted feature become the reason to run a sync.** That is how #152 happened.

## When a taken change turns out to be wrong

This whole document is preventative, and prevention fails. #156 reached prod.

A cherry-picked commit reverts like any other, and `-x` is what makes it findable:

```bash
git log --grep="cherry picked from" --oneline        # everything we have taken
git revert <the-sha-on-our-master>                   # not the upstream sha
```

Two things that are not the revert:

- **If it reached prod, redeploying the previous tag is faster than a revert.** The deploy path keeps the prior image and auto-rolls-back on a failed healthcheck. Get prod right first, then fix master.
- **Record it on the issue that took the change, and say what the review missed.** #156 is only useful because the config-field class got written down afterwards and became checklist item 1. A revert with no note just leaves the next person free to take the same commit again.

## Recording the decision

A change worth taking gets an issue first, like any other work, naming the upstream commit or release it comes from and why we want it.

A change considered and **rejected** is worth one line on that issue too — otherwise the same evaluation gets redone by the next person.
