"""Seal / verify / log the frozen TEST splits (dev/evaluation_protocol.md).

The test splits (`data/ground_truth/<bench>.qa.test.json`) are the only Q&A sets
that produce the paper's headline *generalization* number. To stay auditable they
must be (1) authored from raw source only, (2) SEALED before the next extractor
fix, (3) never edited afterwards, and (4) evaluated exactly once per frozen tool
tag. This script enforces (2)–(4).

Manifest: data/ground_truth/test_split_manifest.json

Usage
-----
  # After authoring the test splits, before writing any new extractor code:
  python experiments/seal_test_split.py --seal

  # Integrity gate run automatically by run_qa_experiment.py before a test run;
  # also runnable by hand. Exits non-zero if any sealed file changed:
  python experiments/seal_test_split.py --verify
  python experiments/seal_test_split.py --verify --file <path>   # one file

  # Record that the test split was evaluated against a given tagged tool version:
  python experiments/seal_test_split.py --log-eval --tool-tag v1.1-frozen-for-eval

  # Re-seal an already-sealed file (requires an explicit reason - this is the
  # audit trail for why a frozen set was changed):
  python experiments/seal_test_split.py --seal --force --reason "..."

Exit codes: 0 = ok; 1 = integrity failure / refused operation.
"""
from __future__ import annotations

import argparse
import datetime as _dt
import hashlib
import json
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
GT_DIR = REPO / "data" / "ground_truth"
MANIFEST = GT_DIR / "test_split_manifest.json"
TEST_GLOB = "*.qa.test.json"


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    h.update(path.read_bytes())
    return h.hexdigest()


def _git(*args: str) -> str:
    try:
        out = subprocess.run(["git", *args], cwd=REPO, capture_output=True, text=True)
        return out.stdout.strip()
    except Exception:
        return ""


def _now() -> str:
    return _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _load_manifest() -> dict:
    if MANIFEST.exists():
        return json.loads(MANIFEST.read_text(encoding="utf-8"))
    return {"_doc": "See dev/evaluation_protocol.md. Seal records are immutable once written; "
                     "re-sealing requires --force --reason.", "splits": {}, "evaluations": []}


def _save_manifest(m: dict) -> None:
    MANIFEST.write_text(json.dumps(m, indent=2), encoding="utf-8")


def _is_authored(path: Path) -> bool:
    """A scaffold with zero questions is not yet a real test split."""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return len(data.get("questions", [])) > 0
    except Exception:
        return False


def cmd_seal(force: bool, reason: str | None) -> int:
    m = _load_manifest()
    files = sorted(GT_DIR.glob(TEST_GLOB))
    if not files:
        print(f"No {TEST_GLOB} files found under {GT_DIR}. Nothing to seal.")
        return 0
    commit = _git("rev-parse", "HEAD")
    short = _git("rev-parse", "--short", "HEAD")
    sealed_any = False
    for f in files:
        name = f.name
        already = m["splits"].get(name, {}).get("sealed")
        if already and not force:
            print(f"  SKIP  {name} - already sealed at {m['splits'][name]['commit'][:7]} "
                  f"({m['splits'][name]['sealed_at']}). Use --force --reason to re-seal.")
            continue
        if not _is_authored(f):
            print(f"  SKIP  {name} - scaffold has 0 questions; author it before sealing.")
            continue
        if already and force and not reason:
            print(f"  REFUSED {name} - --force requires --reason (audit trail).")
            return 1
        rec = {
            "sha256": _sha256(f),
            "commit": commit,
            "commit_short": short,
            "sealed_at": _now(),
            "sealed": True,
            "num_questions": json.loads(f.read_text(encoding="utf-8")).get(
                "metadata", {}).get("num_questions"),
        }
        if already and force:
            rec["resealed_from"] = {k: m["splits"][name].get(k)
                                    for k in ("sha256", "commit_short", "sealed_at")}
            rec["reseal_reason"] = reason
        m["splits"][name] = rec
        sealed_any = True
        verb = "RE-SEALED" if already else "SEALED"
        print(f"  {verb}  {name}  sha={rec['sha256'][:12]}  @ {short}")
    if sealed_any:
        _save_manifest(m)
        print(f"\nManifest updated: {MANIFEST.relative_to(REPO)}")
        print("These files are now frozen. Do not edit them; develop fixes on the tuning split.")
    return 0


def cmd_verify(one_file: str | None) -> int:
    m = _load_manifest()
    if one_file:
        targets = [Path(one_file) if Path(one_file).is_absolute() else REPO / one_file]
    else:
        targets = sorted(GT_DIR.glob(TEST_GLOB))
    if not targets:
        print("No test splits to verify.")
        return 0
    bad = 0
    checked = 0
    for f in targets:
        name = f.name
        rec = m["splits"].get(name)
        if not rec or not rec.get("sealed"):
            # Unsealed authored test file is a hard failure only when it has questions:
            # an empty scaffold is fine, an authored-but-unsealed split is not evaluable.
            if _is_authored(f):
                print(f"  FAIL  {name} - authored but NOT sealed. Run --seal first.")
                bad += 1
            else:
                print(f"  ok    {name} - empty scaffold (not yet authored/sealed).")
            continue
        if not f.exists():
            print(f"  FAIL  {name} - sealed in manifest but file missing.")
            bad += 1
            continue
        cur = _sha256(f)
        checked += 1
        if cur != rec["sha256"]:
            print(f"  FAIL  {name} - CONTENT CHANGED since sealing.")
            print(f"          sealed sha={rec['sha256'][:16]}  now={cur[:16]}")
            print(f"          sealed at {rec['sealed_at']} @ {rec.get('commit_short')}")
            bad += 1
        else:
            print(f"  ok    {name} - sha matches ({cur[:12]}), sealed @ {rec.get('commit_short')}.")
    if bad:
        print(f"\nINTEGRITY FAILURE: {bad} file(s) failed. See dev/evaluation_protocol.md (R2/R5).")
        return 1
    print(f"\nAll {checked} sealed test split(s) verified.")
    return 0


def cmd_log_eval(tool_tag: str | None) -> int:
    if not tool_tag:
        print("--log-eval requires --tool-tag <vX.Y-frozen-for-eval>")
        return 1
    # The tag must exist and be an annotated/lightweight git tag (R4: no floating tips).
    tag_commit = _git("rev-list", "-n", "1", tool_tag)
    if not tag_commit:
        print(f"REFUSED - tool tag '{tool_tag}' does not exist. Cut it first:\n"
              f"  git tag -a {tool_tag} -m '<what changed>' <commit>")
        return 1
    m = _load_manifest()
    sealed = {n: r for n, r in m["splits"].items() if r.get("sealed")}
    if not sealed:
        print("REFUSED - no sealed test splits to evaluate. Seal first.")
        return 1
    prior = [e for e in m["evaluations"] if e["tool_tag"] == tool_tag]
    if prior:
        print(f"WARNING - test split already evaluated against {tool_tag} on "
              f"{prior[-1]['logged_at']}. R4 says evaluate ONCE per tag. If the tool "
              f"changed, cut a NEW tag instead of re-running this one.")
    m["evaluations"].append({
        "tool_tag": tool_tag,
        "tool_commit": tag_commit,
        "logged_at": _now(),
        "splits": {n: r["sha256"][:16] for n, r in sealed.items()},
    })
    _save_manifest(m)
    print(f"Logged evaluation of {len(sealed)} sealed split(s) against {tool_tag} "
          f"({tag_commit[:7]}).")
    return 0


def cmd_status() -> int:
    m = _load_manifest()
    files = sorted(GT_DIR.glob(TEST_GLOB))
    print("Test-split status (dev/evaluation_protocol.md):\n")
    for f in files:
        rec = m["splits"].get(f.name, {})
        if rec.get("sealed"):
            state = f"SEALED @ {rec.get('commit_short')} ({rec.get('num_questions')} Qs, {rec['sealed_at']})"
        elif _is_authored(f):
            state = "AUTHORED, NOT SEALED - run --seal"
        else:
            state = "empty scaffold - author from raw source (R1)"
        print(f"  {f.name:42s} {state}")
    if m["evaluations"]:
        print("\nEvaluations logged:")
        for e in m["evaluations"]:
            print(f"  {e['tool_tag']:24s} {e['logged_at']}")
    else:
        print("\nNo test-split evaluations logged yet.")
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    g = p.add_mutually_exclusive_group()
    g.add_argument("--seal", action="store_true", help="Seal authored test splits.")
    g.add_argument("--verify", action="store_true", help="Verify sealed splits are unchanged.")
    g.add_argument("--log-eval", action="store_true", help="Record a test evaluation run.")
    g.add_argument("--status", action="store_true", help="Show seal/eval status.")
    p.add_argument("--force", action="store_true", help="Re-seal an already-sealed file.")
    p.add_argument("--reason", default=None, help="Required justification for --force re-seal.")
    p.add_argument("--file", default=None, help="Limit --verify to one file.")
    p.add_argument("--tool-tag", default=None, help="Git tag of the tool version for --log-eval.")
    args = p.parse_args()

    if args.seal:
        return cmd_seal(args.force, args.reason)
    if args.verify:
        return cmd_verify(args.file)
    if args.log_eval:
        return cmd_log_eval(args.tool_tag)
    # default / --status
    return cmd_status()


if __name__ == "__main__":
    sys.exit(main())
