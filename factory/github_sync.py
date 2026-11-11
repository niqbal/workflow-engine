"""Create GitHub labels, milestones and issues from factory/items.py (idempotent by title).
Writes factory/issues.json mapping work-item ID -> issue number."""
import json, subprocess, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
import items

REPO = "niqbal/workflow-engine"
ROOT = Path(__file__).resolve().parents[1]
MAP = ROOT / "factory" / "issues.json"

def gh(*args, input=None):
    r = subprocess.run(["gh", *args], capture_output=True, text=True, input=input)
    if r.returncode:
        raise RuntimeError(r.stderr)
    return r.stdout

def api(method, path, **fields):
    args = ["api", "-X", method, f"repos/{REPO}/{path}"]
    data = json.dumps(fields)
    return json.loads(gh(*args, "--input", "-", input=data) or "null")

LABELS = {"owner:human": "5319e7", "owner:factory": "0e8a16", "tracking": "fbca04",
          **{f"tier:{t}": c for t, c in ((1, "c2e0c6"), (2, "bfd4f2"), (3, "d93f0b"))}}
NAMES = {"M0": "Skeleton and contracts", "M1": "Single node, in memory", "M2": "Durable",
         "M3": "Simulation", "M4": "Sharding", "M5": "Versioning and metrics", "M6": "Showcase"}

def main():
    existing_labels = {l["name"] for l in json.loads(gh("label", "list", "-R", REPO, "--json", "name", "-L", "200"))}
    for name, color in LABELS.items():
        if name not in existing_labels:
            gh("label", "create", name, "-R", REPO, "--color", color)
    ms = {m["title"]: m["number"] for m in json.loads(gh("api", f"repos/{REPO}/milestones?state=all&per_page=100"))}
    for m, n in NAMES.items():
        title = f"{m} {n}"
        if title not in ms:
            ms[title] = api("POST", "milestones", title=title, description=f"SPEC §16 milestone {m}")["number"]
    issues = {i["title"]: i["number"] for i in json.loads(gh("issue", "list", "-R", REPO, "--state", "all", "--json", "title,number", "-L", "500"))}
    mapping = json.loads(MAP.read_text()) if MAP.exists() else {}
    for (i, m, owner, tier, title, deps, body, acc) in items.ITEMS:
        t = f"{i} {title}"
        dep_txt = ", ".join(f"{d} (#{mapping[d]})" if d in mapping else d for d in deps) or "none"
        md = (f"{body}\n\n**Owner:** {owner} · **Expected tier:** {tier} · **Milestone:** {m}\n\n"
              f"**Depends on:** {dep_txt}\n\n### Acceptance\n" + "\n".join(f"- [ ] {a}" for a in acc) +
              "\n\nSpec: [docs/SPEC.md](../blob/main/docs/SPEC.md) · Item list: [docs/WORK-ITEMS.md](../blob/main/docs/WORK-ITEMS.md)")
        num = next((n for tt, n in issues.items() if tt.startswith(i + " ")), None)  # match by ID
        if num is None:
            num = api("POST", "issues", title=t, body=md, labels=[f"owner:{owner}", f"tier:{tier}"],
                      milestone=ms[f"{m} {NAMES[m]}"])["number"]
        elif "--update" in sys.argv and i in sys.argv:
            gh("issue", "edit", str(num), "-R", REPO, "--title", t, "--body", md)
        issues[t] = num
        mapping[i] = num
    MAP.write_text(json.dumps(mapping, indent=1) + "\n")
    # Tracking issue per milestone with a task list
    for m, n in NAMES.items():
        t = f"Tracking: {m} {n}"
        lines = [f"- [ ] #{mapping[i]} ({o})" for (i, mm, o, *_rest) in items.ITEMS if mm == m]
        body = f"Work items for milestone {m}. Exit check: see SPEC §16.\n\n" + "\n".join(lines)
        if t not in issues:
            issues[t] = api("POST", "issues", title=t, body=body, labels=["tracking"], milestone=ms[f"{m} {n}"])["number"]
    print(json.dumps({"items": len(mapping), "tracking": len(NAMES)}))

def sync_done():
    """Write factory/done/<WI>.md for every item whose GitHub issue is closed, so dependency
    checks (scripts/factory next, the factory console) see hand-owned work as finished too."""
    mapping = json.loads(MAP.read_text())
    closed = {x["number"] for x in json.loads(gh("issue", "list", "-R", REPO, "--state", "closed", "-L", "500",
                                                  "--json", "number"))}
    done = ROOT / "factory" / "done"
    done.mkdir(exist_ok=True)
    added = []
    for wid, num in mapping.items():
        marker = done / f"{wid}.md"
        if num in closed and not marker.exists():
            marker.write_text(f"{wid}: done (GitHub #{num} closed)\n")
            added.append(wid)
    print("done markers added: " + (", ".join(added) or "none"))


if __name__ == "__main__":
    if "--sync-done" in sys.argv:
        sync_done()
    else:
        main()
