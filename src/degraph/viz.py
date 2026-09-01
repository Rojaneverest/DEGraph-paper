"""Self-contained interactive lineage visualizer for a DEGraph graph.

Reads a `*.graph.json` and emits ONE standalone `lineage.html` (Cytoscape.js +
dagre + expand-collapse from CDN) that renders the column-level lineage:

  * Tables are tier-banded compound nodes (source / intermediate / sink, derived
    structurally from read/write roles), with their columns nested inside.
  * Start collapsed (table-level overview); click a table to drill into its columns.
  * Click a column -> its forward IMPACT (blast radius) and backward PROVENANCE
    light up. This is faithful to the engine *by construction*: we draw exactly the
    forward column-provenance edges (the same inverted index `impact.py` walks), so
    Cytoscape's `successors()` is the impact set and `predecessors()` the provenance.
  * Edges colored by the transformation that produced the downstream column
    (derive / aggregate / group-key / passthrough / opaque); hover for details.

No server, no build step: open the HTML in any browser.

Usage:
  python -m degraph.viz results/graphs/repo_synthetic_small.graph.json
  python -m degraph.viz <graph.json> -o lineage.html
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .compact import _build_column_provenance


def _short(fqn: str) -> str:
    """Drop the catalog/schema prefix for display."""
    for p in ("main.dbdemos_ecom.", "meridian.silver.", "meridian.bronze.", "meridian."):
        if fqn.startswith(p):
            return fqn[len(p):]
    return fqn.split(".")[-1] if fqn.count(".") >= 2 else fqn


def _tiers(graph: dict) -> dict[str, str]:
    """Structural tier per table fqn: source (read-only) / sink (write-only) /
    intermediate (both). Robust even when fqns carry no bronze/silver/gold name."""
    read, written = set(), set()
    for e in graph.get("edges", []):
        if e.get("kind") == "reads":
            s = e.get("source", "")
            if s.startswith("table:"):
                read.add(s.removeprefix("table:"))
        elif e.get("kind") == "writes":
            t = e.get("target", "")
            if t.startswith("table:"):
                written.add(t.removeprefix("table:"))
    tier = {}
    for t in graph.get("tables", []):
        f = t["fqn"]
        if f in written and f in read:
            tier[f] = "mid"
        elif f in written:
            tier[f] = "sink"
        else:
            tier[f] = "source"
    return tier


def build_elements(graph: dict) -> tuple[list[dict], dict]:
    """Cytoscape elements (tier-banded table parents, role-colored column children,
    provenance edges colored by producing transformation) + a small meta dict."""
    prov = _build_column_provenance(graph)
    tier = _tiers(graph)

    # valid column node ids = every real table column, id = "fqn.col"
    valid: set[str] = set()
    elements: list[dict] = []
    for t in graph.get("tables", []):
        f = t["fqn"]
        elements.append({"data": {"id": f"tbl::{f}", "label": _short(f), "kind": "table"},
                         "classes": f"table tier-{tier.get(f, 'source')}"})
        for c in (t.get("columns") or []):
            cid = f"{f}.{c['name']}"
            valid.add(cid)
            info = prov.get(f, {}).get(c["name"], {})
            role = info.get("role", "source")
            # a column that is never produced by an edge is an origin column
            if f not in graph_written(graph):
                role = "source"
            detail = {"role": role, "from": [_short(x) for x in (info.get("from") or [])]}
            if info.get("op"):
                detail["op"] = info["op"]
            if info.get("window"):
                detail["window"] = info["window"]
            elements.append({"data": {
                "id": cid, "parent": f"tbl::{f}", "label": c["name"],
                "kind": "column", "role": role, "dtype": c.get("dtype") or "",
                "detail": _fmt_detail(detail),
            }, "classes": f"column role-{role}"})

    # Some benchmarks (clinical suffix-renames, dbdemos DLT bodies) name provenance
    # sources with intermediate ids (e.g. `spark_churn_users.creation_date`) that are
    # not real table columns. Synthesize a node for any such endpoint so the real
    # lineage still renders, grouped under a derived-table parent.
    synth_tables: set[str] = set()

    def _ensure(cid: str) -> bool:
        """Make sure a column node exists for `cid`; create a derived one if needed.
        Returns False only for un-usable ids (no column part)."""
        if cid in valid:
            return True
        if "." not in cid:
            return False
        tbl, col = cid.rsplit(".", 1)
        if not col or col.endswith(")") or col.endswith("..."):  # expr fragment, not a column
            return False
        if tbl not in synth_tables and f"tbl::{tbl}" not in {e["data"]["id"] for e in elements}:
            elements.append({"data": {"id": f"tbl::{tbl}", "label": _short(tbl), "kind": "table"},
                             "classes": "table tier-source derived-table"})
            synth_tables.add(tbl)
        elements.append({"data": {"id": cid, "parent": f"tbl::{tbl}", "label": col,
                                  "kind": "column", "role": "source", "dtype": "",
                                  "detail": "intermediate (extractor-named)"},
                         "classes": "column role-source"})
        valid.add(cid)
        return True

    # edges from the forward column-provenance index, colored by the downstream
    # column's role (the producing transformation).
    seen_edge: set[tuple[str, str]] = set()
    n_edges = 0
    for tbl, cols in prov.items():
        for col, info in cols.items():
            child = f"{tbl}.{col}"
            if child not in valid:
                continue
            role = info.get("role", "derived")
            for src in (info.get("from") or []):
                if src == child or not _ensure(src):
                    continue
                key = (src, child)
                if key in seen_edge:
                    continue
                seen_edge.add(key)
                elements.append({"data": {
                    "id": f"e{n_edges}", "source": src, "target": child, "role": role,
                }, "classes": f"edge role-{role}"})
                n_edges += 1

    n_cols = sum(1 for e in elements if e["data"].get("kind") == "column")
    meta = {
        "tables": sum(1 for e in elements if e["data"].get("kind") == "table"),
        "columns": n_cols, "edges": n_edges,
        "source_name": graph.get("metadata", {}).get("repo", "lineage"),
    }
    return elements, meta


_WRITTEN_CACHE: dict[int, set[str]] = {}


def graph_written(graph: dict) -> set[str]:
    key = id(graph)
    if key not in _WRITTEN_CACHE:
        w = set()
        for e in graph.get("edges", []):
            if e.get("kind") == "writes" and e.get("target", "").startswith("table:"):
                w.add(e["target"].removeprefix("table:"))
        _WRITTEN_CACHE[key] = w
    return _WRITTEN_CACHE[key]


def _fmt_detail(d: dict) -> str:
    parts = [f"role: {d['role']}"]
    if d.get("op"):
        parts.append(f"op: {d['op']}")
    if d.get("from"):
        parts.append("from: " + ", ".join(d["from"][:6]) + ("…" if len(d["from"]) > 6 else ""))
    if d.get("window"):
        parts.append("window: " + str(d["window"])[:80])
    return " | ".join(parts)


_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<title>DEGraph — __TITLE__</title>
<script src="https://cdn.jsdelivr.net/npm/cytoscape@3.30.2/dist/cytoscape.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/dagre@0.8.5/dist/dagre.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/cytoscape-dagre@2.5.0/cytoscape-dagre.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/cytoscape-expand-collapse@4.1.1/cytoscape-expand-collapse.js"></script>
<style>
  html,body{margin:0;height:100%;font-family:-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;}
  #cy{position:absolute;left:0;right:0;top:52px;bottom:0;background:#0f1115;}
  #bar{position:absolute;left:0;right:0;top:0;height:52px;background:#171a21;color:#e8eaed;
       display:flex;align-items:center;gap:14px;padding:0 16px;box-sizing:border-box;border-bottom:1px solid #2a2f3a;}
  #bar b{font-size:15px;} #bar .muted{color:#9aa3b2;font-size:12px;}
  #bar input{background:#0f1115;border:1px solid #2a2f3a;color:#e8eaed;border-radius:6px;padding:6px 9px;width:230px;}
  #bar button{background:#222734;border:1px solid #2a2f3a;color:#cfd6e4;border-radius:6px;padding:6px 10px;cursor:pointer;}
  #bar button:hover{background:#2b3242;}
  #legend{position:absolute;right:14px;top:66px;background:#171a21ee;border:1px solid #2a2f3a;border-radius:8px;
          padding:10px 12px;color:#cfd6e4;font-size:12px;line-height:1.7;max-width:230px;z-index:5;}
  #legend .sw{display:inline-block;width:11px;height:11px;border-radius:3px;margin-right:7px;vertical-align:middle;}
  #legend h4{margin:2px 0 6px;font-size:12px;color:#e8eaed;}
  #tip{position:absolute;z-index:9;pointer-events:none;background:#0b0d11f2;color:#e8eaed;border:1px solid #2a2f3a;
       border-radius:6px;padding:7px 9px;font-size:12px;max-width:340px;display:none;box-shadow:0 4px 18px #0008;}
  .pill{padding:1px 6px;border-radius:10px;font-size:11px;}
</style>
</head>
<body>
<div id="bar">
  <b>DEGraph lineage</b><span class="muted">__TITLE__ · __NTAB__ tables · __NCOL__ columns · __NEDGE__ edges</span>
  <input id="search" placeholder="search column… (Enter)"/>
  <button id="expand">Expand all</button>
  <button id="collapse">Collapse all</button>
  <button id="reset">Reset highlight</button>
  <button id="fit">Fit</button>
  <span class="muted">click a column → impact + provenance · click a table to drill in</span>
</div>
<div id="cy"></div>
<div id="legend">
  <h4>Tiers</h4>
  <div><span class="sw" style="background:#3a2f1a;border:1px solid #cd7f32"></span>source (read-only)</div>
  <div><span class="sw" style="background:#26303a;border:1px solid #8aa0b6"></span>intermediate</div>
  <div><span class="sw" style="background:#33291a;border:1px solid #d4af37"></span>sink (write-only)</div>
  <h4 style="margin-top:9px">Column role</h4>
  <div><span class="sw" style="background:#6b7280"></span>source</div>
  <div><span class="sw" style="background:#3b82f6"></span>derived</div>
  <div><span class="sw" style="background:#a855f7"></span>aggregate</div>
  <div><span class="sw" style="background:#14b8a6"></span>group key</div>
  <div><span class="sw" style="background:#64748b"></span>passthrough</div>
  <div><span class="sw" style="background:#ef4444"></span>opaque</div>
  <h4 style="margin-top:9px">On click</h4>
  <div><span class="sw" style="background:#ef4444"></span>downstream impact</div>
  <div><span class="sw" style="background:#22c55e"></span>upstream provenance</div>
</div>
<div id="tip"></div>
<script>
var ELEMENTS = __ELEMENTS__;
cytoscape.use(cytoscapeDagre);
cytoscape.use(cytoscapeExpandCollapse);

var cy = cytoscape({
  container: document.getElementById('cy'),
  elements: ELEMENTS,
  wheelSensitivity: 0.2,
  style: [
    {selector:'node[kind="table"]', style:{
      'label':'data(label)','font-size':13,'color':'#e8eaed','text-valign':'top','text-halign':'center',
      'text-margin-y':-4,'shape':'round-rectangle','border-width':2,'padding':10,'font-weight':'bold'}},
    {selector:'.tier-source', style:{'background-color':'#1c1810','border-color':'#cd7f32'}},
    {selector:'.tier-mid',    style:{'background-color':'#141b22','border-color':'#8aa0b6'}},
    {selector:'.tier-sink',   style:{'background-color':'#1d1710','border-color':'#d4af37'}},
    {selector:'.derived-table', style:{'border-style':'dashed','border-color':'#5b6675'}},
    {selector:'node[kind="column"]', style:{
      'label':'data(label)','font-size':10,'color':'#dfe3ea','text-valign':'center','text-halign':'center',
      'width':'label','height':16,'padding':'5px','shape':'round-rectangle','background-color':'#6b7280'}},
    {selector:'.role-derived',    style:{'background-color':'#3b82f6'}},
    {selector:'.role-aggregate',  style:{'background-color':'#a855f7'}},
    {selector:'.role-group_key',  style:{'background-color':'#14b8a6'}},
    {selector:'.role-passthrough',style:{'background-color':'#64748b'}},
    {selector:'.role-opaque',     style:{'background-color':'#ef4444'}},
    {selector:'.role-source',     style:{'background-color':'#6b7280'}},
    {selector:'edge', style:{
      'width':1.4,'line-color':'#3a4150','target-arrow-color':'#3a4150','target-arrow-shape':'triangle',
      'curve-style':'bezier','arrow-scale':0.8,'opacity':0.75}},
    {selector:'node:selected', style:{'border-width':3,'border-color':'#fde047'}},
    {selector:'.cy-expand-collapse-collapsed-node', style:{'shape':'round-rectangle'}},
    // highlight states
    {selector:'.faded', style:{'opacity':0.12}},
    {selector:'.seed', style:{'border-width':3,'border-color':'#fde047','background-color':'#fde047','color':'#111'}},
    {selector:'.imp', style:{'background-color':'#ef4444','color':'#fff'}},
    {selector:'.prov', style:{'background-color':'#22c55e','color':'#06210f'}},
    {selector:'edge.imp', style:{'line-color':'#ef4444','target-arrow-color':'#ef4444','opacity':1,'width':2.2}},
    {selector:'edge.prov', style:{'line-color':'#22c55e','target-arrow-color':'#22c55e','opacity':1,'width':2.2}},
  ],
});

var layoutOpts = {name:'dagre', rankDir:'LR', nodeSep:18, rankSep:80, edgeSep:8, animate:false};
function runLayout(){ cy.layout(layoutOpts).run(); }

var api = cy.expandCollapse({
  layoutBy: layoutOpts, fisheye:false, animate:false, undoable:false,
  expandCollapseCueSize:12, expandCollapseCueLineSize:8,
});
api.collapseAll();           // start as a table-level overview
runLayout(); cy.fit(undefined, 30);

function clearHi(){ cy.elements().removeClass('faded seed imp prov'); }
function highlight(node){
  clearHi();
  var imp = node.successors();      // forward blast radius == impact.py reachability
  var prov = node.predecessors();   // backward provenance
  cy.elements().addClass('faded');
  prov.removeClass('faded').addClass('prov');
  imp.removeClass('faded').addClass('imp');
  node.removeClass('faded').addClass('seed');
}

cy.on('tap','node[kind="column"]', function(e){ highlight(e.target); });
cy.on('tap', function(e){ if(e.target === cy){ clearHi(); } });

// tooltip
var tip = document.getElementById('tip');
cy.on('mouseover','node[kind="column"]', function(e){
  var d = e.target.data();
  tip.innerHTML = '<b>'+d.label+'</b> <span class="pill" style="background:#222">'+(d.dtype||'')+'</span><br>'+(d.detail||'');
  tip.style.display='block';
});
cy.on('mousemove','node[kind="column"]', function(e){
  var p = e.renderedPosition || e.target.renderedPosition();
  tip.style.left = (p.x+14)+'px'; tip.style.top = (p.y+58)+'px';
});
cy.on('mouseout','node[kind="column"]', function(){ tip.style.display='none'; });

// controls
document.getElementById('expand').onclick = function(){ api.expandAll(); runLayout(); cy.fit(undefined,30); };
document.getElementById('collapse').onclick = function(){ clearHi(); api.collapseAll(); runLayout(); cy.fit(undefined,30); };
document.getElementById('reset').onclick = clearHi;
document.getElementById('fit').onclick = function(){ cy.fit(undefined,30); };
document.getElementById('search').addEventListener('keydown', function(ev){
  if(ev.key!=='Enter') return;
  var q = this.value.trim().toLowerCase(); if(!q) return;
  var hit = cy.nodes('[kind="column"]').filter(function(n){ return n.data('label').toLowerCase().indexOf(q)>=0; });
  if(hit.length){
    var n = hit[0];
    if(n.parent().hasClass('cy-expand-collapse-collapsed-node') || n.style('display')==='none'){ api.expand(n.parent()); runLayout(); }
    highlight(n); cy.animate({center:{eles:n}, zoom:1.4}, {duration:350});
  }
});
</script>
</body>
</html>
"""


def render_html(elements: list[dict], meta: dict) -> str:
    return (_HTML
            .replace("__ELEMENTS__", json.dumps(elements))
            .replace("__TITLE__", str(meta.get("source_name", "lineage")))
            .replace("__NTAB__", str(meta["tables"]))
            .replace("__NCOL__", str(meta["columns"]))
            .replace("__NEDGE__", str(meta["edges"])))


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Render a DEGraph graph.json as an interactive HTML.")
    ap.add_argument("graph", help="path to a *.graph.json")
    ap.add_argument("-o", "--out", default=None, help="output HTML (default: <graph>.html next to it)")
    args = ap.parse_args(argv)

    gp = Path(args.graph)
    graph = json.loads(gp.read_text(encoding="utf-8"))
    if "source_name" not in graph.get("metadata", {}):
        graph.setdefault("metadata", {})["repo"] = gp.stem.replace(".graph", "")
    elements, meta = build_elements(graph)
    html = render_html(elements, meta)
    out = Path(args.out) if args.out else gp.with_suffix(".html")
    out.write_text(html, encoding="utf-8")
    print(f"wrote {out}  ({meta['tables']} tables, {meta['columns']} columns, {meta['edges']} edges)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
