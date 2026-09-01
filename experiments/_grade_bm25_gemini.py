"""T3 — Gemini BM25 baseline grades (sealed test set) + 3-mode comparison.

CRITICAL FINDING: at MATCHED token budgets (BM25 retrieved up to the DSL's
token count), token-matched BM25 retrieval BEATS the DEGraph DSL on all three
benchmarks for Gemini 2.0 Flash. This is publication_roadmap.md Risk #2.

  Gemini 2.0 Flash (sealed test, /40 per benchmark):
    bench     RAW    BM25   DSL(v1.2)
    small     80.0   77.5   75.0
    clinical  87.5   82.5   77.5
    medium    90.0   85.0   72.5
    POOLED    85.8   81.7   75.0     (BM25 - DSL = +6.7pp)

WHY: BM25 wins LOCAL code-detail questions (it retrieves the actual source, so
struct-field lists / predicate text / normalization quirks / comment
discrepancies are answerable). DSL only wins GLOBAL whole-repo questions where
retrieval locality fails (TQ2 orphan tables via rb=, TQ13 dead column via pc=).
The test questions — authored from raw source — skew toward local detail, which
favors RAW/BM25 over the DSL abstraction.

Implication: the DSL's value is NOT "higher accuracy than retrieval." It is
(a) global/whole-repo completeness in a fixed budget, (b) a deterministic,
queryable structured artifact, (c) explicit unknowns/warnings. Paper framing
must change accordingly (see strategic note in research_status.md).
"""

# Per-question grades (TQ1..TQ20), Gemini BM25, manually graded vs the sealed
# reference answers.
BM25_GEMINI = {
    "small":    [1,2,2,1,1,1,2,0,1,1, 2,2,2,2,2,2,2,1,2,2],  # 31
    "clinical": [2,2,1,1,1,2,2,1,2,2, 2,2,2,2,2,2,2,1,1,1],  # 33
    "medium":   [2,1,2,2,2,2,2,2,2,2, 2,1,0,2,2,1,2,2,1,2],  # 34
}
RAW_GEMINI = {"small": 32, "clinical": 35, "medium": 36}
DSL_GEMINI_V12 = {"small": 30, "clinical": 31, "medium": 29}


def main():
    print("Gemini 2.0 Flash — RAW / BM25 / DSL (sealed test, /40):\n")
    print(f"{'bench':10s} {'RAW':>6s} {'BM25':>6s} {'DSL':>6s}  BM25-DSL")
    tot = {"raw": 0, "bm25": 0, "dsl": 0}
    for b in ["small", "clinical", "medium"]:
        raw, bm = RAW_GEMINI[b], sum(BM25_GEMINI[b])
        dsl = DSL_GEMINI_V12[b]
        tot["raw"] += raw; tot["bm25"] += bm; tot["dsl"] += dsl
        print(f"{b:10s} {raw/40*100:5.1f}% {bm/40*100:5.1f}% {dsl/40*100:5.1f}%  {(bm-dsl)/40*100:+5.1f}pp")
    print("-" * 44)
    print(f"{'POOLED':10s} {tot['raw']/120*100:5.1f}% {tot['bm25']/120*100:5.1f}% "
          f"{tot['dsl']/120*100:5.1f}%  {(tot['bm25']-tot['dsl'])/120*100:+5.1f}pp")
    print("\nBM25 > DSL on all 3 benchmarks. See module docstring for interpretation.")


if __name__ == "__main__":
    main()
