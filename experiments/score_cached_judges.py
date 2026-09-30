"""Score archived judge selections against the canonical current author labels.
No model calls. Archived selections and candidate lists remain unchanged.
The derived output records which author labels changed and the source hashes.
"""
from __future__ import annotations
import hashlib, json
from collections import defaultdict
from pathlib import Path
from impact_eval import SCENARIOS

ROOT = Path(__file__).resolve().parent.parent

def score_records(records):
    truth = {(b, f"{t}.{c}"): gt for b, t, c, gt in SCENARIOS}
    updated, changes = [], []
    for r in records:
        key = (r['bench'], r['change'])
        if key not in truth:
            raise ValueError(f"Archived scenario absent from canonical labels: {key}")
        label = int(r['candidate'] in truth[key])
        if label != r['author']:
            changes.append({**r, 'corrected_author': label})
        updated.append({**r, 'author': label})
    out = {}
    for model in sorted({r['judge'] for r in updated}):
        rows = [r for r in updated if r['judge'] == model]
        n = len(rows)
        counts = {str(a)+str(j): sum(r['author']==a and r['judge_label']==j for r in rows)
                  for a in [0,1] for j in [0,1]}
        agreement = (counts['00']+counts['11']) / n
        pa = (counts['10']+counts['11']) / n
        pj = (counts['01']+counts['11']) / n
        pe = pa*pj + (1-pa)*(1-pj)
        out[model] = {'n': n, 'n_scenarios':len({(r['bench'],r['change']) for r in rows}),
                      'author_positive':counts['10']+counts['11'],
                      'agreement':agreement,'kappa':(agreement-pe)/(1-pe),
                      'confusion_author_judge':counts}
    return out, changes

def main():
    source = ROOT/'results/metrics/llm_judge_impact.json'
    summary, changes = score_records(json.loads(source.read_text()))
    out = {'source_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),
           'canonical_labels_sha256':hashlib.sha256((ROOT/'experiments/impact_eval.py').read_bytes()).hexdigest(),
           'method':'Archived candidate lists and judge responses; author labels aligned to current impact scenarios; no new judge calls.',
           'summary':summary,'author_label_corrections':changes}
    dest = ROOT/'results/metrics/llm_judge_impact_reconciled.json'
    dest.write_text(json.dumps(out, indent=2)+'\n')
    print(json.dumps(summary,indent=2));print('Corrected author entries:',len(changes))
    print('Saved',dest)

if __name__=='__main__':main()
