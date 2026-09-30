"""Re-score authorized restricted bundles; never fetch or publish them.
Requires the industrial extractor checkout used to build column provenance.
The public source-only benchmark artifact does not include those corpora/bundles.
"""
import argparse, importlib.util, json, hashlib, sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]

def module(name,path):
    spec=importlib.util.spec_from_file_location(name,path)
    obj=importlib.util.module_from_spec(spec);spec.loader.exec_module(obj);return obj

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--tool-src',required=True,type=Path)
    p.add_argument('--bundles',required=True,type=Path)
    p.add_argument('--output',required=True,type=Path)
    args=p.parse_args()
    sys.path.insert(0,str(args.tool_src.resolve()))
    from degraph import eval_impact
    frozen=module('degraph.audit_frozen',ROOT/'experiments/frozen_impact.py')
    repaired=module('degraph.audit_repaired',ROOT/'src/degraph/impact.py')
    result={'scope':'Re-scoring fixed anonymized graphs, not re-extraction of industrial source.',
            'provenance_source_sha256':hashlib.sha256((args.tool_src/'degraph/compact.py').read_bytes()).hexdigest(),
            'seed_source_sha256':hashlib.sha256((ROOT/'src/degraph/impact.py').read_bytes()).hexdigest(),
            'pipelines':{}}
    for label in ['pipeline_a','pipeline_b']:
        path=args.bundles/(label+'.evalbundle.json');b=json.loads(path.read_text())
        data={'bundle_sha256':hashlib.sha256(path.read_bytes()).hexdigest(),'n_scenarios':len(b['scenarios'])}
        for name,implementation in [('frozen',frozen),('repaired',repaired)]:
            eval_impact.column_impact=implementation.column_impact
            score=eval_impact.score_scenarios(b['graph'],b['scenarios'])['pooled']
            if score['tp']+score['fp']==0:score['precision']=None
            data[name]=score
        result['pipelines'][label]=data
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result['pipelines'],indent=2))

if __name__=='__main__':main()
