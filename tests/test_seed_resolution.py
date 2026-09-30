"""Regression tests for identifier identity, uncertainty and cycles."""
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from degraph.impact import column_impact, _resolve_seed_keys, impact_report

def test_no_cross_table_fallback():
    assert column_impact({},'silver.missing','id',_fwd={'silver.other.id':{'gold.report.id'}})==set()

def test_no_substring_match():
    assert column_impact({},'silver.customers','id',_fwd={'silver.customers_archive.id':{'gold.report.id'}})==set()

def test_qualifier_conflict():
    assert column_impact({},'catalog_a.silver.customers','id',_fwd={'catalog_b.silver.customers.id':{'gold.report.id'}})==set()

def test_short_name_must_be_unambiguous():
    f={'a.silver.customers.id':{'gold.a.id'},'b.silver.customers.id':{'gold.b.id'}}
    assert column_impact({},'customers','id',_fwd=f)==set()
    assert column_impact({},'a.silver.customers','id',_fwd=f)=={'gold.a.id'}

def test_compatible_missing_qualifiers():
    f={'customers.id':{'gold.report.id'}}
    assert column_impact({},'main.silver.customers','id',_fwd=f)=={'gold.report.id'}

def test_unqualified_heuristic_is_reported_as_such():
    assert _resolve_seed_keys({'status','gold.out.state'},'bronze.input','status')==(['status'],'unqualified_heuristic')
    assert _resolve_seed_keys({'status','another.table.status'},'bronze.input','status')==([], 'unresolved')

def test_unresolved_report_is_not_a_safety_verdict():
    r=impact_report({},'missing','id')
    assert r['status']=='cannot_determine'
    assert r['coverage_complete'] is False


def test_exact_identifier_transitive_reachability():
    f={'bronze.src.id':{'silver.mid.id'},'silver.mid.id':{'gold.end.id'}}
    assert column_impact({},'bronze.src','id',_fwd=f)=={'silver.mid.id','gold.end.id'}
