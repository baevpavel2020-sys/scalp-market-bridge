"""Read recorded shadow data and produce a JSON-safe evaluation report."""
import os

from scan_plus.markets.crypto.evaluator import evaluate_shadow
from scan_plus.markets.crypto.shadow_recorder import load_recorded_snapshots


def evaluate_recorded_symbol(symbol, root=".scan/shadow_x25", **kwargs):
    safe="".join(c for c in str(symbol).upper() if c.isalnum() or c in ("-","_"))[:40]
    directory=os.path.join(root,safe)
    snapshots=load_recorded_snapshots(directory)
    report=evaluate_shadow(snapshots,**kwargs)
    report["symbol"]=safe
    report["source"]="recorded_point_in_time_shadow_snapshots"
    return report
