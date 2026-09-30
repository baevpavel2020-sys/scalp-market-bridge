"""Bounded causal event memory for Crypto Manipulation x25.

Stores only confirmed stage anchors per symbol. It never creates evidence from
missing data and never mixes symbols. Memory can bridge separate snapshots while
remaining bounded by a configurable age window.
"""
from dataclasses import dataclass
from typing import Dict, Mapping


STAGES=("abnormal_pump","exhaustion","failed_acceptance",
        "structure_break","bearish_displacement","failed_retest")


@dataclass
class EventAnchor:
    timestamp: int
    source: str


class CausalEventMemory:
    def __init__(self, max_age_seconds=2700):
        self.max_age_seconds=int(max_age_seconds)
        self._symbols: Dict[str, Dict[str,EventAnchor]]={}

    def _bucket(self,symbol):
        key=str(symbol).upper()
        return self._symbols.setdefault(key,{})

    def update(self, symbol, timestamp, confirmations: Mapping[str,bool]):
        try: now=int(timestamp)
        except (TypeError,ValueError): return self.snapshot(symbol, None)

        bucket=self._bucket(symbol)
        for stage in STAGES:
            # Preserve the first confirmation inside the causal window. Replacing
            # it on every snapshot would move Pump/Exhaustion timestamps forward
            # and could destroy a genuinely valid event sequence.
            if confirmations.get(stage) and stage not in bucket:
                bucket[stage]=EventAnchor(now,"confirmed")

        cutoff=now-self.max_age_seconds
        for stage,anchor in list(bucket.items()):
            if anchor.timestamp < cutoff:
                del bucket[stage]
        return self.snapshot(symbol,now)

    def snapshot(self,symbol,now=None):
        bucket=self._bucket(symbol)
        cutoff=None if now is None else int(now)-self.max_age_seconds
        if cutoff is not None:
            bucket={k:v for k,v in bucket.items() if v.timestamp>=cutoff}

        anchors={k:{"timestamp":v.timestamp,"source":v.source} for k,v in bucket.items()}
        order_ok=None
        sequence_complete=False
        if all(stage in bucket for stage in STAGES):
            times=[bucket[s].timestamp for s in STAGES]
            order_ok=all(a<=b for a,b in zip(times,times[1:]))
            sequence_complete=bool(order_ok)

        return {
            "window_seconds":self.max_age_seconds,
            "anchors":anchors,
            "sequence":list(STAGES),
            "sequence_complete":sequence_complete,
            "causal_order_confirmed":order_ok,
        }
