from typing import Any, Mapping


def build_operator_view(decision: Mapping[str, Any], candidate_limit=10, watch_limit=8):
    candidate_limit=max(0,int(candidate_limit))
    watch_limit=max(0,int(watch_limit))
    results=list(decision.get('results') or [])
    candidates=[x for x in results if x.get('status') in ('CANDIDATE','SETUP')]
    watches=[x for x in results if x.get('status')=='WATCH']
    errors=list(decision.get('errors') or [])
    def compact(x):
        return {k:x.get(k) for k in (
            'market','symbol','status','direction','situation','reason',
            'priority','mtf_gate_blocked','mtf_state'
        )}
    return {'summary':{'scanned':int(decision.get('count') or 0),'candidates':len(candidates),'shown_candidates':min(len(candidates),int(candidate_limit)),'watch':len(watches),'shown_watch':min(len(watches),int(watch_limit)),'errors':len(errors)},'shortlist':[compact(x) for x in candidates[:int(candidate_limit)]],'watchlist':[compact(x) for x in watches[:int(watch_limit)]],'errors':errors,'policy':{'presentation_only':True,'no_candidate_rewrite':True,'no_universal_score':True,'no_probability_claim':True,'provenance_preserved':True,'mtf_gate_reason_preserved':True}}
