"""Scan+ V4 Block 3 analytical-core contracts."""
import hashlib, json

VERSION="analytical_core_v4_block3"
HARMONIC_FAMILIES=("Gartley","Bat","Alternate Bat","Butterfly","Crab","Deep Crab","Cypher","Shark","5-0","AB=CD","Extended AB=CD")

def _fp(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,default=str,separators=(",",":")).encode()).hexdigest()[:18]

def causal_evidence_contract(graph):
    """Deduplicate derived confirmations without turning evidence into vote counting."""
    g=dict(graph or {})
    seen=set(); confirmations=[]; duplicates=[]
    for item in g.get("confirmations",[]) or []:
        if not isinstance(item,dict): continue
        basis=tuple(sorted(str(x) for x in (item.get("derived_from") or [])))
        value=item.get("value")
        direction=value.get("direction") if isinstance(value,dict) else None
        key=(basis,direction,item.get("type"))
        if key in seen:
            duplicates.append({"source":item.get("source"),"type":item.get("type"),"derived_from":list(basis)})
            continue
        seen.add(key); confirmations.append(item)
    g["confirmations"]=confirmations
    g["deduplicated_confirmations"]=duplicates
    g["vote_counting"]=False
    g["contract_version"]=VERSION
    return g

def elliott_degree_contract(by_degree, preferred_degree=None):
    valid={k:v for k,v in (by_degree or {}).items() if isinstance(v,dict) and v.get("ready")}
    order=("major","intermediate","minor")
    higher=next((d for d in order if d in valid and (valid[d].get("primary") or valid[d].get("candidates"))),None)
    selected=preferred_degree if preferred_degree in valid else higher
    return {"contract_version":VERSION,"degrees":valid,"selected_degree":selected,
            "higher_degree_anchor":higher,"preservation_rule":"higher_degree_count_survives_lower_degree_recounts_until_its_own_hard_invalidation",
            "overlap_rule":"wave_1_4_overlap_invalidates_standard_impulse_but_may_support_a_separately_validated_diagonal",
            "direction_authority":False}

def harmonic_contract(harmonics):
    h=dict(harmonics or {})
    found=set()
    for bucket in ("confirmed","developing"):
        for p in h.get(bucket,[]) or []:
            if isinstance(p,dict) and p.get("name"): found.add(p["name"])
    h["contract_version"]=VERSION
    h["supported_families"]=list(HARMONIC_FAMILIES)
    h["detected_families"]=sorted(found)
    h["role"]="PRZ_context_not_reversal_command"
    return h

def analytical_core_contract(structure,fibonacci,elliott,harmonics,divergences,liquidity,smc,technical,evidence):
    modules={"structure":structure,"fibonacci":fibonacci,"elliott":elliott,"harmonics":harmonics,
             "divergences":divergences,"liquidity":liquidity,"smart_money":smc,"technical":technical}
    readiness={k:bool((v or {}).get("ready")) if isinstance(v,dict) else False for k,v in modules.items()}
    return {"version":VERSION,"readiness":readiness,"evidence":causal_evidence_contract(evidence),
            "authority_order":["structure","liquidity_event","causal_smc","elliott_fib_harmonic_context","divergence","momentum"],
            "principles":{"structure_is_fact_authority":True,"patterns_cannot_override_structure":True,
                          "hard_invalidation_beats_confluence":True,"double_counting_forbidden":True,
                          "long_and_short_are_evaluated_downstream":True}}
