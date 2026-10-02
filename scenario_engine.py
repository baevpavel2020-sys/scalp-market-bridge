"""Scenario and event-lifecycle layer.

Scenarios describe possible futures; they never rewrite the authoritative MTF
direction. Event lifecycle prevents the same causal event from being counted
as a new signal on every scan tick.
"""
import hashlib,json,time

def _direction(analysis,tf):
    item=(analysis or {}).get(tf) or {}
    if not item.get("ready"): return None
    state=(item.get("structure") or {}).get("state")
    return "bullish" if state=="uptrend" else "bearish" if state=="downtrend" else None

def build_scenarios(analysis_core,events):
    analysis=(analysis_core or {}).get("analysis") or {}
    d1=_direction(analysis,"1h"); d15=_direction(analysis,"15m")
    event_dirs={e.get("direction") for e in events or [] if e.get("direction") in ("bullish","bearish")}
    scenarios=[]
    if d1 and d15 and d1==d15:
        scenarios.append({"id":f"continuation_{d1}","type":"CONTINUATION","direction":d1,
                          "authority":"MTF_STRUCTURE","state":"PRIMARY","conditions":["1h_structure","15m_alignment"]})
        opposite="bearish" if d1=="bullish" else "bullish"
        if opposite in event_dirs:
            scenarios.append({"id":f"reversal_{opposite}","type":"REVERSAL_CANDIDATE","direction":opposite,
                              "authority":"EVENT_ONLY","state":"WATCH","conditions":["event_confirmation","structure_break","retest"]})
    elif d1:
        scenarios.append({"id":f"higher_tf_{d1}","type":"CONTINUATION","direction":d1,
                          "authority":"1H_STRUCTURE","state":"PROVISIONAL","conditions":["15m_confirmation"]})
    elif d15:
        scenarios.append({"id":f"lower_tf_{d15}","type":"LOCAL","direction":d15,
                          "authority":"15M_STRUCTURE","state":"PROVISIONAL","conditions":["1h_confirmation"]})
    else:
        scenarios.append({"id":"no_trade","type":"NO_TRADE","direction":None,
                          "authority":"INSUFFICIENT_STRUCTURE","state":"ACTIVE","conditions":[]})
    return scenarios

def scenario_snapshot(analysis_core,events):
    scenarios=build_scenarios(analysis_core,events)
    primary=next((s for s in scenarios if s["state"]=="PRIMARY"),scenarios[0])
    return {"primary":primary,"alternatives":scenarios[1:],"count":len(scenarios)}

def event_fingerprint(event):
    payload={k:event.get(k) for k in ("event","type","direction","level","timeframe","confirmed","confirmed_by_close")}
    return hashlib.sha256(json.dumps(payload,sort_keys=True,default=str,separators=(",",":")).encode()).hexdigest()[:20]

class EventLifecycle:
    STATES=("DETECTED","CONFIRMED","ACTIVE","TRIGGERED","CONSUMED","EXPIRED","INVALIDATED")
    def __init__(self,ttl_seconds=900):
        self.ttl=ttl_seconds; self.items={}
    def update(self,event,now=None,confirmed=False,triggered=False,invalidated=False):
        now=now or time.time(); fp=event_fingerprint(event)
        prev=self.items.get(fp)
        if invalidated: state="INVALIDATED"
        elif triggered: state="TRIGGERED"
        elif prev and prev["state"] in ("TRIGGERED","CONSUMED"): state=prev["state"]
        elif confirmed: state="CONFIRMED"
        else: state="DETECTED"
        item={"fingerprint":fp,"state":state,"first_seen":prev["first_seen"] if prev else now,
              "updated_at":now,"event":dict(event)}
        self.items[fp]=item
        return dict(item)
    def expire(self,now=None):
        now=now or time.time()
        for fp,item in list(self.items.items()):
            if now-item["updated_at"]>self.ttl and item["state"] not in ("CONSUMED","INVALIDATED"):
                item["state"]="EXPIRED"
    def snapshot(self):
        self.expire()
        return [dict(x) for x in self.items.values()]
