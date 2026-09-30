"""Bounded point-in-time recorder for x25 shadow evaluation."""
import json
import os
import time


class ShadowRecorder:
    def __init__(self, root=".scan/shadow_x25", max_files_per_symbol=2000):
        self.root=root
        self.max_files_per_symbol=max_files_per_symbol

    @staticmethod
    def _safe_symbol(symbol):
        return "".join(c for c in str(symbol).upper() if c.isalnum() or c in ("-","_"))[:40]

    def record(self, scan, event_engine):
        symbol=self._safe_symbol(scan.get("symbol") or "UNKNOWN")
        directory=os.path.join(self.root,symbol)
        os.makedirs(directory,exist_ok=True)
        ts=int(float(scan.get("generated_at") or time.time())*1000)
        path=os.path.join(directory,f"{ts}.json")
        payload={"recorded_at":time.time(),"symbol":symbol,"scan":scan,"event_engine":event_engine}
        tmp=path+".tmp"
        with open(tmp,"w",encoding="utf-8") as fh:
            json.dump(payload,fh,ensure_ascii=False,separators=(",",":"),default=str)
        os.replace(tmp,path)
        self._prune(directory)
        return path

    def _prune(self,directory):
        files=sorted(x for x in os.listdir(directory) if x.endswith(".json"))
        excess=len(files)-self.max_files_per_symbol
        for name in files[:max(0,excess)]:
            try: os.remove(os.path.join(directory,name))
            except FileNotFoundError: pass


def load_recorded_snapshots(directory):
    out=[]
    if not os.path.isdir(directory): return out
    for name in sorted(x for x in os.listdir(directory) if x.endswith(".json")):
        with open(os.path.join(directory,name),"r",encoding="utf-8") as fh:
            payload=json.load(fh)
        if isinstance(payload.get("scan"),dict):
            out.append(payload["scan"])
    return out
