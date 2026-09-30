"""Rolling baseline helpers for adaptive abnormal-move detection."""
from collections import deque
from statistics import median


class RollingMoveBaseline:
    def __init__(self, maxlen=240, min_samples=30):
        self.values=deque(maxlen=maxlen)
        self.min_samples=min_samples

    def add(self, value):
        try: value=abs(float(value))
        except (TypeError,ValueError): return
        if value >= 0:
            self.values.append(value)

    def ready(self):
        return len(self.values) >= self.min_samples

    def threshold(self, multiplier=3.0, floor=0.25):
        if not self.ready():
            return None
        vals=sorted(self.values)
        med=median(vals)
        deviations=sorted(abs(x-med) for x in vals)
        mad=median(deviations)
        robust_sigma=1.4826*mad
        return max(float(floor), med + float(multiplier)*robust_sigma)

    def is_abnormal(self, value, multiplier=3.0, floor=0.25):
        t=self.threshold(multiplier,floor)
        if t is None:
            return None
        try: return abs(float(value)) >= t
        except (TypeError,ValueError): return None
