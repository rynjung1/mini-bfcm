from dataclasses import dataclass


@dataclass
class SpikeProfile:
    baseline_rate: float
    multiplier: float
    delay_seconds: float
    ramp_seconds: float
    hold_seconds: float
    decay_seconds: float

    def rate_at(self, elapsed: float) -> float:
        """Orders/sec target at `elapsed` seconds into the run."""
        peak = self.baseline_rate * self.multiplier

        ramp_start = self.delay_seconds
        hold_start = ramp_start + self.ramp_seconds
        decay_start = hold_start + self.hold_seconds
        decay_end = decay_start + self.decay_seconds

        if elapsed < ramp_start:
            return self.baseline_rate

        if elapsed < hold_start:
            progress = (elapsed - ramp_start) / self.ramp_seconds
            return self.baseline_rate + (peak - self.baseline_rate) * progress

        if elapsed < decay_start:
            return peak

        if elapsed < decay_end:
            progress = (elapsed - decay_start) / self.decay_seconds
            return peak - (peak - self.baseline_rate) * progress

        return self.baseline_rate

    def phase_at(self, elapsed: float) -> str:
        ramp_start = self.delay_seconds
        hold_start = ramp_start + self.ramp_seconds
        decay_start = hold_start + self.hold_seconds
        decay_end = decay_start + self.decay_seconds

        if elapsed < ramp_start:
            return "baseline"
        if elapsed < hold_start:
            return "ramp"
        if elapsed < decay_start:
            return "hold"
        if elapsed < decay_end:
            return "decay"
        return "baseline"
