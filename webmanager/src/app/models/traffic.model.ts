export interface TrafficCounters {
    rxPackets: number,
    rxBytes: number,
    txPackets: number,
    txBytes: number,
    dropPackets: number,
    dropBytes: number,
}

export interface TrafficStats {
    data: TrafficCounters,
    control: TrafficCounters,
}

export interface PathStatus {
    lastSeen: number,
    lastSuccess: number,
    rttMillis: number,
    rttVarianceMillis: number,
    failures: number,
    eligible: boolean,
}
