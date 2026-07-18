export interface TrafficCounters {
    rxPackets: number,
    rxBytes: number,
    txPackets: number,
    txBytes: number,
    dropPackets: number,
    dropBytes: number,
    skippedPackets?: number,
    skippedBytes?: number,
}

export interface TrafficStats {
    data: TrafficCounters,
    control: TrafficCounters,
}
