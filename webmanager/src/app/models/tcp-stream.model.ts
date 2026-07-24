export interface TCPStreamModel {
    id: string,
    protocolVersion: number,
    destination: string,
    carriers: number,
    state: string,
    recoverable?: boolean,
    carrierGeneration?: number
}
