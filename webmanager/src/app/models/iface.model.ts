import { TrafficStats } from './traffic.model';

export interface IfaceModel {
    "name": string,
    "label"?: string,
    "status": string,
    "senderAddress": string,
    "dstAddress": string,
    "last": number | null,
    "traffic"?: TrafficStats,
}
