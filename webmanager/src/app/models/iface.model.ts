import { PathStatus, TrafficStats } from './traffic.model';

export interface IfaceModel {
    "name": string,
    "label"?: string,
    "status": string,
    "senderAddress": string,
    "dstAddress": string,
    "last": number,
    "primary"?: boolean,
    "traffic"?: TrafficStats,
    "path"?: PathStatus,
}
