import { PathStatus, TrafficStats } from './traffic.model';

export interface IfaceModel {
    "name": string,
    "label"?: string,
    "status": string,
    "senderAddress": string,
    "dstAddress": string,
    "last": number,
    "pathRole"?: string,
    "traffic"?: TrafficStats,
    "path"?: PathStatus,
}
