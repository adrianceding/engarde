import { PathStatus, TrafficStats } from './traffic.model';

export interface SocketModel {
    "address": string,
    "last": number,
    "pathRole"?: string,
    "traffic"?: TrafficStats,
    "path"?: PathStatus,
}
