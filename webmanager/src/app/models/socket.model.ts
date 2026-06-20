import { PathStatus, TrafficStats } from './traffic.model';

export interface SocketModel {
    "address": string,
    "last": number,
    "primary"?: boolean,
    "traffic"?: TrafficStats,
    "path"?: PathStatus,
}
