import { TrafficStats } from './traffic.model';

export interface SocketModel {
    "address": string,
    "traffic"?: TrafficStats,
}
