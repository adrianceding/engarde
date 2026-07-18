import { IfaceModel } from './iface.model';
import { SocketModel } from './socket.model';
import { TCPStreamModel } from './tcp-stream.model';

export interface RespModel {
    type: string,
    version: string,
    listenAddress: string,
    interfaces?: IfaceModel[],
    sockets?: SocketModel[],
    description?: string,
    frontendAuthEnabled?: boolean,
    peerAuthEnabled?: boolean,
    tcpStreams?: TCPStreamModel[],
    streams?: number,
    carriers?: number,
    sessions?: number
}
