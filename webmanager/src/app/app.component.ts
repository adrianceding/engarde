import { ChangeDetectionStrategy, ChangeDetectorRef, Component, OnDestroy } from '@angular/core';
import { APICallerService } from './services/apicaller.service';
import { IfaceModel } from './models/iface.model';
import { SocketModel } from './models/socket.model';
import { getScaleInAnimation } from './animations/scalein.animation';
import { MatDialog } from '@angular/material/dialog';
import { getSlideOutAnimation } from './animations/slideout.animation';
import { DialogComponent } from './components/dialog/dialog.component';
import { RespModel } from './models/resp.model';
import { TrafficStats } from './models/traffic.model';
import { TCPStreamModel } from './models/tcp-stream.model';

const EMPTY_TRAFFIC: TrafficStats = {
  data: { rxPackets: 0, rxBytes: 0, txPackets: 0, txBytes: 0, dropPackets: 0, dropBytes: 0, skippedPackets: 0, skippedBytes: 0 },
  control: { rxPackets: 0, rxBytes: 0, txPackets: 0, txBytes: 0, dropPackets: 0, dropBytes: 0, skippedPackets: 0, skippedBytes: 0 },
};
const BYTE_UNITS = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
const PACKET_UNITS = ['', 'K', 'M', 'G', 'T'];

@Component({
    selector: 'app-root',
    templateUrl: './app.component.html',
    styleUrls: ['./app.component.css'],
    animations: [getScaleInAnimation(), getSlideOutAnimation()],
    changeDetection: ChangeDetectionStrategy.OnPush,
    standalone: false
})
export class AppComponent implements OnDestroy {

  public type: string
  public version: string
  public listenAddress: string
  public description: string
  public ifaces: IfaceModel[]
  public sockets: SocketModel[]
  public streams: number = 0
  public carriers: number = 0
  public sessions: number = 0
  public frontendAuthEnabled: boolean = false
  public peerAuthEnabled: boolean = false
  public tcpStreams: TCPStreamModel[] = []
  public errorMessage;
  public loaded : boolean = false;
  public hintAnimationActive : boolean = false;
  private getListErrors: number = 0;
  private listTimeout: number = null;



  constructor(public api: APICallerService, public dialog: MatDialog, private changeDetector: ChangeDetectorRef) { }

  getList() {
    this.listTimeout = null;
    const startTime = Date.now()
    this.api.getList().subscribe((resp: RespModel) => {
      this.loaded = true;
      this.type = resp.type
      this.version = resp.version
      this.description = resp.description
      this.listenAddress = resp.listenAddress
      this.streams = resp.streams || 0
      this.carriers = resp.carriers || 0
      this.sessions = resp.sessions || 0
      this.frontendAuthEnabled = !!resp.frontendAuthEnabled
      this.peerAuthEnabled = !!resp.peerAuthEnabled
      this.tcpStreams = resp.tcpStreams || []
      if(this.type == 'client') {
        this.ifaces = resp.interfaces
      } else {
        this.sockets = resp.sockets
      }
      this.getListErrors = 0;
      this.errorMessage = null;
      this.changeDetector.markForCheck();
      const callDuration = Date.now() - startTime
      if(this.listTimeout) {
        clearTimeout(this.listTimeout)
      }
      this.listTimeout = window.setTimeout(() => { this.getList() }, Math.max(1000 - callDuration, 0))
    }, err => {
      this.getListErrors += 1;
      if(this.getListErrors >= 2) {
        this.type = null;
        this.loaded = true;
        this.errorMessage = err.statusText || err.message ;
        if(this.listTimeout) {
          clearTimeout(this.listTimeout)
        }
        this.listTimeout = window.setTimeout(() => { this.getList() }, 1000);
      } else {
        const callDuration = Date.now() - startTime
        if(this.listTimeout) {
          clearTimeout(this.listTimeout)
        }
        this.listTimeout = window.setTimeout(() => { this.getList() }, Math.max(1000 - callDuration, 0))
      }
      this.changeDetector.markForCheck();
    })
  }

  filterExcluded(iface: IfaceModel) {
    return iface.status == 'excluded'
  }

  filterIncluded(iface: IfaceModel) {
    return iface.status != 'excluded'
  }

  interfaceCount(status: string): number {
    return (this.ifaces || []).filter(iface => iface.status == status).length;
  }

  destinationAddress(): string {
    const iface = (this.ifaces || []).find(item => !!item.dstAddress);
    return iface ? iface.dstAddress : '--';
  }


  trackByName(index, iface) {
    return iface.name;
  }

  trackByAddress(index, iface) {
    return iface.address;
  }

  trackByStreamId(index, stream) {
    return stream.id;
  }

  emptyTraffic(): TrafficStats {
    return EMPTY_TRAFFIC;
  }

  traffic(item: IfaceModel | SocketModel): TrafficStats {
    return item && item.traffic ? item.traffic : this.emptyTraffic();
  }

  formatBytes(bytes: number = 0): string {
    if (!bytes) {
      return '0 B';
    }
    let value = bytes;
    let unit = 0;
    while (value >= 1024 && unit < BYTE_UNITS.length - 1) {
      value = value / 1024;
      unit += 1;
    }
    return `${value >= 10 || unit === 0 ? value.toFixed(0) : value.toFixed(1)} ${BYTE_UNITS[unit]}`;
  }

  formatPackets(packets: number = 0): string {
    if (!packets) {
      return '0 pkts';
    }
    if (packets == 1) {
      return '1 pkt';
    }
    let value = packets;
    let unit = 0;
    while (value >= 1000 && unit < PACKET_UNITS.length - 1) {
      value = value / 1000;
      unit += 1;
    }
    const formatted = unit === 0 ? value.toFixed(0) : (value >= 10 ? value.toFixed(0) : value.toFixed(1));
    return `${formatted}${PACKET_UNITS[unit]} pkts`;
  }

  formatPacketBytes(packets: number = 0, bytes: number = 0): string {
    return `${this.formatPackets(packets)} / ${this.formatBytes(bytes)}`;
  }

  toggleExclude(ifname: string) {
    let activeIfaces = this.ifaces.filter(i => i.status == "active");
    if (activeIfaces.length == 1  && activeIfaces[0].name === ifname) {
        this.dialog.open(DialogComponent, { data:{
          title: "OCIO! WARNING!",
          content: `Ehi, wait a second. You're going to exclude the only active interface.
          This way, the tunnel will go down FOR SURE! Do you <b>REALLY</b> want to proceed?`,
          thingsToBeDone : [
            {
                label: "NO - Please save me and let engarde live.",
              whatToDo: () => { this.dialog.closeAll()}
            },
            {
              label : "YES - I want engarde to drop!",
              whatToDo : () => { this.dialog.closeAll(); this.api.toggleOverride(ifname)}
            }
        ]
        } })
    } else {
      this.api.toggleOverride(ifname)
    }
  }

  resetExcludes() {
    this.api.clearOverrides()
  }

  ngOnInit() {
    this.getList();
  }

  ngOnDestroy() {
    if (this.listTimeout) {
      clearTimeout(this.listTimeout);
      this.listTimeout = null;
    }
  }
}
