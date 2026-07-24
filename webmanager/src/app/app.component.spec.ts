import { CommonModule } from '@angular/common';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed, waitForAsync } from '@angular/core/testing';
import { MatDialog } from '@angular/material/dialog';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { NEVER } from 'rxjs';
import { AppComponent } from './app.component';
import { CallbackPipe } from './pipes/callback.pipe';
import { SortByPipe } from './pipes/sortby.pipe';
import { APICallerService } from './services/apicaller.service';

describe('AppComponent', () => {
  let fixture: ComponentFixture<AppComponent>;
  let component: AppComponent;

  const api = {
    getList: jasmine.createSpy('getList').and.returnValue(NEVER),
    toggleOverride: jasmine.createSpy('toggleOverride'),
    clearOverrides: jasmine.createSpy('clearOverrides')
  };

  beforeEach(waitForAsync(() => {
    TestBed.configureTestingModule({
      imports: [CommonModule, NoopAnimationsModule],
      declarations: [AppComponent, CallbackPipe, SortByPipe],
      providers: [
        { provide: APICallerService, useValue: api },
        { provide: MatDialog, useValue: jasmine.createSpyObj('MatDialog', ['open', 'closeAll']) }
      ],
      schemas: [NO_ERRORS_SCHEMA]
    }).compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(AppComponent);
    component = fixture.componentInstance;
    component.version = 'test';
    component.description = 'TCP SOCKS5';
  });

  it('renders the TCP SOCKS5 client summary and interface controls', () => {
    component.type = 'client';
    component.ifaces = [];
    component.streams = 2;
    component.carriers = 3;
    component.sessions = 1;
    component.carrierMode = 'active-standby';
    component.recovering = 1;

    fixture.detectChanges();

    const root: HTMLElement = fixture.nativeElement;
    expect(root.textContent).toContain('SOCKS5 over TCP');
    expect(root.textContent).toContain('Carriers');
    expect(root.textContent).toContain('Sessions');
    expect(root.querySelectorAll('.tcp-overview .tcp-summary-item').length).toBe(7);
    expect(root.textContent).toContain('active-standby');
    expect(root.textContent).toContain('Excluded interfaces');
  });

  it('renders connecting and unavailable interface quality states', () => {
    component.type = 'client';
    component.carrierMode = 'active-standby';
    component.ifaces = [
      {
        name: 'path-a',
        status: 'idle',
        senderAddress: '192.0.2.10',
        dstAddress: '198.51.100.1:59501',
        last: null,
        qualityState: 'connecting'
      },
      {
        name: 'path-b',
        status: 'idle',
        senderAddress: '',
        dstAddress: '198.51.100.1:59501',
        last: null
      }
    ];

    fixture.detectChanges();

    const root: HTMLElement = fixture.nativeElement;
    const qualityStates = Array.from(root.querySelectorAll('.quality-state'));
    expect(qualityStates.map(item => item.textContent.trim())).toEqual(['connecting', 'unavailable']);
    expect(qualityStates[0].classList).toContain('quality-connecting');
    expect(qualityStates[1].classList).toContain('quality-unavailable');
    expect(root.querySelector('.quality-unhealthy')).toBeNull();
  });

  it('renders SOCKS5 relay streams without removed socket path metadata', () => {
    component.type = 'server';
    component.sockets = [{ address: '127.0.0.1:1080' }];
    component.tcpStreams = [{
      id: 'stream-1',
      protocolVersion: 3,
      destination: 'example.com:443',
      carriers: 2,
      state: 'active',
      recoverable: true,
      carrierGeneration: 3
    }];

    fixture.detectChanges();

    const root: HTMLElement = fixture.nativeElement;
    expect(root.textContent).toContain('SOCKS5 relay');
    expect(root.querySelectorAll('.tcp-overview .tcp-summary-item').length).toBe(6);
    expect(root.querySelector('.stream-destination').textContent).toContain('example.com:443');
    expect(root.querySelector('.tcp-stream-row:not(.tcp-stream-header)').textContent).toContain('3');
    expect(root.textContent).not.toContain('Last received packet');
  });

  it('formats TCP traffic counters with binary byte units', () => {
    expect(component.formatPacketBytes(1500, 1536)).toBe('1.5K pkts / 1.5 KiB');
  });
});
