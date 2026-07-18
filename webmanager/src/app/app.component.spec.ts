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

    fixture.detectChanges();

    const root: HTMLElement = fixture.nativeElement;
    expect(root.textContent).toContain('SOCKS5 over TCP');
    expect(root.textContent).toContain('Carriers');
    expect(root.textContent).toContain('Sessions');
    expect(root.querySelectorAll('.tcp-overview .tcp-summary-item').length).toBe(5);
    expect(root.textContent).toContain('Excluded interfaces');
  });

  it('renders SOCKS5 relay streams without removed socket path metadata', () => {
    component.type = 'server';
    component.sockets = [{ address: '127.0.0.1:1080' }];
    component.tcpStreams = [{
      id: 'stream-1',
      protocolVersion: 3,
      destination: 'example.com:443',
      carriers: 2,
      state: 'active'
    }];

    fixture.detectChanges();

    const root: HTMLElement = fixture.nativeElement;
    expect(root.textContent).toContain('SOCKS5 relay');
    expect(root.querySelectorAll('.tcp-overview .tcp-summary-item').length).toBe(4);
    expect(root.querySelector('.stream-destination').textContent).toContain('example.com:443');
    expect(root.textContent).not.toContain('Last received packet');
  });

  it('formats TCP traffic counters with binary byte units', () => {
    expect(component.formatPacketBytes(1500, 1536)).toBe('1.5K pkts / 1.5 KiB');
  });
});
