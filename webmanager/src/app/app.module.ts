import { BrowserModule } from '@angular/platform-browser';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { NgModule } from '@angular/core';

import { AppComponent } from './app.component';
import { HttpClient, provideHttpClient, withInterceptorsFromDi } from '@angular/common/http';
import { APICallerService } from './services/apicaller.service';
import { CallbackPipe } from './pipes/callback.pipe';
import { ActionbarService } from './services/actionbar.service';
import { ActionbarComponent } from './components/actionbar/actionbar.component';
import { MaterialModule } from './modules/material/material.module';
import { SortByPipe } from './pipes/sortby.pipe';
import { DialogComponent } from './components/dialog/dialog.component';


@NgModule({ declarations: [
        AppComponent,
        CallbackPipe,
        SortByPipe,
        ActionbarComponent,
        DialogComponent
    ],
    bootstrap: [AppComponent], imports: [BrowserModule,
        BrowserAnimationsModule,
        MaterialModule], providers: [APICallerService, HttpClient, ActionbarService, provideHttpClient(withInterceptorsFromDi())] })
export class AppModule { }
