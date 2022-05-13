import { NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';
import { HttpClientModule} from '@angular/common/http'
import {MatTableModule} from '@angular/material/table'
import { Ng2GoogleChartsModule } from 'ng2-google-charts';

import { AppRoutingModule } from './app-routing.module';
import { AppComponent } from './app.component';
import { ApiStatsComponent } from './api-stats/api-stats.component';
import { CountryStatsComponent } from './country-stats/country-stats.component';

@NgModule({
  declarations: [
    AppComponent,
    ApiStatsComponent,
    CountryStatsComponent
  ],
  imports: [
    BrowserModule,
    AppRoutingModule,
    HttpClientModule,
    MatTableModule,
    Ng2GoogleChartsModule
  ],
  providers: [],
  bootstrap: [AppComponent]
})
export class AppModule { }
