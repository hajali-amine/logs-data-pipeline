import { HttpClient } from '@angular/common/http';
import { ThisReceiver } from '@angular/compiler';
import { Component, OnInit } from '@angular/core';
import { ApiStat } from './api-stats.model';

@Component({
  selector: 'app-api-stats',
  templateUrl: './api-stats.component.html',
  styleUrls: ['./api-stats.component.css']
})
export class ApiStatsComponent implements OnInit {

  API_STATS_URL: string = "http://localhost:5000/api";
  displayedColumns: string[] = ['api', 'avg', 'min', 'max'];

  apiStats: ApiStat[] = [];
  constructor(
    private httpClient: HttpClient
  ) { }

  getApiStats (): void {
    this.httpClient.get<ApiStat[]>(this.API_STATS_URL).subscribe(
      response => {
        console.log(response);
        this.apiStats = response;
      }
    );
  }

  ngOnInit(): void {
    this.getApiStats();
  }

}
