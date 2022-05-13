import { HttpClient } from '@angular/common/http';
import { Component, HostListener, Input, OnInit, ViewChild } from '@angular/core';
import { GoogleChartInterface, GoogleChartType } from 'ng2-google-charts';
import { CountryStat } from './country-stats.model';

@Component({
  selector: 'app-country-stats',
  templateUrl: './country-stats.component.html',
  styleUrls: ['./country-stats.component.css']
})
export class CountryStatsComponent implements OnInit {

  COUNTRY_STATS_URL: string = "http://localhost:5000/country";
  
  countryStats: CountryStat[] = [];
  countryNames: string[] = [];
  countryCounts: number[] = [];

  pieChart: GoogleChartInterface = {
    chartType: GoogleChartType.PieChart,
    dataTable: [
      ['Country', 'Count'],
    ],
    options: {'title': 'Country Stats', 
            'width': 700,
            'height': 700},
  };

  constructor(
    private httpClient: HttpClient
  ) { }

  getCountryStats (): void {
    this.httpClient.get<CountryStat[]>(this.COUNTRY_STATS_URL).subscribe(
      response => {
        console.log(response);
        this.countryStats = response;
        this.countryStats.map(stat => {
          console.log(stat);
          this.countryNames.push(stat.country);
          this.countryCounts.push(stat.count);
          this.pieChart.dataTable.push([stat.country, stat.count]);
        }
      )
      }
    );
  }

  ngOnInit(): void {
    this.getCountryStats()
    console.log(this.pieChart.dataTable)
  }
}
