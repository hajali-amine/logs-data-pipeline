import { ComponentFixture, TestBed } from '@angular/core/testing';

import { CountryStatsComponent } from './country-stats.component';

describe('CountryStatsComponent', () => {
  let component: CountryStatsComponent;
  let fixture: ComponentFixture<CountryStatsComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ CountryStatsComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(CountryStatsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
