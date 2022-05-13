import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ApiStatsComponent } from './api-stats.component';

describe('ApiStatsComponent', () => {
  let component: ApiStatsComponent;
  let fixture: ComponentFixture<ApiStatsComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ ApiStatsComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ApiStatsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
