CREATE OR REPLACE VIEW transportation.gold.fact_trips AS (
  SELECT 
    t.id,
    t.business_date, 
    t.city_id,
    c.city_name,
    t.passenger_category,
    t.distance_kms,
    t.sales_amt,
    t.passenger_rating,
    t.driver_rating,
    ca.month,
    ca.day_of_month,
    ca.day_of_week,
    ca.month_name,
    ca.month_year,
    ca.quarter,
    ca.quarter_year,
    ca.week_of_year,
    ca.is_weekday,
    ca.is_weekend,
    ca.is_holiday AS national_holiday
  FROM transportation.silver.trips AS t 
  JOIN transportation.silver.city AS c ON t.city_id = c.city_id 
  JOIN transportation.silver.calender AS ca ON t.business_date = ca.date
);