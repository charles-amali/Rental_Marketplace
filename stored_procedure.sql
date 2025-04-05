CREATE OR REPLACE PROCEDURE sp_etl_transform_pipeline()
LANGUAGE plpgsql
AS $$
BEGIN
    -- ===========================================
    -- 1. CURATED LAYER: Transform and Load
    -- ===========================================

    -- Clear old data
    TRUNCATE curated.apartments;
    TRUNCATE curated.apartment_attributes;
    TRUNCATE curated.user_viewing;
    TRUNCATE curated.bookings;

    -- Insert cleaned apartments
    INSERT INTO curated.apartments
    SELECT *
    FROM raw.apartments
    WHERE is_active = TRUE;

    -- Insert cleaned apartment_attributes
    INSERT INTO curated.apartment_attributes
    SELECT *
    FROM raw.apartment_attributes
    WHERE latitude IS NOT NULL AND longitude IS NOT NULL;

    -- Insert cleaned user_viewing
    INSERT INTO curated.user_viewing
    SELECT *
    FROM raw.user_viewing;

    -- Insert only confirmed + paid bookings
    INSERT INTO curated.bookings
    SELECT *
    FROM raw.bookings
    WHERE booking_status = 'confirmed'
      AND payment_status = 'paid';

    -- ===========================================
    -- 2. PRESENTATION LAYER: KPI Computations
    -- ===========================================

    -- Clear old KPI data
    TRUNCATE presentation.avg_listing_price_weekly;
    TRUNCATE presentation.occupancy_rate_monthly;
    TRUNCATE presentation.popular_locations_weekly;
    TRUNCATE presentation.top_performing_listings_weekly;
    TRUNCATE presentation.total_bookings_per_user_weekly;
    TRUNCATE presentation.avg_booking_duration_weekly;
    TRUNCATE presentation.repeat_customers_rolling_30d;

    -- Average Listing Price Weekly
    INSERT INTO presentation.avg_listing_price_weekly
    SELECT
        DATE_TRUNC('week', a.listing_created_on) AS week_start,
        aa.cityname,
        AVG(a.price) AS avg_price
    FROM curated.apartments a
    JOIN curated.apartment_attributes aa ON a.id = aa.id
    GROUP BY 1, 2;

    -- Occupancy Rate Monthly
    INSERT INTO presentation.occupancy_rate_monthly
    SELECT
        DATE_TRUNC('month', checkin_date) AS month,
        aa.cityname,
        (SUM(DATEDIFF(day, checkin_date, checkout_date))::DECIMAL /
         COUNT(DISTINCT DATE_TRUNC('day', checkin_date))) * 100 AS occupancy_rate
    FROM curated.bookings b
    JOIN curated.apartment_attributes aa ON b.apartment_id = aa.id
    GROUP BY 1, 2;

    -- Most Popular Locations Weekly
    INSERT INTO presentation.popular_locations_weekly
    SELECT
        DATE_TRUNC('week', booking_date) AS week_start,
        aa.cityname,
        COUNT(*) AS total_bookings
    FROM curated.bookings b
    JOIN curated.apartment_attributes aa ON b.apartment_id = aa.id
    GROUP BY 1, 2;

    -- Top Performing Listings Weekly (by revenue)
    INSERT INTO presentation.top_performing_listings_weekly
    SELECT
        DATE_TRUNC('week', booking_date) AS week_start,
        apartment_id,
        SUM(total_price) AS revenue
    FROM curated.bookings
    GROUP BY 1, 2;

    -- Total Bookings per User Weekly
    INSERT INTO presentation.total_bookings_per_user_weekly
    SELECT
        DATE_TRUNC('week', booking_date) AS week_start,
        user_id,
        COUNT(*) AS total_bookings
    FROM curated.bookings
    GROUP BY 1, 2;

    -- Average Booking Duration Weekly
    INSERT INTO presentation.avg_booking_duration_weekly
    SELECT
        DATE_TRUNC('week', booking_date) AS week_start,
        AVG(DATEDIFF(day, checkin_date, checkout_date)) AS avg_duration_days
    FROM curated.bookings
    GROUP BY 1;

    -- Repeat Customers (rolling 30-day window)
    INSERT INTO presentation.repeat_customers_rolling_30d
    SELECT
        DISTINCT DATE_TRUNC('day', booking_date) AS date_checked,
        COUNT(DISTINCT user_id) AS repeat_customer_count
    FROM (
        SELECT
            user_id,
            booking_date,
            COUNT(*) OVER (PARTITION BY user_id ORDER BY booking_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS rolling_count
        FROM curated.bookings
    ) t
    WHERE rolling_count > 1
    GROUP BY 1;

END;
$$;





