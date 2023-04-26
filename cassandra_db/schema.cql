CREATE KEYSPACE IF NOT EXISTS sales WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 };

CREATE TABLE IF NOT EXISTS sales.datagen_v1 (
    customer_id int,
    purch_week int,
    sku int,
    promo_cat text,
    promo_disc text,
    store_id int,
    PRIMARY KEY((purch_week, sku, store_id, promo_cat, promo_disc, customer_id))
);

CREATE TABLE IF NOT EXISTS sales.datagen_0_v1 (
    customer_id int,
    purch_week int,
    sku int,
    promo_cat text,
    promo_disc text,
    store_id int,
    PRIMARY KEY((purch_week, sku, store_id, promo_cat, promo_disc, customer_id))
);

CREATE TABLE IF NOT EXISTS sales.calendar_v1 (
    calendar_day text,
    calendar_week int,
    calendar_month int,
    calendar_year int,
    week_of_year int,
    day_of_week int,
    week_of_month int,
    PRIMARY KEY(calendar_day, calendar_week, calendar_month, calendar_year)
);

CREATE TABLE IF NOT EXISTS sales.forecasts_v1 (
    purch_week int,
    week_of_year int,
    date_time text,
    store_id int,
    store_region int,
    promo_cat text,
    promo_disc text,
    sku int,
    prod_subclass int,
    subclass_labels text,
    label int,
    prediction float,
    PRIMARY KEY((purch_week, sku, store_id), promo_cat, promo_disc, prediction)
);

CREATE TABLE IF NOT EXISTS sales.products_v1 (
    sku int,
    prod_dept int,
    dept_label text,
    prod_class int,
    class_labels text,
    prod_subclass int,
    subclass_labels text,
    subclass_coefficients int,
    prod_base_price float,
    prod_md1 float,
    start_date int,
    end_date int,
    md1_week int,
    pareto_weights float,
    margin float,
    PRIMARY KEY((sku), prod_base_price)
);