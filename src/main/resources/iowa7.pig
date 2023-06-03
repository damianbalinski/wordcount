alcohol = LOAD '$input_file' USING PigStorage(',') AS (purchase_date: DateTime, city: chararray, price: float,
            pure_alcohol_volume: float, store_name: chararray);

grouped_alcohol = GROUP alcohol BY (purchase_date, city);
result = FOREACH grouped_alcohol GENERATE group.purchase_date, group.city,
        SUM(alcohol.pure_alcohol_volume) AS total_alcohol_consumption,
        SUM(alcohol.pure_alcohol_volume) / SUM(alcohol.price) AS avg_alcohol_price;

STORE result INTO '$output_file' USING PigStorage(',');


