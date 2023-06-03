crime = LOAD '$input_file' USING PigStorage(',') AS (year: int, city: chararray, population: int,
                violent_crime_total: int, murder_and_nonnegligent_manslaughter: int, rape: int, robbery: int,
                aggravated_assault: int, property_crime_total: int, burglary: int, larceny_theft: int,
                motor_vehicle_theft: int, arson: int);
alcohol = LOAD '$input_file2' USING PigStorage(',') AS (purchase_date: DateTime, city: chararray, price: float,
                    pure_alcohol_volume: float, store_name: chararray);

parsed_crime = FOREACH crime GENERATE year, UPPER(city) AS city, population, violent_crime_total,
                   murder_and_nonnegligent_manslaughter, rape, robbery, aggravated_assault, property_crime_total,
                   burglary, larceny_theft, motor_vehicle_theft, arson;
parsed_alcohol = FOREACH alcohol GENERATE GetYear(purchase_date) AS year, city, pure_alcohol_volume;

grouped_alcohol = GROUP parsed_alcohol BY (year, city);
sum_alcohol = FOREACH grouped_alcohol GENERATE group.year, group.city, SUM(parsed_alcohol.pure_alcohol_volume) AS total_pure_alcohol_volume;

joined_data = JOIN sum_alcohol BY (year, city), parsed_crime BY (year, city);
result = FOREACH joined_data GENERATE sum_alcohol::year AS year, sum_alcohol::city AS city,
        sum_alcohol::total_pure_alcohol_volume / parsed_crime::population AS avg_alcohol_consumption,
        parsed_crime::violent_crime_total / (parsed_crime::population / 100.0) AS violent_crime_total_rate,
        parsed_crime::murder_and_nonnegligent_manslaughter / (parsed_crime::population / 100.0) AS murder_and_nonnegligent_manslaughter_rate,
        parsed_crime::rape / (parsed_crime::population / 100.0) AS rape_rate,
        robbery / (parsed_crime::population / 100.0) AS robbery_rate,
        parsed_crime::aggravated_assault / (parsed_crime::population / 100.0) AS aggravated_assault_rate,
        parsed_crime::property_crime_total / (parsed_crime::population / 100.0) AS property_crime_total_rate,
        parsed_crime::burglary / (parsed_crime::population / 100.0) AS burglary_rate,
        parsed_crime::larceny_theft / (parsed_crime::population / 100.0) AS larceny_theft_rate,
        parsed_crime::motor_vehicle_theft / (parsed_crime::population / 100.0) AS motor_vehicle_theft_rate,
        parsed_crime::arson / (parsed_crime::population / 100.0) AS arson_rate;

STORE result INTO '$output_file' USING PigStorage(',');



