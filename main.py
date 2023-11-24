import duckdb
import os

def create_table(con):
    con.sql("""CREATE TABLE electric_cars (vin VARCHAR, country VARCHAR, city VARCHAR, state VARCHAR, postal_code INTEGER, 
                        model_year INTEGER, make VARCHAR, model VARCHAR, elecrtic_vehicle_type VARCHAR, clean_alternative_fuel_vehicle_eligibility VARCHAR,
                        electric_range INTEGER, base_msrp INTEGER, legislative_district INTEGER, dol_vehicle_id INTEGER, vehicle_location VARCHAR,
                        eletric_utility VARCHAR, census_2020_tract VARCHAR)""")


def import_data_into_table(con):
    con.sql("INSERT INTO electric_cars SELECT * FROM '/app/data/Electric_Vehicle_Population_Data.csv'")


def count_electric_vehicle_by_city_and_save(con):
    con.sql("COPY (SELECT city, COUNT(*) as 'count' FROM electric_cars GROUP BY city ORDER BY count DESC) TO '/app/data/results/task_1.parquet' (FORMAT PARQUET)")


def get_top_3_electric_vehicle_and_save(con):
    con.sql("COPY (SELECT make, model, COUNT(*) as 'count' FROM electric_cars GROUP BY make, model ORDER BY count DESC LIMIT 3) TO '/app/data/results/task_2.parquet' (FORMAT PARQUET)")


def get_most_popular_electric_vehicle_for_every_postal_code_and_save(con):
    con.sql("COPY (SELECT make, model, postal_code, count FROM (SELECT make, model, postal_code, COUNT(*) as 'count', ROW_NUMBER() OVER (PARTITION BY postal_code ORDER BY count desc) as row_num FROM electric_cars GROUP BY make, model, postal_code ORDER BY count DESC) WHERE row_num = 1 ORDER BY count DESC) TO '/app/data/results/task_3.parquet' (FORMAT PARQUET)")


def count_electric_vehicle_by_year_of_production_and_save(con):
    con.sql("COPY (SELECT model_year, COUNT(*) as 'count' FROM electric_cars GROUP BY model_year ORDER BY count DESC) TO '/app/data/results/task_4' (FORMAT PARQUET, PARTITION_BY (model_year))")


def main():

    os.mkdir("/app/data/results")

    con = duckdb.connect()

    create_table(con)

    import_data_into_table(con)

    count_electric_vehicle_by_city_and_save(con)

    get_top_3_electric_vehicle_and_save(con)

    get_most_popular_electric_vehicle_for_every_postal_code_and_save(con)

    count_electric_vehicle_by_year_of_production_and_save(con)

    con.close()

    pass


if __name__ == "__main__":
    main()
