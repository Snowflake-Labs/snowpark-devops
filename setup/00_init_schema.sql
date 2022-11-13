-- First create database using the Knoema Economical Data Atlas
-- Go to Marketplace to get database

CREATE VIEW IF NOT EXISTS BEANIPA ("Table", Table_Name, Table_Description, Table_Full_Name, Table_Unit, Indicator, Indicator_Name, Indicator_Description, Indicator_Full_Name, Units, Scale, Frequency, Date, Value) 
as
SELECT * FROM ECONOMY_DATA_ATLAS.ECONOMY.BEANIPA;

create or replace procedure my_procedure()
    returns string
    language python
    runtime_version = '3.8'
    PACKAGES = ('snowflake-snowpark-python', 'scikit-learn', 'pandas')
    handler = 'procedure.process.run'
    imports = ('@deploy/app.zip');