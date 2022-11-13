import pytest
from snowflake.snowpark import Session
from snowflake.snowpark.types import LongType, DateType, StringType, StructType, StructField, DoubleType, IntegerType
import pandas as pd
from utils import get_session
get_session.session()
from procedure import process

@pytest.fixture
def session() -> Session:
    return get_session.session()

def test_filter(session: Session):
    source_data = [
        ("T10109", "Implicit Price Deflators For Gross Domestic Product", None, "Table 1.1.9. Implicit Price Deflators For Gross Domestic Product (A) (Q)", "Index, 2012=100", "DPCERD-2", "Personal consumption expenditures", None, None, "Index, 2012=100", 1, "Q", "1976-01-01", 29.437),
        ("T20304", "Price Indexes For Personal Consumption Expenditures By Major Type Of Product", None, "Table 2.3.4. Price Indexes For Personal Consumption Expenditures By Major Type Of Product (A) (Q)", "Index, 2012=100", "DPCERG-1", "Personal consumption expenditures (PCE)", None, None, "Index, 2012=100", 1, "A", "2021-01-01", 115.53),
        ("T20304", "Price Indexes For Personal Consumption Expenditures By Major Type Of Product", None, "Table 2.3.4. Price Indexes For Personal Consumption Expenditures By Major Type Of Product (A) (Q)", "Index, 2012=100", "DPCERG-1", "Personal consumption expenditures (PCE)", None, None, "Index, 2012=100", 1, "A", "1929-01-01", 9.296)
    ]
    schema=StructType([StructField('Table', StringType(), nullable=True), StructField('Table_Name', StringType(), nullable=True), StructField('Table_Description', StringType(), nullable=True), StructField('Table_Full_Name', StringType(), nullable=True), StructField('Table_Unit', StringType(), nullable=True), StructField('Indicator', StringType(), nullable=True), StructField('Indicator_Name', StringType(), nullable=True), StructField('Indicator_Description', StringType(), nullable=True), StructField('Indicator_Full_Name', StringType(), nullable=True), StructField('Units', StringType(), nullable=True), StructField('Scale', LongType(), nullable=True), StructField('Frequency', StringType(), nullable=True), StructField('Date', DateType(), nullable=True), StructField('Value', DoubleType(), nullable=True)])
    source_df = session.create_dataframe(
        source_data,
        schema=schema
    )
    actual_df = process.filter_personal_consumption_expenditures(source_df)
    expected_data = [
        (2021, 115.53)     
    ]
    expected_df = session.create_dataframe(expected_data,
        schema=StructType([StructField('Year', IntegerType(), nullable=True), StructField('PCE', DoubleType(), nullable=True)])
    )
    assert (actual_df.collect() == expected_df.collect())

def test_linear_regression():
    source_data = [
        (2017, 106.051),
        (2018, 108.318),
        (2019, 109.922),
        (2020, 111.225)     
    ]
    source_pd = pd.DataFrame(source_data, columns=['YEAR', 'PCE'])
    actual_model = process.train_linear_regression_model(source_pd)
    assert (actual_model.predict([[2021]])[0] == pytest.approx(113.1605, 0.01))