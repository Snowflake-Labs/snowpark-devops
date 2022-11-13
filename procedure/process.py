#!/usr/bin/env python
# coding: utf-8

from snowflake.snowpark import Session, DataFrame
from snowflake.snowpark.types import PandasSeriesType, IntegerType, FloatType, StructType, StructField, DoubleType, PandasDataFrameType
from snowflake.snowpark.functions import col, year, max, lit
from sklearn.linear_model import LinearRegression
import pandas as pd

OUTPUTS = []

def run(session: Session) -> str:
    pce_df = session.table('BEANIPA')
    filtered_df = filter_personal_consumption_expenditures(pce_df)
    pce_pred = train_linear_regression_model(filtered_df.to_pandas())  # type: ignore
    register_udf(pce_pred, session)
    forecast_df = generate_new_table_with_predicted(filtered_df, pce_pred, session, 10)
    forecast_df.write.save_as_table('PCE_PREDICT', mode='overwrite')
    return str(OUTPUTS)

# get PCE data
def filter_personal_consumption_expenditures(input_df: DataFrame) -> DataFrame:
    df_pce = (input_df 
        .filter(col("Table_Name") == 'Price Indexes For Personal Consumption Expenditures By Major Type Of Product') 
        .filter(col('Indicator_Name') == 'Personal consumption expenditures (PCE)')
        .filter(col('Frequency') == 'A')
        .filter(col('Date') >= '1972-01-01'))
    df_pce_year = df_pce.select(year(col('Date')).alias('Year'), col('Value').alias('PCE') )
    return df_pce_year

def train_linear_regression_model(input_pd: pd.DataFrame) -> LinearRegression:
    x = input_pd["YEAR"].to_numpy().reshape(-1,1)
    y = input_pd["PCE"].to_numpy()

    model = LinearRegression().fit(x, y)

    # test model for 2023
    predictYear = 2023
    pce_pred = model.predict([[predictYear]])
    OUTPUTS.append(input_pd.tail())
    OUTPUTS.append('Prediction for '+str(predictYear)+': '+ str(round(pce_pred[0],2)))
    return model

def register_udf(model, session):
    def predict_pce(ps: pd.Series) -> pd.Series:
        return ps.transform(lambda x: model.predict([[x]])[0].round(2).astype(float))
    session.udf.register(predict_pce,
                        return_type=PandasSeriesType(FloatType()),
                        input_types=[PandasSeriesType(IntegerType())],
                        packages= ["pandas","scikit-learn"],
                        is_permanent=True, 
                        name="predict_pce_udf", 
                        replace=True,
                        stage_location="@deploy")
    OUTPUTS.append('UDF registered')

def generate_new_table_with_predicted(input_df: DataFrame, model: LinearRegression, session: Session, num_years: int) -> DataFrame:
    maxYear: int = input_df.agg(max(col('Year'))).collect()[0][0] # type: ignore
    df = []
    for x in range(1, num_years+1):
        df.append([maxYear+x, model.predict([[maxYear+x]])[0].round(3).astype(float), 1])
    predict_df = session.create_dataframe(df, schema=StructType([StructField('Year', IntegerType(), nullable=True), StructField('PCE', DoubleType(), nullable=True), StructField('Is_Predicted', IntegerType(), nullable=True)]))
    input_df = input_df.with_column('Is_Predicted', lit(0))
    return input_df.union(predict_df).sort(col('Year'))


if __name__ == "__main__":
    from utils import get_session
    session = get_session.session()
    run(session)
