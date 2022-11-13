#!/usr/bin/env python
# coding: utf-8

from snowflake.snowpark import Session, DataFrame
from snowflake.snowpark.types import PandasSeriesType, PandasDataFrameType, IntegerType, FloatType
from snowflake.snowpark.functions import col, year
from sklearn.linear_model import LinearRegression
import pandas as pd

OUTPUTS = []

def run(session: Session) -> str:
    pce_df = session.table('BEANIPA')
    filtered_df = filter_personal_consumption_expenditures(pce_df)
    pce_pred = train_linear_regression_model(filtered_df.to_pandas())  # type: ignore
    register_udf(pce_pred, session)
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

if __name__ == "__main__":
    from utils import get_session
    session = get_session.session()
    run(session)
