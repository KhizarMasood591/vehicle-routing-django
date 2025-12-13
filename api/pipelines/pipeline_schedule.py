import pandas as pd
from datetime import datetime
import numpy as np
from django.db import models


class PipelineSchedule:
    CC = "CC"
    ACC_CC = 'Males Accomodation C.C'
    TRIP_TYPE = "Trip Type"
    TRANSPORTATION_TYPE = "Transportation type"
    REGION = "Region"
    CITY = "City"
    ACC_CODE = "Accomodation Code"
    ACC_NAME = "Accomodation Name"
    STORE_CODE = "Store Code"
    STORE_NAME = "Store Name"
    BRAND = "Brand"
    SHIFT_TIME = "Shift Time"
    STAFF = "Staff"
    ACC_LAT = "Acc latitude"
    ACC_LON = "Acc longitude"
    STORE_LAT = "Store Latitude"
    STORE_LON = "Store longitude"

    def __init__(self, filename: bytes, time: datetime):
        self.file = filename
        self.upload_time = time

    def extract(self) -> pd.DataFrame:
        df_schedule = pd.read_excel(self.file, sheet_name="Shift schedule Males")
        df_stores = pd.read_excel(self.file, sheet_name='Stores data')
        return df_schedule, df_stores
    

    def transform(self, *dataframes) -> pd.DataFrame:
        df_schedule = dataframes[0]
        df_stores = dataframes[1]
        df_merged = pd.merge(
            left=df_stores,
            right=df_schedule,
            how = 'left',
            left_on=[PipelineSchedule.CC, PipelineSchedule.ACC_CC],
            right_on=[PipelineSchedule.STORE_CODE, PipelineSchedule.ACC_CODE],
        )
        df_merged[PipelineSchedule.CITY] = df_merged[PipelineSchedule.CITY].str.title()
        df_city = df_merged[
            df_merged[PipelineSchedule.TRANSPORTATION_TYPE]=="Need transportation"
        ]
        df_city = df_city.drop(columns=['# of trips', 'Total Staff'])
        df_unpivot = pd.melt(
            df_city,
            df_city.columns[:20],
            df_city.columns[20:],
            PipelineSchedule.SHIFT_TIME,
            PipelineSchedule.STAFF
        )
        df_unpivot = df_unpivot[df_unpivot[PipelineSchedule.STAFF].notnull()]
        df_select_columns= df_unpivot[[
            PipelineSchedule.TRIP_TYPE,
            PipelineSchedule.TRANSPORTATION_TYPE,
            PipelineSchedule.REGION + "_x",
            PipelineSchedule.CITY,
            PipelineSchedule.ACC_CODE,
            PipelineSchedule.ACC_NAME,
            PipelineSchedule.STORE_CODE,
            PipelineSchedule.STORE_NAME,
            PipelineSchedule.BRAND,
            PipelineSchedule.SHIFT_TIME,
            PipelineSchedule.STAFF,
            PipelineSchedule.ACC_LAT,
            PipelineSchedule.ACC_LON,
            PipelineSchedule.STORE_LAT,
            PipelineSchedule.STORE_LON
        ]]
        df_select_columns = df_select_columns[df_select_columns.notnull().all(axis=1)]
        return df_select_columns
    

    def load(self, df: pd.DataFrame, db: models.Model) -> None:
        df.to_csv("df_schedule.csv")
        for index, row in df.iterrows():
            schedule = db.objects.create(
                upload_time = self.upload_time,
                trip_type = row[PipelineSchedule.TRIP_TYPE],
                transportation_type = row[PipelineSchedule.TRANSPORTATION_TYPE],
                region = row[PipelineSchedule.REGION + "_x"],
                city = row[PipelineSchedule.CITY],
                acc_code = row[PipelineSchedule.ACC_CODE],
                acc_name = row[PipelineSchedule.ACC_NAME],
                store_code = row[PipelineSchedule.STORE_CODE],
                store_name = row[PipelineSchedule.STORE_NAME],
                brand = row[PipelineSchedule.BRAND],
                shift_time = row[PipelineSchedule.SHIFT_TIME],
                staff = row[PipelineSchedule.STAFF],
                acc_lattitude = row[PipelineSchedule.ACC_LAT],
                acc_longitude = row[PipelineSchedule.ACC_LON],
                store_latitude = row[PipelineSchedule.STORE_LAT],
                store_longitude = row[PipelineSchedule.STORE_LON],
            )
        schedule.save()