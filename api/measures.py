from dataclasses import dataclass
from typing import Dict, List
from datetime import datetime
import pandas as pd
from api.schemas.schema_route import SchemaRoute as sr


@dataclass
class Measures:
    last_updated: datetime
    total_employees: str
    total_buses: str
    total_distance_travelled: str
    total_round_trips: str
    total_pickups: str
    total_dropoffs: str
    total_trips_bus: Dict[str,Dict[str,List]]
    bus_utlization: float
    total_distance_bus: Dict[str, List]
    max_employees_bus: Dict[str,List]
    capacity: Dict[str,Dict[str,int]]
    total_capacity: Dict[str,Dict[str,str]]


def calulate_measures(df: pd.DataFrame):
    last_updated = df[sr.UPLOAD_TIME].min()
    total_emp = int(df[df[sr.ACTION]=="Pickup"][sr.PICKUP_COUNT].sum())
    total_buses = df[sr.VEHICLE_ID].nunique()
    total_distance = df[sr.DISTANCE].sum()
    total_drop = df[df[sr.ACTION]=="Dropoff"].shape[0]
    total_pickup = df[df[sr.ACTION]=="Pickup"].shape[0]
    total_trips = {}
    total_pickup_trips = {}
    total_pickup_trips['x'] = df[df[sr.ACTION]=="Pickup"].groupby(by=sr.VEHICLE_ID)[sr.ACTION].count().index.to_list()
    total_pickup_trips['y'] = df[df[sr.ACTION]=="Pickup"].groupby(by=sr.VEHICLE_ID)[sr.ACTION].count().to_list()
    total_trips['total_pickup_trips'] = total_pickup_trips
    total_drop_trips = {}
    total_drop_trips['x'] = df[df[sr.ACTION]=="Dropoff"].groupby(by=sr.VEHICLE_ID)[sr.ACTION].count().index.to_list()
    total_drop_trips['y'] = df[df[sr.ACTION]=="Dropoff"].groupby(by=sr.VEHICLE_ID)[sr.ACTION].count().to_list()
    total_trips['total_drop_trips'] = total_drop_trips
    bus_utlization = 0.6 * 100
    total_distance_bus = {}
    total_distance_bus['x'] = df.groupby(by=sr.VEHICLE_ID)[sr.DISTANCE].sum().index.to_list()
    total_distance_bus['y'] = df.groupby(by=sr.VEHICLE_ID)[sr.DISTANCE].sum().to_list()
    max_employees = {}
    max_employees['x'] = df.groupby(by=sr.VEHICLE_ID)['in_bus'].max().index.to_list()
    max_employees['y'] = df.groupby(by=sr.VEHICLE_ID)['in_bus'].max().to_list()
    total_capacity = {'Total':{}}
    capacity_total_df = df.groupby(by=sr.VEHICLE_ID)['in_bus'].max().to_frame().reset_index()
    capacity_total_df['capacity'] = capacity_total_df[sr.IN_BUS].apply(
        lambda x: '10' if x <= 10 else 
        '20' if x >10 and x<=20 else
        '30' if x > 20 and x<=30 else 'Above_30')
    capacity_grp = capacity_total_df.groupby(by='capacity').count()
    capacity_grp[sr.VEHICLE_ID] = capacity_grp[sr.VEHICLE_ID].astype('str')
    for index, row in capacity_grp.iterrows():
        total_capacity['Total'][index]=row[sr.VEHICLE_ID]
    df_capacity = df.groupby(by=[sr.VEHICLE_ID, sr.SHIFT_TIME])[sr.IN_BUS].max().to_frame().reset_index()
    df_bus = df_capacity.groupby(sr.VEHICLE_ID)[sr.IN_BUS].max().to_frame()
    df_bus['capacity'] = df_bus[sr.IN_BUS].apply(
            lambda x: '10' if x <= 10 else 
            '20' if x >10 and x<=20 else
            '30' if x > 20 and x<=30 else 'Above_30')
    df_merged = pd.merge(df_capacity, df_bus, how='left', left_on=sr.VEHICLE_ID, right_on=df_bus.index)
    shift_time = df_merged.groupby(by=[sr.SHIFT_TIME, 'capacity'])[sr.VEHICLE_ID].nunique().reset_index()
    capacity = {}
    for idx, row in shift_time.iterrows():
        shift = row[sr.SHIFT_TIME].strftime("%H:%M")
        if not shift in capacity.keys():
            capacity[shift] = {"10":0, "20":0, "30":0, "Above_30":0}
        capacity[shift][row['capacity']] = row[sr.VEHICLE_ID]
    return Measures(
        last_updated=last_updated,
        total_employees=total_emp,
        total_buses=total_buses,
        total_distance_travelled=total_distance,
        total_round_trips=100,
        total_dropoffs=total_drop,
        total_pickups=total_pickup,
        total_trips_bus=total_trips,
        bus_utlization=bus_utlization,
        total_distance_bus=total_distance_bus,
        max_employees_bus=max_employees,
        capacity=capacity,
        total_capacity=total_capacity
    )


    


