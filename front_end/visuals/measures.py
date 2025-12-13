import pandas as pd
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List
from api.schemas.schema_route import SchemaRoute as sr

@dataclass
class Measures:
    last_updated: datetime
    total_buses: int
    total_employees: int
    total_distance: float
    bus_utilization: float
    total_round_trips: int
    total_pickup_locations: int
    total_dropoff_locations: int


@dataclass
class Graphs:
    pickup_types_date: Dict[str, List]
    distance_travelled_bus: Dict[str, List]
    employess_on_bus = Dict[str, List]



def calculate_measures(df: pd.DataFrame) -> dataclass:
    df[sr.UPLOAD_TIME] = pd.to_datetime(df[sr.UPLOAD_TIME])
    last_updated = df[sr.UPLOAD_TIME].max()
    df_latest = df[df[sr.UPLOAD_TIME] == last_updated]
    total_buses = df_latest[sr.VEHICLE_ID].nunique()
    total_employees = df_latest[sr.EMPLOYEE_ID].nunique()
    total_distance = "test"

    


        
        


