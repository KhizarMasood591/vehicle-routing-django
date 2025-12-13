import pandas as pd
import sqlite3
from api.schemas.schema_route import SchemaRoute as sr
from api.schemas.schema_schedule import SchemaSchedule as ss
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from collections import defaultdict
from api.models import Route
import warnings
from sklearn import metrics
import itertools
warnings.filterwarnings('ignore')
from django.utils import timezone


class PipelineRoute:
    PICKUP_LAT = 'Pickup Lat'
    PICKUP_LON = 'Pickup Lon'
    DROP_LAT = 'Drop Lat'
    DROP_LON = 'Drop Lon'
    VEHICLE_NO = 'vehicle_no'
    TRIP_TYPE = 'trip_type'
    ACTION = 'action'
    IN_BUS = 'in_bus'
    ARRIVAL_TIME = 'arrival_time'
    DISTANCE = 'distance'
    LATITUDE = 'latitude'
    LONGITUDE = 'longitude'
    UPLOAD_TIME = 'upload_time'


    def __init__(self, city: str) -> None:
        self.df_extracted = None
        self.df_transformed = None
        self.city = city
        self.locations = None
        self.vehicle_time = defaultdict(list)
        self.merged_route = defaultdict(lambda: None)
        self._conn = sqlite3.connect("db.sqlite3")


    def remove_outliers(self, outliers_idx):
        df_rows = self.df_extracted.shape[0]
        pickup_outliers = outliers_idx[outliers_idx < df_rows]
        dropoff_outliers = outliers_idx[outliers_idx >= df_rows]
        drop_rows = np.unique(np.concatenate([pickup_outliers, dropoff_outliers - df_rows]))
        cleaned_df = self.df_extracted.drop(drop_rows).reset_index(drop=True)
        pickups = cleaned_df[[PipelineRoute.PICKUP_LAT, PipelineRoute.PICKUP_LON]].to_numpy().tolist()
        drop = cleaned_df[[PipelineRoute.DROP_LAT, PipelineRoute.DROP_LON]].to_numpy().tolist()
        self.locations = pickups + drop
        return cleaned_df

    def extract_data(self) -> None:
        df = pd.read_sql(
            "select * from api_schedule where upload_time = (select max(upload_time) from api_schedule)", 
            self._conn)
        df_filter_city = df[df[ss.CITY]==self.city]
        from_store = df_filter_city[df_filter_city[ss.TRIP_TYPE]=='From Store']
        group_from_store = from_store.groupby(
            [ss.SHIFT_TIME, ss.ACC_LAT, ss.ACC_LON, ss.STORE_LAT, ss.STORE_LON]
        )[ss.STAFF].sum().to_frame().reset_index()
        to_store = df_filter_city[df_filter_city[ss.TRIP_TYPE]=='To Store']
        group_to_store = to_store.groupby(
            [ss.SHIFT_TIME, ss.ACC_LAT, ss.ACC_LON, ss.STORE_LAT, ss.STORE_LON]
        )[ss.STAFF].sum().to_frame().reset_index()
        group_from_store = group_from_store.rename(
            columns={
                ss.STORE_LAT: PipelineRoute.PICKUP_LAT,
                ss.STORE_LON: PipelineRoute.PICKUP_LON,
                ss.ACC_LAT:PipelineRoute.DROP_LAT,
                ss.ACC_LON:PipelineRoute.DROP_LON,
            }
        )
        group_from_store[PipelineRoute.TRIP_TYPE] = 'From Store'
        group_to_store = group_to_store.rename(
            columns={
                ss.STORE_LAT: PipelineRoute.DROP_LAT,
                ss.STORE_LON: PipelineRoute.DROP_LON,
                ss.ACC_LAT:PipelineRoute.PICKUP_LAT,
                ss.ACC_LON:PipelineRoute.PICKUP_LON,
            }
        )
        group_to_store[PipelineRoute.TRIP_TYPE] = 'To Store'
        all_trips = pd.concat([group_from_store, group_to_store], ignore_index=True)
        self.df_extracted = all_trips
        self.df_extracted[ss.SHIFT_TIME] = pd.to_datetime(self.df_extracted[ss.SHIFT_TIME],format='%H:%M:%S').dt.time
        pickups = self.df_extracted[[PipelineRoute.PICKUP_LAT, PipelineRoute.PICKUP_LON]].to_numpy().tolist()
        drop = self.df_extracted[[PipelineRoute.DROP_LAT, PipelineRoute.DROP_LON]].to_numpy().tolist()
        self.locations = pickups + drop
        print(len(self.locations))


    def transform_data(
            self,
            df_route: pd.DataFrame,
            matrix: dict) -> pd.DataFrame:
        shifts = sorted(df_route[sr.SHIFT_TIME])
        for shift in shifts:
            shift_df = df_route[df_route[ss.SHIFT_TIME]==shift]
            for bus_id in shift_df[sr.VEHICLE_ID].unique():
                bus_df = shift_df[shift_df[sr.VEHICLE_ID]==bus_id]
                if not bus_df.empty:
                    last_node = bus_df.iloc[-1][sr.FROM_NODE]
                    first_node = bus_df.iloc[0][sr.FROM_NODE]
                    last_time = bus_df.iloc[-1][sr.ARRIVAL_TIME]
                    first_time = bus_df.iloc[0][sr.ARRIVAL_TIME]
                    self.vehicle_time[shift].append(
                        {
                            bus_id:{
                                'Start_node': first_node,
                                'End_node' : last_node,
                                'Start_time' : first_time,
                                'End_time': last_time
                            }
                        }
                    )
        i = 0
        first_shift = shifts[i]
        vehicle_time_copy = self.vehicle_time.copy()
        vehicle_used = vehicle_time_copy[first_shift]
        node_replacement = {}
        while i < len(shifts) - 1:
            shift_b = shifts[i+1]
            vehicle_shift_b = vehicle_time_copy[shift_b]
            for vehicle in vehicle_used:
                for id, arrival in vehicle.items():
                    for idx,vehicle_b in enumerate(vehicle_shift_b):
                        for v_id, arrv in vehicle_b.items():
                            if arrival['End_time'] + matrix['time'][arrival['End_node']][arrv['Start_node']] <= arrv['Start_time']:
                                node_replacement[v_id] = arrv['Start_node']
                                vehicle[id]['End_node'] = arrv['End_node']  
                                vehicle[id]['End_time'] = arrv['End_time']
                                self.merged_route[v_id] = id
                                vehicle_shift_b.pop(idx)
            vehicle_used = vehicle_used + vehicle_shift_b
            vehicle_shift_b.clear()
            i += 1
        df_route[sr.TO_NODE] = df_route.apply(
            lambda x : node_replacement[x[sr.VEHICLE_ID]] if (self.merged_route.get(x[sr.VEHICLE_ID]) == x[sr.VEHICLE_ID]) and (x[sr.FROM_NODE]==x[sr.TO_NODE]) else x[sr.TO_NODE],
            axis=1  
        ) 
        df_route[sr.VEHICLE_ID] = df_route[sr.VEHICLE_ID].apply(
            lambda x: x if self.merged_route.get(x) == None else self.merged_route.get(x)
        )      
        df_route = df_route.sort_values(by=[sr.VEHICLE_ID, sr.ARRIVAL_TIME])
        # df_route[sr.VEHICLE_ID] = (df_route[sr.VEHICLE_ID] != df_route[sr.VEHICLE_ID].shift(1)).cumsum()
        self.df_transformed = df_route
        return df_route
        
    
    def create_clusters(self, df: pd.DataFrame):
        scores = []
        clusters = []
        df_features = df[[
            PipelineRoute.PICKUP_LAT, PipelineRoute.PICKUP_LON, 
            PipelineRoute.DROP_LAT, PipelineRoute.DROP_LON
        ]].to_numpy()
        standardscaler = StandardScaler()
        x_scaled = standardscaler.fit_transform(df_features)
        print(df.shape[0])
        for cluster in range(2,6):
            if df.shape[0] <= cluster:
                continue
            kmeans = KMeans(cluster, random_state=42).fit(x_scaled)
            labels = kmeans.labels_
            score = metrics.silhouette_score(x_scaled, labels)
            scores.append(score)
            clusters.append(cluster)
        if  scores:
            best_index = np.argmax(scores)
            best_cluster = clusters[best_index]
            df['Clusters'] = KMeans(best_cluster, random_state=42).fit_predict(x_scaled)
        else:
            df['Clusters'] = 0
        return df
    

    def load_data(self):
        uploaded_time=timezone.now()
        for index, row in self.df_transformed.iterrows():
            route = Route.objects.create(
                    upload_time=uploaded_time,
                    employee_id = index,
                    vehicle_id = row[sr.VEHICLE_ID],
                    trip_type = row[sr.TRIP_TYPE],
                    action = row[sr.ACTION],
                    in_bus = row[sr.IN_BUS],
                    arrival_time = row[sr.ARRIVAL_TIME],
                    distance = row[sr.DISTANCE],
                    lattitude = row[sr.LAT],
                    longitude = row[sr.LON],
                    shift_time = row[sr.SHIFT_TIME],
                    pickup_drop_count = row[sr.PICKUP_COUNT],
                    start = row[sr.FROM_NODE],
                    end = row[sr.TO_NODE]
                    
                )
            route.save()
