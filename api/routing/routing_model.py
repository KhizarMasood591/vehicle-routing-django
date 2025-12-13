import pandas as pd
import numpy as np
from api.schemas.schema_schedule import SchemaSchedule as ss
from api.schemas.schema_route import SchemaRoute as sr
from api.pipelines.pipeline_route import PipelineRoute as pr
from ortools.constraint_solver import pywrapcp, routing_enums_pb2
import warnings
warnings.filterwarnings('ignore')


class Routing:
    def __init__(
            self, 
            dataframe: pd.DataFrame,
            df_all, 
            matrix: dict, 
            capacity: int,
            vehicle_no: int ,
            max_ride_time: int) -> None:
        self.df = dataframe
        self.df_all = df_all
        self.number_of_vehicles = 100
        self.vehicle_no = vehicle_no
        self.route = pd.DataFrame()
        self.rows = []
        self.max_ride = max_ride_time * 60
        self.matrix_dist = matrix['distance']
        self.matrix_time = matrix['time']
        self.pickup_df = self.df[[ss.SHIFT_TIME, pr.PICKUP_LAT, pr.PICKUP_LON, ss.TRIP_TYPE, ss.STAFF]]
        self.drop_df = self.df[[ss.SHIFT_TIME, pr.DROP_LAT, pr.DROP_LON, ss.TRIP_TYPE, ss.STAFF]]
        self.pickup_all = self.df_all[[ss.SHIFT_TIME, pr.PICKUP_LAT, pr.PICKUP_LON, ss.TRIP_TYPE, ss.STAFF]]
        self.drop_all = self.df_all[[ss.SHIFT_TIME, pr.DROP_LAT, pr.DROP_LON, ss.TRIP_TYPE, ss.STAFF]]
        self.shift_time = None
        self.manager: pywrapcp.RoutingIndexManager = None
        self.routing: pywrapcp.RoutingModel = None
        self.capacity = capacity


    @property
    def trip_types(self):
        type_pickup = self.pickup_df[ss.TRIP_TYPE].to_list()
        type_dropoff = self.drop_df[ss.TRIP_TYPE].to_list()
        return type_pickup + type_dropoff
    

    @property
    def demand(self):
        pickup_demand = self.pickup_df[ss.STAFF].astype(int).to_list()
        dropoff_demand = self.drop_df[ss.STAFF].astype(int).to_list()
        drop_demand = [demand * -1 for demand in dropoff_demand]
        return pickup_demand + drop_demand + [0]
    

    @property
    def pickup_deliveries(self):
        pickup_loc = self.pickup_df[[pr.PICKUP_LAT, pr.PICKUP_LON]].to_numpy().tolist()
        return [
            (pickup_idx, pickup_idx + len(pickup_loc)) for pickup_idx, pickup_coords in enumerate(pickup_loc)
        ]

    
    @property
    def pickup_time_windows(self):
        pickup_time = [
            (time, time+900) for time in self.pickup_df[ss.SHIFT_TIME].apply(lambda x: x.hour * 60 *60).to_list()
        ]
        return pickup_time
    
    @property
    def drop_time_windows(self):
        drop_time = [
            (time - 900, time) for time in self.drop_df[ss.SHIFT_TIME].apply(lambda x: x.hour * 60 *60).to_list()
        ]
        return drop_time
    
    
    @property
    def indices(self):
        pickup_indices = self.df.index.to_list()
        drop_indices = [i + self.df_all.shape[0] for i in pickup_indices]
        return pickup_indices + drop_indices


    
    def data(self) -> dict:
        data = {}
        data['distance_matrix'] = self.get_matrix(self.matrix_dist).tolist()
        data['time_matrix'] = self.get_matrix(self.matrix_time).tolist()
        data["pickups_deliveries"] = self.pickup_deliveries
        data["num_vehicles"] = self.number_of_vehicles
        data['demands'] = self.demand
        data['vehicle_capacities'] =  [self.capacity] * self.number_of_vehicles
        data["depot"] = self.get_matrix(self.matrix_dist).shape[0] - 1
        return data    
    

    @property
    def locations(self):
        pickup = self.pickup_df[[pr.PICKUP_LAT, pr.PICKUP_LON]].to_numpy().tolist()
        drop = self.drop_df[[pr.DROP_LAT, pr.DROP_LON]].to_numpy().tolist()
        return pickup + drop
    
    @property
    def locations_all(self):
        pickup = self.pickup_all[[pr.PICKUP_LAT, pr.PICKUP_LON]].to_numpy().tolist()
        drop = self.drop_all[[pr.DROP_LAT, pr.DROP_LON]].to_numpy().tolist()
        return pickup + drop
    

    def get_matrix(self, matrix: np)->np:
        matrix_subset: np = matrix[np.ix_(self.indices,self.indices)]
        original_size = matrix_subset.shape[0]
        new_size = original_size + 1
        new_matrix = np.full((new_size, new_size),0)
        new_matrix[:original_size,:original_size] = matrix_subset
        row_avg = np.mean(matrix_subset, axis=1)
        col_avg = np.mean(matrix_subset, axis=0)
        for i in range(original_size):
            new_matrix[i, new_size -1] = row_avg[i]
            new_matrix[new_size-1, i] = col_avg[i]
        return new_matrix
            

    def run_model(self):
        data = self.data()
        manager = pywrapcp.RoutingIndexManager(
            len(data['time_matrix']),
            self.number_of_vehicles,
            data['depot']
        )
        routing = pywrapcp.RoutingModel(manager)
        def distance_callback(from_index, to_index):
            from_node = manager.IndexToNode(from_index)
            to_node = manager.IndexToNode(to_index)
            return data['distance_matrix'][from_node][to_node]
        distance_callback_index = routing.RegisterTransitCallback(distance_callback)
        routing.AddDimension(
            distance_callback_index,
            0,
            12000000,
            True,
            'Distance'
        )
        def time_callback(from_index, to_index):
            from_node = manager.IndexToNode(from_index)
            to_node = manager.IndexToNode(to_index)
            return data['time_matrix'][from_node][to_node]
        time_callback_index = routing.RegisterTransitCallback(time_callback)
        routing.SetArcCostEvaluatorOfAllVehicles(time_callback_index)
        routing.AddDimension(
            time_callback_index,
            10 * 60,
            24 * 60 * 60,
            False,
            'Time'
        )
        def demand_callback(from_index):
            node = manager.IndexToNode(from_index)
            return data['demands'][node]
        demand_callback_index = routing.RegisterUnaryTransitCallback(demand_callback)
        routing.AddDimensionWithVehicleCapacity(
            demand_callback_index,
            0,
            data['vehicle_capacities'],
            True,
            'Capacity'
        )
        capacity_dimension = routing.GetDimensionOrDie('Capacity')
        distance_dimension = routing.GetDimensionOrDie('Distance')
        time_dimension = routing.GetDimensionOrDie('Time')
        time_dimension.SetGlobalSpanCostCoefficient(10000)
        for pickup, dropoff in data['pickups_deliveries']:
            pickup_index = manager.NodeToIndex(pickup)
            dropoff_index = manager.NodeToIndex(dropoff)
            routing.AddPickupAndDelivery(pickup_index, dropoff_index)
            routing.solver().Add(
                routing.VehicleVar(pickup_index) == routing.VehicleVar(dropoff_index)
            )
            routing.solver().Add(
                distance_dimension.CumulVar(pickup_index) <= 
                distance_dimension.CumulVar(dropoff_index)
            )
            if self.pickup_df.iloc[pickup, 3] == 'To Store':
                time_dimension.CumulVar(dropoff_index).SetRange(
                    self.drop_time_windows[pickup][0] ,
                    self.drop_time_windows[pickup][1]
                )
            else:
                time_dimension.CumulVar(pickup_index).SetRange(
                    self.pickup_time_windows[pickup][0],
                    self.pickup_time_windows[pickup][1]
                )
            routing.solver().Add(
                time_dimension.CumulVar(dropoff_index) <=
                time_dimension.CumulVar(pickup_index) + self.max_ride
            )
        search_params = pywrapcp.DefaultRoutingSearchParameters()
        search_params.first_solution_strategy = (
            routing_enums_pb2.FirstSolutionStrategy.AUTOMATIC
        )
        search_params.local_search_metaheuristic = (
            routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
        )
        search_params.time_limit.FromSeconds(10)
        search_params.solution_limit = 1
        solution = routing.SolveWithParameters(search_params)
        if solution:
            self.get_route(data, manager, routing, solution, 
                           distance_dimension, capacity_dimension, time_dimension)
            self.route = pd.DataFrame(self.rows)
            self.route[sr.ACTION] = self.route[sr.FROM_NODE].apply(
                lambda x: 'Pickup' if x < len(self.pickup_df) else 'Dropoff'
            )
            self.route[sr.TRIP_TYPE] = self.route[sr.FROM_NODE].apply(
                lambda x: self.trip_types[x]
            )
            self.route[sr.LAT] = self.route[sr.FROM_NODE].apply(
                lambda x: self.locations_all[self.indices[x]][0]
            )
            self.route[sr.LON] = self.route[sr.FROM_NODE].apply(
                lambda x: self.locations_all[self.indices[x]][1]
            )
            self.route[sr.DISTANCE] = self.route.apply(
                lambda x: self.matrix_dist[self.indices[x[sr.FROM_NODE]]][self.indices[x[sr.TO_NODE]]]/1000,
                axis=1
            )
            self.route[sr.PICKUP_COUNT] = self.route[sr.FROM_NODE].apply(
                lambda x: self.demand[x]
            )
            self.route[sr.SHIFT_TIME] = self.shift_time
            self.route[sr.FROM_NODE] = self.route[sr.FROM_NODE].apply(
                lambda x: self.indices[x]
            )
            self.route[sr.TO_NODE] = self.route[sr.TO_NODE].apply(
                lambda x: self.indices[x]
            )
            print("Route Successfully Generated")
        else:
            print("No Solution Found!")


    def get_route(self, data, manager, routing, solution, 
                  distance_dimension, capacity_dimension, time_dimension) -> None:
        for vehicle_id in range(data['num_vehicles']):
            index = routing.Start(vehicle_id)
            route_load = 0
            if routing.IsEnd(solution.Value(routing.NextVar(index))):
                continue
            index = solution.Value(routing.NextVar(index))
            while not routing.IsEnd(index):
                distance = solution.Value(distance_dimension.CumulVar(index))
                node = manager.IndexToNode(index)
                route_load = solution.Value(capacity_dimension.CumulVar(index))
                time_var = time_dimension.CumulVar(index)
                arrival_time = solution.Value(time_var)
                previous_index = index
                index = solution.Value(routing.NextVar(index))
                next_node  = manager.IndexToNode(index)
                if routing.IsEnd(index):
                    next_node = previous_index
                row = {
                    sr.VEHICLE_ID: self.vehicle_no,
                    sr.FROM_NODE: node,
                    sr.TO_NODE:next_node,
                    sr.ARRIVAL_TIME: arrival_time,
                    sr.IN_BUS : route_load,
                    sr.RIDE_TIME: routing.GetArcCostForVehicle(
                        previous_index, index, vehicle_id
                    )
                }
                self.rows.append(row)
            self.vehicle_no += 1


    
        

    


