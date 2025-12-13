import pandas as pd
import numpy as np
import requests


class DistanceMatrix:
    def __init__(self, locations: list, rows) -> None:
        self.all_coordinates = locations
        self.coords = np.unique(locations,axis=0).tolist()
        self.total_locations = len(locations)
        self.unique_locations = np.unique(locations,axis=0).shape[0]
        self.url = "http://localhost:5050/table/v1/driving"
        self.outliers = None
        self.matrix = {}
        self.rows = rows


    def generate_matrix(self, threshold: int) -> None:
        final_dist_matrix = np.zeros((self.total_locations, self.total_locations))
        final_time_matrix = np.zeros((self.total_locations, self.total_locations))
        location_idx = [self.coords.index(i) for i in self.all_coordinates]
        formatted_cords = [f"{lon},{lat}" for lat, lon in self.coords]
        n = len(self.coords)
        full_matrix_dist = np.zeros((n,n))
        full_matrix_time = np.zeros((n,n))
        chunk_size = 100
        osrm_url = self.url
        for i in range(0, n, chunk_size):
            for j in range(0,n,chunk_size):
                sources = list(range(i,min(i+chunk_size, n)))
                destinations = list(range(j,min(j+chunk_size, n)))
                coords_param =";".join(formatted_cords)
                src_params = ";".join(map(str,sources))
                dst_params = ";".join(map(str,destinations)) 
                url = f"{osrm_url}/{coords_param}?sources={src_params}&destinations={dst_params}&annotations=distance,duration"
                print(f"Fetching: sources{i}-{i+chunk_size}, destinations {j}-{j+chunk_size}")
                r = requests.get(url)
                if r.status_code != 200:
                    print(f"❌ Error: {r.status_code} — {r.text}")
                    continue
                distances = r.json()['distances']
                durations = r.json()['durations']
                sub_matrix_dist = np.array(distances)
                sub_matrix_time = np.array(durations)
                full_matrix_dist[np.ix_(sources, destinations)] = sub_matrix_dist
                full_matrix_time[np.ix_(sources, destinations)] = sub_matrix_time
        for i in range(0, self.total_locations,n):
            sources_idx = location_idx[i:min(i+n, self.total_locations)]
            for j in range(0, self.total_locations,n):
                destinations_idx = location_idx[j:min(j+n, self.total_locations)]
                final_dist_matrix[i:min(i+n,self.total_locations),j:min(j+n, self.total_locations)] = full_matrix_dist[np.ix_(sources_idx, destinations_idx)]
                final_time_matrix[i:min(i+n,self.total_locations),j:min(j+n, self.total_locations)] = full_matrix_time[np.ix_(sources_idx, destinations_idx)]
        row_means = np.mean(final_dist_matrix, axis=1)
        outliers = np.where(row_means > threshold)[0]
        self.outliers = outliers
        pickup_outliers = outliers[outliers < self.rows]
        dropoff_outliers = outliers[outliers >= self.rows]
        outliers = np.unique(np.concatenate([pickup_outliers, dropoff_outliers - self.rows,dropoff_outliers,pickup_outliers+self.rows]))
        matrix_dist_clean = np.delete(final_dist_matrix, outliers, axis=0)
        matrix_dist_clean = np.delete(matrix_dist_clean, outliers, axis=1)
        matrix_time_clean = np.delete(final_time_matrix, outliers, axis=0)
        matrix_time_clean = np.delete(matrix_time_clean, outliers, axis=1)
        self.matrix['distance'] = matrix_dist_clean
        self.matrix['time'] = matrix_time_clean
    







    




