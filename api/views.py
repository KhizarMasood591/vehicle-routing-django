from django.shortcuts import render
from rest_framework.views import APIView
from api.serializers import ScheduleSerializers, RouteSerializers
from api.models import Schedule, Route
from django.http import response, request
from rest_framework.response import Response
from api.routing.distance_matrix import DistanceMatrix
from api.pipelines.pipeline_route import PipelineRoute
from api.pipelines.pipeline_schedule import PipelineSchedule
from api.schemas.schema_route import SchemaRoute as sr
from api.routing.routing_model import Routing
import pandas as pd
from django.utils import timezone
from django.views.decorators.csrf import csrf_exempt
from api.measures import calulate_measures
from django.db.models import Max
from dataclasses import asdict
import base64
import datetime
# Create your views here.


def employees(request:request.HttpRequest, format='json'):
        db = Schedule.objects.all()
        schedule = ScheduleSerializers(db, many=True)
        return response.JsonResponse(schedule.data, safe=False)

@csrf_exempt
def upload_schedule(request: request.HttpRequest):
        if request.method =='POST':
                file = request.FILES['file'].read()
                upload_time = timezone.localtime()
                pipeline = PipelineSchedule(file, upload_time)
                df_schedule, df_store = pipeline.extract()
                df_transformed = pipeline.transform(df_schedule, df_store)
                pipeline.load(df_transformed, Schedule)
                data = {
                        "Time":upload_time,    
                        "message": "Data Uploaded",
                        "status" : 200
                }
                return response.JsonResponse(data)

@csrf_exempt
def run_model(request: request.HttpRequest):
        city = request.GET['city']
        max_capacity = int(request.GET['capacity'])
        ride_time = int(request.GET['ridetime'])
        pipeline = PipelineRoute(city)
        pipeline.extract_data()
        df_schedule = pipeline.df_extracted
        df_extracted_rows = df_schedule.shape[0]
        df_full_route = pd.DataFrame()
        locations = pipeline.locations
        distance_matrix = DistanceMatrix(locations, df_extracted_rows)
        distance_matrix.generate_matrix(60000)
        df_schedule_cleaned = pipeline.remove_outliers(distance_matrix.outliers)
        shift_times = sorted(df_schedule_cleaned[sr.SHIFT_TIME].unique())
        vehicle = 0
        # print(f"Generating Route for {shift}")
        # df_schedule_shift = df_schedule_cleaned[df_schedule_cleaned[sr.SHIFT_TIME]==shift]
        # if df_schedule_shift.empty:
        #         continue
        df_clusters = pipeline.create_clusters(df_schedule_cleaned)
        clusters = df_clusters['Clusters'].unique()
        for cluster in clusters:
                print(f"Generating Route for {cluster}")
                df_schedule_cluster = df_clusters[df_clusters['Clusters']==cluster]
                routing_model = Routing(
                        df_schedule_cluster,
                        df_schedule_cleaned,
                        distance_matrix.matrix,
                        max_capacity,
                        vehicle,
                        ride_time
                        )
                routing_model.run_model()
                vehicle = routing_model.vehicle_no
                df_full_route = pd.concat([df_full_route, routing_model.route])
        pipeline.transform_data(df_full_route,distance_matrix.matrix)
        pipeline.load_data()
        pipeline.df_transformed.to_excel("route.xlsx")
        data = {
                "message": "Route has been sucessfully generated",
                "status": 200
        }
        return response.JsonResponse(data)
        # for shift in shift_times:
        #         print(f"Generating Route for {shift}")
        #         df_schedule_shift = df_schedule_cleaned[df_schedule_cleaned[sr.SHIFT_TIME]==shift]
        #         if df_schedule_shift.empty:
        #                 continue
        #         df_clusters = pipeline.create_clusters(df_schedule_shift)
        #         clusters = df_clusters['Clusters'].unique()
        #         # for cluster in clusters:
        #         # df_schedule_cluster = df_clusters[df_clusters['Clusters']==cluster]
        #         routing_model = Routing(
        #                 df_schedule_shift,
        #                 df_schedule_cleaned,
        #                 distance_matrix.matrix,
        #                 max_capacity,
        #                 vehicle,
        #                 ride_time
        #                 )
        #         routing_model.shift_time = shift
        #         routing_model.run_model()
        #         vehicle = routing_model.vehicle_no
        #         df_full_route = pd.concat([df_full_route, routing_model.route])
        # pipeline.transform_data(df_full_route,distance_matrix.matrix)
        # pipeline.load_data()
        # pipeline.df_transformed.to_excel("route.xlsx")
        # data = {
        #         "message": "Route has been sucessfully generated",
        #         "status": 200
        # }
        # return response.JsonResponse(data)


def metrics(request: request.HttpRequest):
        max_date = Route.objects.aggregate(max_date=Max('upload_time'))
        query = Route.objects.filter(upload_time=max_date['max_date']).all()
        df_route = pd.DataFrame(query.values())
        df_route = df_route.sort_values(by=[sr.VEHICLE_ID, sr.ARRIVAL_TIME])
        data = calulate_measures(df_route)
        data.last_updated=data.last_updated.strftime("%Y-%m-%d %H:%M")
        return response.JsonResponse(asdict(data))


def route(request: request.HttpRequest):
        query = Route.objects.all()
        route = RouteSerializers(query, many=True)
        return response.JsonResponse(route.data, safe=False)

        



