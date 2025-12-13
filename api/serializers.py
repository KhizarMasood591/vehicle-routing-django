from rest_framework import serializers
from api.models import Schedule


class ScheduleSerializers(serializers.Serializer):
    trip_type = serializers.CharField()
    transportation_type = serializers.CharField(max_length=100)
    region = serializers.CharField(max_length=50)
    city = serializers.CharField(max_length=100)
    acc_code = serializers.IntegerField()
    acc_name = serializers.CharField(max_length=250)
    store_code = serializers.IntegerField()
    store_name = serializers.CharField(max_length=100)
    brand = serializers.CharField(max_length=50)
    shift_time = serializers.TimeField()
    staff = serializers.IntegerField()
    acc_lattitude = serializers.FloatField()
    acc_longitude = serializers.FloatField()
    store_latitude = serializers.FloatField()
    store_longitude = serializers.FloatField()


class RouteSerializers(serializers.Serializer):
    upload_time = serializers.DateTimeField()
    employee_id =serializers.CharField(max_length=50)
    vehicle_id = serializers.IntegerField()
    trip_type = serializers.CharField(max_length=50)
    action = serializers.CharField(max_length=10)
    in_bus = serializers.IntegerField()
    arrival_time = serializers.FloatField()
    distance = serializers.FloatField()
    lattitude = serializers.FloatField()                  
    longitude = serializers.FloatField()
    shift_time = serializers.TimeField()
    pickup_drop_count = serializers.IntegerField()