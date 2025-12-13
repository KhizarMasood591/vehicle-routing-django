from django.db import models

# Create your models here.
class Schedule(models.Model):
    upload_time = models.DateTimeField(blank=False)
    trip_type = models.CharField(max_length=50, blank=False)
    transportation_type = models.CharField(max_length=100, blank=False)
    region = models.CharField(max_length=50, blank=False)
    city = models.CharField(max_length=100, blank=False)
    acc_code = models.IntegerField(blank=False)
    acc_name = models.CharField(max_length=250, blank=False)
    store_code = models.IntegerField(blank=False)
    store_name = models.CharField(max_length=100, blank=False)
    brand = models.CharField(max_length=50, blank=False)
    shift_time = models.TimeField(blank=False)
    staff = models.IntegerField(blank=False)
    acc_lattitude = models.FloatField(blank=False)
    acc_longitude = models.FloatField(blank=False)
    store_latitude = models.FloatField(blank=False)
    store_longitude = models.FloatField(blank=False)


class Route(models.Model):
    upload_time = models.DateTimeField(blank=False)
    employee_id = models.CharField(max_length=50, blank=False)
    vehicle_id = models.IntegerField(blank=False)
    trip_type = models.CharField(max_length=50, blank=False)
    action = models.CharField(max_length=10, blank=False)
    in_bus = models.IntegerField(blank=False)
    arrival_time = models.FloatField(blank=False)
    distance = models.FloatField(blank=False)
    lattitude = models.FloatField(blank=False)                  
    longitude = models.FloatField(blank=False)
    shift_time = models.TimeField(blank=False)
    start = models.CharField(max_length=250)
    end = models.CharField(max_length=250)
    pickup_drop_count = models.IntegerField(blank=False)