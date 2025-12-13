from django.urls import path, include
from . import views


urlpatterns = [
    path("employee/", views.employees,name="employee"),
    path('model/', views.run_model, name="run_model"),
    path('schedule/',views.upload_schedule, name="schedule"),
    path('route/', views.route, name="route"),
    path('metrics/', views.metrics, name="metrics")
]

