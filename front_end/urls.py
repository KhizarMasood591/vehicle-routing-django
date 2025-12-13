from . import views
from django.urls import path, include



urlpatterns = [
    path('dashboard/', views.dashboard, name="dashboard"),
    path('signin/', views.sign_in, name="signin"),
    path('upload/', views.upload, name="upload"),
    path('model/', views.model, name="model")
]