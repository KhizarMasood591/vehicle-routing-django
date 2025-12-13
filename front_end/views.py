from django.shortcuts import render, redirect
from django.template import loader
from django.http import request, response, FileResponse
from django.views.decorators.csrf import csrf_exempt
from front_end.forms import Upload, Model
import pandas as pd
import requests
from django.urls import reverse
import base64
# Create your views here.
def dashboard(request: request.HttpRequest):
    template = loader.get_template("front_end/index.html")
    metrics_path = reverse("metrics")
    host = request.get_host()
    metrics_url = f"http://{host}{metrics_path}"
    metrics_resp = requests.get(metrics_url)
    if metrics_resp:
        metrics = metrics_resp.json()
        return response.HttpResponse(template.render(context=metrics))
    return response.HttpResponse(template.render())


def sign_in(request: request.HttpRequest):
    template = loader.get_template("front_end/sign_in.html")
    return response.HttpResponse(template.render())


def upload(request: request.HttpRequest):
    upload = Upload()
    model_form = Model()
    if request.method == "POST":
        path = reverse("schedule")
        host = request.get_host()
        schedule_url = f"http://{host}{path}"
        file = request.FILES['file']
        request_file = {
            "file": file
        }
        response = requests.post(
            schedule_url,
            files=request_file,
            verify=False
        )
        json_resp = response.json()
        return render(request=request, 
                      template_name="front_end/upload.html", 
                      context={
                          "form":upload, 
                          "upload_time":json_resp['Time'],
                          "message": json_resp['message'],
                          "model":model_form
                          })
    return render(request=request, 
                  template_name="front_end/upload.html" , 
                  context={
                      "form":upload,
                      "model":model_form
                      })


def model(request: request.HttpRequest):
    if request.method == "POST":
        model_params = Model(request.POST)
        max_capacity = model_params.data['max_capacity_per_bus']
        max_ride_time = model_params.data['max_ride_time']
        city = model_params.data['city']
        path = reverse("run_model")
        host = request.get_host()
        model_url = f"http://{host}{path}"
        params = {
            'capacity':max_capacity,
            'ridetime':max_ride_time,
            'city' : city
        }
        resp = requests.get(model_url, params=params)
        response = FileResponse(open('route.xlsx', 'rb'))
        response['Content-Disposition'] = f'atachement ; filename="route.xlsx"'
        return response
       
        
        
