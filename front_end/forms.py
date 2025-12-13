from django import forms




class Upload(forms.Form):
    file = forms.FileField(label="File", required=True)


class Model(forms.Form):
    max_capacity_per_bus = forms.IntegerField(label="Max Capacity Per Bus",required=True, initial=60)
    max_ride_time = forms.IntegerField(label="Max Ride Time - mins", required=True, initial=60)
    max_wait_time = forms.IntegerField(required=True, initial=10)
    city = forms.CharField(required=True, initial="Jeddah")
