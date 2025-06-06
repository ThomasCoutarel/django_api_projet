from django.urls import path
from . import views

urlpatterns = [
    path("test_json_view", views.test_json_view, name="test_json_view"),
    path("test_json_post", views.test_json_post, name="test_json_post"),
    path('get_available_products', views.get_available_products, name='get_available_products'),
    path('get_most_expensive_product', views.get_most_expensive_product, name='get_most_expensive_product'),
    path('add_product', views.add_product, name='add_product'),
    path('update_product', views.update_product, name='update_product'),
    path('grant_right', views.grant_right, name='grant_right'),
    path('revoke_right', views.revoke_right, name='revoke_right'),

]
