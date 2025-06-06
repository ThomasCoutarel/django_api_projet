# urls.py
from django.urls import path
from . import views

urlpatterns = [
    path('topics/', views.list_topics_view),
    path('clean/', views.clean_data),
    path('datalake/', views.send_to_datalake),
    path('datawarehouse/', views.send_to_warehouse),
    path('api/messages/<str:topic_name>/', views.get_paginated_messages),
    path('api/projection/<str:topic_name>/', views.project_kafka_columns_view, name='kafka_project_column'),
    path("grant_access/", views.grant_access, name="grant_access"),
    path("revoke_access/", views.revoke_access, name="revoke_access"),
    path('spent_last_5_minutes/<str:topic_name>/', views.spent_last_5_minutes),
    path('total_spent_by_user/<str:topic_name>/', views.total_spent_by_user),
    path('top_products/<str:topic_name>/', views.top_products),
    # Routes version data lake
    path('api/datalake/messages/<str:topic_name>/', views.get_paginated_messages_from_datalake),
    path('datalake/projection/<str:topic_name>/', views.project_column_view, name='project_column'),
    path('datalake/spent_last_5_minutes/<str:topic_name>/', views.spent_last_5_minutes_from_datalake),
    path('datalake/total_spent_by_user/<str:topic_name>/', views.total_spent_by_user_from_datalake),
    path('datalake/top_products/<str:topic_name>/', views.top_products_from_datalake),
    path('datalake/version/<str:topic_name>/', views.get_versioned_data_view, name='get_versioned_data'),

]
