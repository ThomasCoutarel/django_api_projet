from django.shortcuts import render

# Create your views here.
# views.py
from .datalake_reader import read_messages_from_datalake
from .kafka_consumer import consume_to_datalake, consume_to_warehouse, TOPICS
from .data_cleaner import clean_old_data_lake, clean_sqlite_data
from .kafka_consumer import preview_messages
from django.core.paginator import Paginator
from rest_framework.permissions import IsAuthenticated
from django.contrib.auth.models import User
from .models import AccessRight
from rest_framework.decorators import api_view, authentication_classes, permission_classes
from datetime import datetime, timedelta
from collections import defaultdict
from django.http import JsonResponse
from confluent_kafka.admin import AdminClient
import os
import pandas as pd
import numpy as np
from rest_framework.response import Response
from rest_framework import status


@api_view(['GET'])
def clean_data(request):
    clean_old_data_lake("C:/Users/couta/Python project/data/data_lake", TOPICS)
    clean_sqlite_data("C:/Users/couta/Python project/cp-all-in-one-7.5.0-post/tp3_dataintegration.db")
    return Response({"status": "Nettoyage terminé."})

@api_view(['POST'])
def send_to_datalake(request):
    topic = request.data.get('topic')
    if topic not in TOPICS:
        return Response({"error": "Topic invalide."}, status=400)
    consume_to_datalake(topic)
    return Response({"status": f"Messages de {topic} envoyés au data lake."})

@api_view(['POST'])
def send_to_warehouse(request):
    consume_to_warehouse("C:/Users/couta/Python project/cp-all-in-one-7.5.0-post/tp3_dataintegration.db")
    return Response({"status": "Messages envoyés vers le data warehouse."})


def apply_filters(messages, query_params):
    def check_condition(value, operator, reference):
        try:
            if operator == 'gt':
                return float(value) > float(reference)
            elif operator == 'lt':
                return float(value) < float(reference)
            elif operator == 'eq':
                return str(value).lower() == str(reference).lower()
            return False
        except:
            return False

    filtered = []
    for msg in messages:
        valid = True
        for key, val in query_params.items():
            if key == "page":
                continue

            if "__" in key:
                field, op = key.split("__", 1)
                if field not in msg or not check_condition(msg[field], op, val):
                    valid = False
                    break
            else:
                if key not in msg or str(msg[key]).lower() != str(val).lower():
                    valid = False
                    break
        if valid:
            filtered.append(msg)
    return filtered

@api_view(['GET'])
def get_paginated_messages(request, topic_name):
    if topic_name not in TOPICS:
        return Response({"error": "Topic invalide."}, status=400)

    try:
        page_number = int(request.GET.get("page", 1))
    except ValueError:
        return Response({"error": "Page doit être un entier."}, status=400)

    messages = preview_messages(topic_name)
    filtered_messages = apply_filters(messages, request.GET)

    paginator = Paginator(filtered_messages, 10)
    try:
        page = paginator.page(page_number)
    except:
        return Response({"error": "Page inexistante."}, status=400)

    return Response({
        "topic": topic_name,
        "page": page.number,
        "total_pages": paginator.num_pages,
        "messages": list(page.object_list)
    })

@api_view(['POST'])
@permission_classes([IsAuthenticated])
def grant_access(request):
    data = request.data
    try:
        user = User.objects.get(username=data['username'])
    except User.DoesNotExist:
        return Response({"error": "User not found"}, status=404)

    right, created = AccessRight.objects.get_or_create(
        user=user,
        resource=data['resource']
    )
    right.can_read = data.get('can_read', False)
    right.can_write = data.get('can_write', False)
    right.save()
    return Response({'message': 'Access granted'})

@api_view(['DELETE'])
@permission_classes([IsAuthenticated])
def revoke_access(request):
    data = request.data
    username = data.get('username')
    resource = data.get('resource')
    if not username or not resource:
        return Response({"error": "username and resource required"}, status=400)

    try:
        user = User.objects.get(username=username)
        right = AccessRight.objects.get(user=user, resource=resource)
    except (User.DoesNotExist, AccessRight.DoesNotExist):
        return Response({"error": "AccessRight not found"}, status=404)

    right.delete()
    return Response({"message": "Access revoked"})

@api_view(['GET'])
def project_kafka_columns_view(request, topic_name):
    selected_columns = list(request.query_params.keys())

    if not selected_columns:
        return Response(
            {"error": "Veuillez spécifier au moins une colonne à projeter en paramètres (ex: ?CITY)."},
            status=status.HTTP_400_BAD_REQUEST
        )

    messages = preview_messages(topic_name, 1000)

    result = []
    for msg in messages:
        try:
            row = {col: msg.get(col, None) for col in selected_columns}
            cleaned_row = {k: (None if isinstance(v, float) and (np.isnan(v) or np.isinf(v)) else v)
                           for k, v in row.items()}
            result.append(cleaned_row)
        except Exception as e:
            continue

    if not result:
        return Response(
            {"message": "Aucune donnée trouvée pour les colonnes demandées."},
            status=status.HTTP_204_NO_CONTENT
        )

    return Response(result, status=status.HTTP_200_OK)

@api_view(['GET'])
def spent_last_5_minutes(request, topic_name):
    messages = preview_messages(topic_name, 500)
    now = datetime.now()
    spent = 0.0

    for msg in messages:
        try:
            timestamp_str = msg.get("TIMESTAMP_OF_RECEPTION_LOG", "")
            timestamp = datetime.strptime(timestamp_str, "%d/%m/%Y %H:%M:%S")
            if (now - timestamp).total_seconds() <= 30000000000000000:
                amount = float(msg.get("AMOUNT_EUR", 0) or msg.get("AMOUNT", 0))
                quantity = int(msg.get("QUANTITY", 1))
                spent += amount * quantity
        except:
            continue

    return Response({"money_spent_last_5_minutes": round(spent, 2)})

@api_view(['GET'])
def total_spent_by_user(request, topic_name):
    messages = preview_messages(topic_name, 1000)
    result = defaultdict(lambda: defaultdict(float))

    for msg in messages:
        user = msg.get('ANO_USER_NAME') or msg.get('ANO_USER_ID') or msg.get('USER_ID')
        t_type = msg.get("TRANSACTION_TYPE", "unknown")
        try:
            amount = float(msg.get("AMOUNT_EUR", 0) or msg.get("AMOUNT", 0))
            quantity = int(msg.get("QUANTITY", 1))
            result[user][t_type] += amount * quantity
        except:
            continue

    formatted_result = [
        {
            "user": user,
            "transactions": [{"type": t_type, "total_spent": round(spent, 2)} for t_type, spent in types.items()]
        } for user, types in result.items()
    ]

    return Response(formatted_result)


@api_view(['GET'])
def top_products(request, topic_name):
    try:
        x = int(request.GET.get("x", 5))
    except:
        return Response({"error": "Le paramètre 'x' doit être un entier."}, status=400)

    messages = preview_messages(topic_name, 1000)
    product_counts = defaultdict(int)

    for msg in messages:
        product_id = msg.get("PRODUCT_ID")
        try:
            quantity = int(msg.get("QUANTITY", 1))
            if product_id:
                product_counts[product_id] += quantity
        except:
            continue

    top = sorted(product_counts.items(), key=lambda item: item[1], reverse=True)[:x]

    return Response([{"product_id": pid, "total_quantity": qty} for pid, qty in top])

def list_topics_view(request):
    admin = AdminClient({'bootstrap.servers': 'localhost:9092'})
    cluster_metadata = admin.list_topics(timeout=10)
    topics = [t for t in cluster_metadata.topics.keys() if not t.startswith('_')]  # ignore les topics internes
    return JsonResponse({'topics': topics})


#######################
#######################
#######################

DATA_LAKE_PATH = "C:/Users/couta/Python project/data/data_lake"

@api_view(['GET'])
def get_paginated_messages_from_datalake(request, topic_name):
    try:
        page_number = int(request.GET.get("page", 1))
    except ValueError:
        return Response({"error": "Page doit être un entier."}, status=400)

    messages = read_messages_from_datalake(topic_name)
    filtered_messages = apply_filters(messages, request.GET)

    paginator = Paginator(filtered_messages, 10)
    try:
        page = paginator.page(page_number)
    except:
        return Response({"error": "Page inexistante."}, status=400)

    return Response({
        "topic": topic_name,
        "source": "data_lake",
        "page": page.number,
        "total_pages": paginator.num_pages,
        "messages": list(page.object_list)
    })

@api_view(['GET'])
def project_column_view(request, topic_name):
    topic_path = os.path.join(DATA_LAKE_PATH, topic_name)

    if not os.path.exists(topic_path):
        return Response({"error": "Topic not found"}, status=status.HTTP_404_NOT_FOUND)

    selected_columns = list(request.query_params.keys())
    if not selected_columns:
        return Response({"error": "Aucune colonne demandée en projection."}, status=status.HTTP_400_BAD_REQUEST)

    result = []
    date_dirs = sorted(
        [d for d in os.listdir(topic_path) if d.startswith("date=")],
        reverse=True
    )

    for date_dir in date_dirs:
        full_dir = os.path.join(topic_path, date_dir)
        if not os.path.isdir(full_dir):
            continue

        for file in os.listdir(full_dir):
            if file.endswith(".csv"):
                file_path = os.path.join(full_dir, file)
                try:
                    df = pd.read_csv(file_path)

                    df.replace([np.nan, np.inf, -np.inf], None, inplace=True)

                    filtered_columns = [col for col in selected_columns if col in df.columns]
                    if filtered_columns:
                        subset = df[filtered_columns].to_dict(orient="records")
                        result.extend(subset)

                except Exception as e:
                    print(f"Erreur fichier {file_path} : {e}")
                    continue

    if not result:
        return Response({"error": "Aucune donnée trouvée pour les colonnes demandées."}, status=status.HTTP_204_NO_CONTENT)

    return Response(result, status=status.HTTP_200_OK)

@api_view(['GET'])
def spent_last_5_minutes_from_datalake(request, topic_name):
    messages = read_messages_from_datalake(topic_name)
    now = datetime.now()
    spent = 0.0

    for msg in messages:
        try:
            timestamp_str = msg.get("TIMESTAMP_OF_RECEPTION_LOG", "")
            timestamp = datetime.strptime(timestamp_str, "%d/%m/%Y %H:%M:%S")
            if (now - timestamp).total_seconds() <= 300000000000000000000000000000000000000000000000000:
                amount = float(msg.get("AMOUNT_EUR", 0) or msg.get("AMOUNT", 0))
                quantity = int(msg.get("QUANTITY", 1))
                spent += amount * quantity
        except:
            continue

    return Response({"money_spent_last_5_minutes": round(spent, 2)})


@api_view(['GET'])
def total_spent_by_user_from_datalake(request, topic_name):
    messages = read_messages_from_datalake(topic_name)
    result = defaultdict(lambda: defaultdict(float))

    for msg in messages:
        user = msg.get('ANO_USER_NAME') or msg.get('ANO_USER_ID') or msg.get('USER_ID')
        t_type = msg.get("TRANSACTION_TYPE", "unknown")
        try:
            amount = float(msg.get("AMOUNT_EUR", 0) or msg.get("AMOUNT", 0))
            quantity = int(msg.get("QUANTITY", 1))
            result[user][t_type] += amount * quantity
        except:
            continue

    formatted_result = [
        {
            "user": user,
            "transactions": [{"type": t_type, "total_spent": round(spent, 2)} for t_type, spent in types.items()]
        } for user, types in result.items()
    ]

    return Response(formatted_result)

@api_view(['GET'])
def top_products_from_datalake(request, topic_name):
    try:
        x = int(request.GET.get("x", 5))
    except:
        return Response({"error": "Le paramètre 'x' doit être un entier."}, status=400)

    messages = read_messages_from_datalake(topic_name)
    product_counts = defaultdict(int)

    for msg in messages:
        product_id = msg.get("PRODUCT_ID")
        try:
            quantity = int(msg.get("QUANTITY", 1))
            if product_id:
                product_counts[product_id] += quantity
        except:
            continue

    top = sorted(product_counts.items(), key=lambda item: item[1], reverse=True)[:x]

    return Response([{"product_id": pid, "total_quantity": qty} for pid, qty in top])

@api_view(['GET'])
def get_versioned_data_view(request, topic_name):
    version = request.query_params.get('version')
    if not version:
        return Response({'error': 'Paramètre version requis.'}, status=status.HTTP_400_BAD_REQUEST)

    # Exemple : version = "2025-06-02"
    version_path = os.path.join(DATA_LAKE_PATH, topic_name, f'date={version}')
    if not os.path.exists(version_path):
        return Response({'error': f'Version {version} non trouvée pour le topic {topic_name}.'}, status=status.HTTP_404_NOT_FOUND)

    all_data = []
    for file_name in os.listdir(version_path):
        if file_name.endswith('.csv'):
            file_path = os.path.join(version_path, file_name)
            try:
                df = pd.read_csv(file_path)
                df.replace([np.nan, np.inf, -np.inf], None, inplace=True)
                all_data.extend(df.to_dict(orient='records'))
            except Exception as e:
                print(f"Erreur lors de la lecture de {file_path} : {e}")

    return Response(all_data, status=status.HTTP_200_OK)
