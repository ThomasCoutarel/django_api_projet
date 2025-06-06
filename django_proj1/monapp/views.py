from django.http import JsonResponse
import json
from django.views.decorators.csrf import csrf_exempt
from .models import Product
from functools import wraps
from .models import UserRight
import logging
from datetime import datetime
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

import logging

# Configuration basique du logging
logging.basicConfig(
    filename='access.log',  # fichier dans lequel on écrit les logs
    level=logging.INFO,     # niveau INFO
    format='%(asctime)s - %(message)s'  # format des messages
)

def log_access(view_func):
    def wrapper(request, *args, **kwargs):
        token = None
        try:
            data = json.loads(request.body)
            token = data.get("token", "unknown")
        except:
            token = "unknown"

        logging.info(
            f"Token: {token} | URL: {request.path} | Méthode: {request.method} | Body: {request.body.decode('utf-8')}"
        )
        return view_func(request, *args, **kwargs)
    return wrapper



def check_permission(request):
    def decorator(view_func):
        def wrapper(request, *args, **kwargs):
            token = request.headers.get("Authorization")
            if not token:
                return JsonResponse({"error": "Token manquant"}, status=401)

            try:
                rights = UserRight.objects.get(token=token)
            except UserRight.DoesNotExist:
                return JsonResponse({"error": "Token invalide"}, status=403)

            if not getattr(rights, permission_name, False):
                return JsonResponse({"error": "Permission refusée"}, status=403)

            return view_func(request, *args, **kwargs)
        return wrapper
    return decorator

from functools import wraps
from django.http import JsonResponse
from .models import UserRight
import json

def admin_required(view_func):
    @wraps(view_func)
    def _wrapped_view(request, *args, **kwargs):
        try:
            data = json.loads(request.body)
            token = data.get("token")
        except Exception:
            return JsonResponse({"error": "requête invalide ou token manquant"}, status=400)

        if not token:
            return JsonResponse({"error": "token requis"}, status=400)

        try:
            user = UserRight.objects.get(token=token)
        except UserRight.DoesNotExist:
            return JsonResponse({"error": "token non trouvé"}, status=404)

        if not user.is_admin:
            return JsonResponse({"error": "accès non autorisé, admin requis"}, status=403)

        return view_func(request, *args, **kwargs)
    return _wrapped_view


@csrf_exempt
@admin_required
@log_access
def grant_right(request):
    data = json.loads(request.body)
    target_token = data.get("target_token")  # au lieu de "token"
    rights = data.get("rights")

    if not target_token or not rights:
        return JsonResponse({"error": "target_token et rights obligatoires"}, status=400)

    try:
        user_right = UserRight.objects.get(token=target_token)
    except UserRight.DoesNotExist:
        return JsonResponse({"error": "utilisateur cible non trouvé"}, status=404)

    for right in rights:
        if hasattr(user_right, right):
            setattr(user_right, right, True)
    user_right.save()

    return JsonResponse({"status": "droits accordés", "token": target_token})

@csrf_exempt
@admin_required
@log_access
def revoke_right(request):
    if request.method == "POST":
        data = json.loads(request.body)
        target_token = data.get("target_token")  # ➤ Token de l'utilisateur à modifier
        rights = data.get("rights")              # ➤ Liste des droits à révoquer

        if not target_token or not rights:
            return JsonResponse({"error": "target_token et rights obligatoires"}, status=400)

        try:
            user_right = UserRight.objects.get(token=target_token)
        except UserRight.DoesNotExist:
            return JsonResponse({"error": "utilisateur cible non trouvé"}, status=404)

        for right in rights:
            if hasattr(user_right, right):
                setattr(user_right, right, False)

        user_right.save()
        return JsonResponse({"status": "droits révoqués", "token": target_token})
    else:
        return JsonResponse({"error": "méthode non autorisée"}, status=405)



def test_json_view(request):
    data = {
        'name': 'John Doe',
        'age': 30,
        'location': 'New York',
        'is_active': True,
    }
    return JsonResponse(data)

@csrf_exempt
def test_json_post(request):
    if request.method == 'POST':
        try:
            body_unicode = request.body.decode('utf-8')
            body_data = json.loads(body_unicode)
            user_name = body_data.get('user', 'John Doe')
        except json.JSONDecodeError:
            return JsonResponse({'error': 'Invalid JSON'}, status=400)

        data = {
            'name': user_name,
            'age': 30,
            'location': 'New York',
            'is_active': True,
        }
        return JsonResponse(data)
    else:
        return JsonResponse({'error': 'Only POST method is allowed'}, status=405)


@check_permission("can_view_products")
def get_available_products(request):
    if request.method == 'GET':
        try:
            page = int(request.GET.get('page', 1))
            page_size = int(request.GET.get('page_size', 3))
            if page < 1 or page_size < 1:
                raise ValueError()
        except ValueError:
            return JsonResponse({'error': 'Paramètres de pagination invalides'}, status=400)

        offset = (page - 1) * page_size
        limit = offset + page_size

        # On filtre les produits disponibles, puis on les coupe selon la page
        total_products = Product.objects.filter(available=True).count()
        products = Product.objects.filter(available=True)[offset:limit]

        products_data = []
        for product in products:
            products_data.append({
                'id': product.id,
                'name': product.name,
                'price': str(product.price),
                'description': product.description,
                'created_at': product.created_at.isoformat(),
                'updated_at': product.updated_at.isoformat(),
            })

        total_pages = (total_products + page_size - 1) // page_size

        return JsonResponse({
            'page': page,
            'page_size': page_size,
            'total_products': total_products,
            'total_pages': total_pages,
            'products': products_data
        })
    else:
        return JsonResponse({'error': 'Only GET method is allowed'}, status=405)


@check_permission("can_view_most_expensive")
def get_most_expensive_product(request):
    if request.method == 'GET':
        product = Product.objects.order_by('-price').first()

        if product:
            product_data = {
                'id': product.id,
                'name': product.name,
                'price': str(product.price),
                'description': product.description,
                'created_at': product.created_at.isoformat(),
                'updated_at': product.updated_at.isoformat(),
            }
            return JsonResponse({'most_expensive_product': product_data})
        else:
            return JsonResponse({'message': 'No products found'}, status=404)
    else:
        return JsonResponse({'error': 'Only GET method is allowed'}, status=405)



@csrf_exempt
@check_permission("can_add_product")
def add_product(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            name = data.get('name')
            price = data.get('price')
            description = data.get('description', '')

            if not name or not price:
                return JsonResponse({'error': 'Le nom et le prix sont obligatoires'}, status=400)

            product = Product.objects.create(
                name=name,
                price=price,
                description=description
            )

            return JsonResponse({
                'message': 'Produit ajouté avec succès',
                'product': {
                    'id': product.id,
                    'name': product.name,
                    'price': str(product.price),
                    'description': product.description,
                    'created_at': product.created_at.isoformat(),
                    'updated_at': product.updated_at.isoformat(),
                }
            }, status=201)

        except json.JSONDecodeError:
            return JsonResponse({'error': 'Format JSON invalide'}, status=400)

    return JsonResponse({'error': 'Seule la méthode POST est autorisée'}, status=405)



@csrf_exempt
@check_permission("can_update_product")
def update_product(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            product_id = data.get('id')
            name = data.get('name')
            price = data.get('price')
            description = data.get('description')

            if not product_id:
                return JsonResponse({'error': 'ID du produit requis'}, status=400)

            try:
                product = Product.objects.get(id=product_id)
            except Product.DoesNotExist:
                return JsonResponse({'error': 'Produit non trouvé'}, status=404)

            if name:
                product.name = name
            if price:
                product.price = price
            if description is not None:
                product.description = description

            product.save()

            return JsonResponse({
                'message': 'Produit mis à jour avec succès',
                'product': {
                    'id': product.id,
                    'name': product.name,
                    'price': str(product.price),
                    'description': product.description,
                    'created_at': product.created_at.isoformat(),
                    'updated_at': product.updated_at.isoformat()
                }
            })

        except json.JSONDecodeError:
            return JsonResponse({'error': 'Données JSON invalides'}, status=400)

    return JsonResponse({'error': 'Seule la méthode POST est autorisée'}, status=405)
