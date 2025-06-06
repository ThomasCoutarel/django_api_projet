from django.db import models

class Product(models.Model):
    name = models.CharField(max_length=100)
    price = models.DecimalField(max_digits=10, decimal_places=2)
    description = models.TextField(blank=True, null=True)
    available = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.name

class UserRight(models.Model):
    token = models.CharField(max_length=100, unique=True)  # Token d'authentification
    can_view_products = models.BooleanField(default=False)
    can_add_product = models.BooleanField(default=False)
    can_update_product = models.BooleanField(default=False)
    can_view_most_expensive = models.BooleanField(default=False)
    is_admin = models.BooleanField(default=False)

def __str__(self):
        return f"Droits pour le token {self.token}"

class ApiAccessLog(models.Model):
    token = models.CharField(max_length=255)
    timestamp = models.DateTimeField(auto_now_add=True)
    method = models.CharField(max_length=10)
    path = models.CharField(max_length=255)
    body = models.TextField()

    def __str__(self):
        return f"{self.token} - {self.method} {self.path} at {self.timestamp}"
