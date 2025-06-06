from django.db import models
from django.contrib.auth.models import User

class AccessRight(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    resource = models.CharField(max_length=100)  # ex: "BLACKLIST_TRANSACTIONS"
    version = models.CharField(max_length=20, blank=True, null=True)  # ex: "2025-06-02"
    can_read = models.BooleanField(default=True)
    can_write = models.BooleanField(default=False)

    class Meta:
        unique_together = ('user', 'resource', 'version')  # permet de gérer version par version

    def __str__(self):
        return f"{self.user.username} rights on {self.resource} [{self.version or 'ALL'}]"


class AccessLog(models.Model):
    user = models.ForeignKey(User, on_delete=models.SET_NULL, null=True)
    method = models.CharField(max_length=10)
    path = models.CharField(max_length=255)
    body = models.TextField(blank=True, null=True)
    timestamp = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.user} {self.method} {self.path} at {self.timestamp}"