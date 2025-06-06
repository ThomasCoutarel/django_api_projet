import json
from .models import AccessLog

class AccessLogMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        response = self.get_response(request)

        user = request.user if request.user.is_authenticated else None
        method = request.method
        path = request.path

        try:
            body = request.body.decode('utf-8')
            try:
                body_json = json.loads(body)
                body = json.dumps(body_json, indent=2)
            except Exception:
                pass
        except Exception:
            body = ''

        # Crée le log
        AccessLog.objects.create(
            user=user,
            method=method,
            path=path,
            body=body
        )

        return response