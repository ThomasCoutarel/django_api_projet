from django.contrib import admin
from django import forms
import os

from .models import AccessRight, AccessLog

# Chemin vers le data lake local
DATA_LAKE_PATH = "C:/Users/couta/Python project/data/data_lake"

# Formulaire personnalisé pour AccessRight
class AccessRightForm(forms.ModelForm):
    class Meta:
        model = AccessRight
        fields = '__all__'

    def __init__(self, *args, **kwargs):
        super(AccessRightForm, self).__init__(*args, **kwargs)

        # Récupérer le topic actuel (initial ou envoyé via POST)
        topic = self.initial.get('topic_name') or self.data.get('topic_name')
        if topic:
            topic_path = os.path.join(DATA_LAKE_PATH, topic)
            if os.path.exists(topic_path):
                versions = sorted([
                    d.replace('date=', '') for d in os.listdir(topic_path)
                    if os.path.isdir(os.path.join(topic_path, d)) and d.startswith('date=')
                ])
                # Ajouter les versions trouvées comme choix (avec "ALL" comme option par défaut)
                self.fields['version'].widget = forms.Select(choices=[('', 'ALL')] + [(v, v) for v in versions])

from django.contrib import admin
from .models import AccessRight, AccessLog

class AccessRightAdmin(admin.ModelAdmin):
    list_display = ('user', 'resource', 'version', 'can_read', 'can_write')
    search_fields = ('user__username', 'resource', 'version')

admin.site.register(AccessRight, AccessRightAdmin)
admin.site.register(AccessLog)
