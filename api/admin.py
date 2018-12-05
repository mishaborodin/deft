__author__ = 'Dmitry Golubkov'

from django.contrib import admin
from deftcore.helpers import ReadOnlyAdmin
from api.models import Request


class RequestAdmin(ReadOnlyAdmin):
    list_display = ['id', 'created', 'timestamp', 'action', 'owner', 'body', 'status']
    search_fields = ['id']


admin.site.register(Request, RequestAdmin)
