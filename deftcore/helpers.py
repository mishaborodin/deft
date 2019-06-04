__author__ = 'Dmitry Golubkov'

import threading
import requests
import cx_Oracle
from django.contrib import admin
from deftcore.settings import DEBUG


class ReadOnlyAdmin(admin.ModelAdmin):
    def __init__(self, model, admin_site):
        super(ReadOnlyAdmin, self).__init__(model, admin_site)
        self.readonly_fields = [field.name for field in filter(lambda f: not f.auto_created, model._meta.fields)]

    def has_delete_permission(self, request, obj=None):
        return False

    def has_add_permission(self, request, obj=None):
        return False


class DeleteOnlyAdmin(ReadOnlyAdmin):
    def has_delete_permission(self, request, obj=None):
        return True


class Singleton(type):
    def __init__(cls, *args, **kwargs):
        super(Singleton, cls).__init__(*args, **kwargs)
        cls.__instance = None

    def __call__(cls, *args, **kwargs):
        if cls.__instance is None:
            cls.__instance = super(Singleton, cls).__call__(*args, **kwargs)
        return cls.__instance


class ImportHelper(object):
    def __init__(self, module_name):
        self.module_name = module_name

    def import_module(self):
        module = __import__(self.module_name)
        for component in self.module_name.split('.')[1:]:
            module = getattr(module, component)
        return module


class AsyncRequest(threading.Thread):
    def __init__(self, method, url, args=None):
        super(AsyncRequest, self).__init__()
        self.method = method
        self.url = url
        self.args = args
        self.callback = None

    def set_callback(self, callback):
        self.callback = callback

    def run(self):
        try:
            if self.args:
                response = requests.request(self.method, self.url, **self.args)
            else:
                response = requests.request(self.method, self.url)
            if self.callback:
                self.callback(response)
        except:
            pass


class Enum(object):
    values = []

    class __metaclass__(type):
        def __getattr__(self, name):
            return self.values.index(name)


class OracleClob(unicode):
    def __new__(cls, *args, **kwargs):
        obj = unicode.__new__(cls, *args, **kwargs)
        obj.input_size = cx_Oracle.CLOB
        return obj
