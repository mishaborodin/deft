__author__ = 'Dmitry Golubkov'

from django.contrib import admin


class ReadOnlyAdmin(admin.ModelAdmin):
    def __init__(self, model, admin_site):
        super(ReadOnlyAdmin, self).__init__(model, admin_site)
        self.readonly_fields = [field.name for field in [f for f in model._meta.fields if not f.auto_created]]

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
