__author__ = 'Dmitry Golubkov'

class DefaultRouter(object):
    def db_for_read(self, model, **hints):
        if model._meta.app_label == 'admin':
            return 'deft_intr'
        if model._meta.app_label == 'auth':
            return 'deft_intr'
        try:
            return model._meta.db_name
        except AttributeError:
            return None

    def db_for_write(self, model, **hints):
        try:
            return model._meta.db_name
        except AttributeError:
            return None

    def allow_relation(self, obj1, obj2, **hints):
        return None

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        return None
