__author__ = 'Dmitry Golubkov'


class DefaultRouter(object):
    def db_for_read(self, model, **hints):
        try:
            return model._meta.db_name
        except AttributeError:
            return 'default'

    def db_for_write(self, model, **hints):
        try:
            return model._meta.db_name
        except AttributeError:
            return 'default'

    def allow_relation(self, obj1, obj2, **hints):
        return None

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        if db == 'default':
            return True
        else:
            return False
