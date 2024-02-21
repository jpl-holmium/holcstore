============
HoLcStore
============

HoLcStore is a Django app for creating a simple TimeSeries store in your database.

Quick start
-----------

1. Add "holcstore" to your INSTALLED_APPS setting like this::

    INSTALLED_APPS = [
        ...,
        "holcstore",
    ]


2. Run ``python manage.py migrate`` to create the models.

3. Start using the abstract model ``Store`` by importing it ::

    from holcstore.store.models import Store

    class YourStore(Store):
        # add new fields

        class Meta(Store.Meta):
            abstract = False
            # add your meta

