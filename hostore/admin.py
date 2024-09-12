from django.contrib import admin

from hostore.admin_actions import download_timeseries_from_store
from hostore.models import TestTimeseriesStoreWithAttribute


class TestTimeseriesStoreWithAttributeAdmin(admin.ModelAdmin):
    resource_classes = [TestTimeseriesStoreWithAttribute]
    list_display = ('year', 'kind', )
    list_filter = ('year', 'kind', )
    actions = [download_timeseries_from_store]
