################################################################
# THIS FILE IS ONLY USED FOR TESTING PURPOSES
################################################################
from django.db import models
from hostore.models import TimeseriesChunkStore


class MyTimeseriesChunkStore(TimeseriesChunkStore):
    version = models.IntegerField()
    kind_very_long_name_for_testing_purpose = models.CharField(max_length=50)
    CHUNK_AXIS = ('year', 'month')

################################################################
# THIS FILE IS ONLY USED FOR TESTING PURPOSES
################################################################