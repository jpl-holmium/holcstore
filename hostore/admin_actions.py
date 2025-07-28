import io
import logging
import zipfile

import datetime as dt
from gettext import gettext

import pandas as pd
from django.contrib import admin, messages
from django.http import HttpResponse

logger = logging.getLogger(__name__)


class CompressedExport:
    def __init__(self):
        self.attachment_files = []

    def append_df_attach(self, df, filename):
        """
        Append Dataframe as CSV
        """
        buffer = io.BytesIO()
        df.to_csv(buffer, **dict(sep=';', decimal='.', float_format='%.8f'))
        buffer.seek(0)
        self.attachment_files.append((filename, buffer.getvalue()))

    def make_zip_binary(self):
        # Create a bytes buffer for the ZIP file
        zip_buffer = io.BytesIO()
        # Create a ZIP file in memory and add the CSV data
        with zipfile.ZipFile(zip_buffer, 'a', zipfile.ZIP_DEFLATED, False) as zip_file:
            for attach in self.attachment_files:
                zip_file.writestr(attach[0], attach[1])
        zip_buffer.seek(0)  # Go back to the start of the buffer
        return zip_buffer


@admin.action(description="Download selected timeseries")
def download_timeseries_from_store(modeladmin, request, queryset):
    holc_ts_qs = queryset.all()
    if not holc_ts_qs.exists():
        modeladmin.message_user(
            request,
            gettext("Please select at least one object"),
            messages.WARNING,
        )
        return
    datas = []
    ts_obj = None
    for ts_obj in holc_ts_qs:
        obj_content = ts_obj.decoded_ts_data
        ds = obj_content.pop('data')
        obj_content.pop('id')

        obj_content = {k: v for k, v in obj_content.items() if k[0] != '_'}
        datas.append((ds, obj_content))

    exp = CompressedExport()
    summary_data = []
    for ii, (ds, obj_content) in enumerate(datas):
        # Application du format
        filename = f'export_serie_{ii}.csv'
        df = ds.to_frame(name='data')
        exp.append_df_attach(df, filename)
        summary_data.append({'filename': filename, **obj_content})

    # Append summary
    df_summary = pd.DataFrame(summary_data)
    exp.append_df_attach(df_summary, f'content_summary.csv')

    # Make binary
    zip_buffer = exp.make_zip_binary()

    # Prepare the response, setting the HTTP headers for a downloadable file
    model_name_exported = ts_obj.__class__.__name__
    output_file = f'export_{len(datas)}_{model_name_exported}_{dt.datetime.utcnow().isoformat()}.zip'
    response = HttpResponse(zip_buffer, content_type='application/zip')
    response['Content-Disposition'] = f'attachment; filename="{output_file}"'

    return response

@admin.action(description="Download selected timeseries chunks")
def download_timeseries_from_chunkstore(modeladmin, request, queryset):
    holc_ts_qs = queryset.all()
    if not holc_ts_qs.exists():
        modeladmin.message_user(
            request,
            gettext("Please select at least one object"),
            messages.WARNING,
        )
        return

    datas = []
    ts_obj = None
    for ts_obj in holc_ts_qs:
        obj_content = vars(ts_obj)
        ds = ts_obj.__class__._decompress(ts_obj)
        obj_content.pop("data")
        obj_content.pop("id", None)
        obj_content = {k: v for k, v in obj_content.items() if k[0] != "_"}
        datas.append((ds, obj_content))

    exp = CompressedExport()
    summary_data = []
    for ii, (ds, obj_content) in enumerate(datas):
        filename = f"export_serie_{ii}.csv"
        df = ds.to_frame(name="data")
        exp.append_df_attach(df, filename)
        summary_data.append({"filename": filename, **obj_content})

    df_summary = pd.DataFrame(summary_data)
    exp.append_df_attach(df_summary, "content_summary.csv")

    zip_buffer = exp.make_zip_binary()

    model_name_exported = ts_obj.__class__.__name__
    output_file = (
        f"export_{len(datas)}_{model_name_exported}_{dt.datetime.utcnow().isoformat()}.zip"
    )
    response = HttpResponse(zip_buffer, content_type="application/zip")
    response["Content-Disposition"] = f'attachment; filename="{output_file}"'

    return response