# Generated by Django 5.0.2 on 2024-03-21 16:21

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('hostore', '0001_initial'),
    ]

    operations = [
        migrations.RenameIndex(
            model_name='testdatastore',
            new_name='hostore_tes_prm_897468_idx',
            old_name='store_testd_prm_91e86e_idx',
        ),
    ]
