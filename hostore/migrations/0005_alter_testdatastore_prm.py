# Generated by Django 5.0.2 on 2024-05-23 15:35

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('hostore', '0004_testdatastore_created_at'),
    ]

    operations = [
        migrations.AlterField(
            model_name='testdatastore',
            name='prm',
            field=models.CharField(max_length=70),
        ),
    ]
