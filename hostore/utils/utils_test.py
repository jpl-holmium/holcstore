from django.db import connection


class TempTestTableHelper:
    test_table = None

    def _ensure_tables(self, delete_kw=None):
        """(Re)crée les tables manquantes après un flush Django."""
        delete_kw = delete_kw or dict()
        existing = connection.introspection.table_names()
        if self.test_table is None:
            raise ValueError('test_table is None')
        with connection.schema_editor(atomic=False) as se:
            if self.test_table._meta.db_table not in existing:
                se.create_model(self.test_table)
        self.test_table.objects.all().delete(**delete_kw)
