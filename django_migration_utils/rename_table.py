


def fwd_rename_app(apps, schema_editor, apps_to_rename):

    for old_appname, new_appname in apps_to_rename:

        # Renaming model from 'Foo' to 'Bar'
        schema_editor.execute("UPDATE django_migrations SET app_name = %s WHERE app_name = %s", [new_appname, old_appname])
        schema_editor.execute("UPDATE django_content_type SET app_label = %s WHERE app_label = %s", [new_appname, old_appname])

        new_app = apps.get_app_config(new_appname)
        app_models = new_app.get_models(include_auto_created=True)
        for model in app_models:
            if model._meta.proxy == True:
                continue

            new_table_name = model._meta.db_table
            old_table_name = old_appname + new_table_name[len(new_appname):]

            schema_editor.alter_db_table(old_table_name, new_table_name)


def back_rename_app(apps, schema_editor, apps_to_rename):

    for old_appname, new_appname in apps_to_rename:

        # Renaming model back from 'Bar' to 'Foo'
        schema_editor.execute("UPDATE django_migrations SET app_name = %s WHERE app_name = %s", [old_appname, new_appname])
        schema_editor.execute("UPDATE django_content_type SET app_label = %s WHERE app_label = %s", [old_appname, new_appname])

        new_app = apps.get_app_config(new_appname)
        app_models = new_app.get_models(include_auto_created=True)
        for model in app_models:
            if model._meta.proxy == True:
                continue

            old_table_name = model._meta.db_table
            new_table_name = old_appname + old_table_name[len(new_appname):]

            schema_editor.alter_db_table(old_table_name, new_table_name)

