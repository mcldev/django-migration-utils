from django.db import connection


def get_cursor():
    return connection.cursor()


def fetch_rows_as_dict(cursor=None, table_name=None):
    cursor = cursor or get_cursor()
    "Return all rows from a cursor as a dict"
    if table_name:
        cursor.execute("SELECT * FROM %s" % table_name)
    columns = [col[0] for col in cursor.description]
    return [
        dict(zip(columns, row)) for row in cursor.fetchall()
    ]


def remove_all_rows(table_name, cursor=None):
    cursor = cursor or get_cursor()
    cursor.execute("DELETE FROM ONLY %s" % table_name)


def remove_table(table_name, cursor=None):
    remove_tables([table_name], cursor)


def remove_tables(tables, cursor=None):
    cursor = cursor or get_cursor()
    cursor.execute("DROP TABLE IF EXISTS %s CASCADE" % ','.join(tables))


# Need to reset indexes when copying over primary keys for postgres.
def update_indexes(cursor, table_name):
    cursor.execute("SELECT setval('%s_id_seq', (SELECT MAX(id) FROM %s)+1)" % (table_name, table_name))


def check_table_exists(cursor, table_name):
    check_exists_query = "SELECT relname FROM pg_class WHERE relname=%s;"
    cursor.execute(check_exists_query, [table_name])
    result = cursor.fetchone()
    return result


# old_value_mapping = {
#     'popup_image': {
#         True: 'pop-up',
#         None: 'default'
#     },
#     'name': {
#         None: get_slug,
#     }
# }

def convert_value_mapping(apps, field_name, value_mapping, new_value, old_data_row):
    if not (value_mapping and field_name and field_name in value_mapping):
        return new_value

    if new_value and new_value in value_mapping[field_name]:
        # Mapped Static Value: True > 'pop-up'
        new_value = value_mapping[field_name][new_value]
    elif None in value_mapping[field_name]:
        # Get default value or convert function
        val_or_func = value_mapping[field_name][None]
        if callable(val_or_func):
            try:
                new_value = val_or_func(new_value, apps, old_data_row)
            except:
                try:
                    new_value = val_or_func(new_value, apps)
                except:
                    new_value = val_or_func(new_value)
        else:
            # Default value if not in mapping table e.g. False or 0 or... > 'default'
            new_value = val_or_func
    return new_value


def add_fields_to_model(apps, model_inst, data_row, fields_to_migrate, old_value_mapping, new_value_mapping):
    for field in fields_to_migrate:
        old_field = field[0]
        new_field = field[1]
        new_value = None
        if old_field:
            new_value = data_row[old_field]

        # Map Values to Old/New Value mapping
        new_value = convert_value_mapping(apps, old_field, old_value_mapping, new_value, data_row)
        new_value = convert_value_mapping(apps, new_field, new_value_mapping, new_value, data_row)

        setattr(model_inst, new_field, new_value)
    return model_inst


def convert_old_table_to_new_models(apps, old_table, new_app, new_model,
                                    fields_to_migrate=None,
                                    old_value_mapping=None,
                                    new_value_mapping=None,
                                    cursor=None):
    cursor = cursor or get_cursor()

    # Check table exists
    if not check_table_exists(cursor, old_table):
        print('Table: %s does not exist, no migration.' % old_table)
        return
    print('Starting migration for table: %s .' % old_table)

    # Get raw data as dictionary from Old Table
    old_data_rows = fetch_rows_as_dict(cursor, old_table)

    # Exit if no rows to migrate...!
    if not old_data_rows:
        return

    # Get fieldnames if provided, else use field names from previous table
    if not fields_to_migrate:
        fields_to_migrate = [(f, f) for f in old_data_rows[0].keys()]

    # Get New Model
    NewModel = apps.get_model(new_app, new_model)

    new_models = []

    # Copy over all data
    for old_data_row in old_data_rows:

        # Create New Model
        new_model_instance = NewModel()

        # Get Each Field Value
        add_fields_to_model(apps, new_model_instance, old_data_row,
                            fields_to_migrate, old_value_mapping, new_value_mapping)

        new_models.append(new_model_instance)

    return NewModel, new_models


def migrate_cms_plugin(apps, old_table, new_app, new_model, new_plugin_type, fields_to_migrate,
                       old_value_mapping=None, new_value_mapping=None, cursor=None):

    cursor = cursor or get_cursor()

    # Check table exists
    if not check_table_exists(cursor, old_table):
        print('Table: %s does not exist, no migration.' % old_table)
        return
    print('Starting migration for table: %s .' % old_table)

    # Get raw data as dictionary from Old Table
    old_plugin_data_rows = fetch_rows_as_dict(cursor, old_table)

    # Exit if no rows to migrate...!
    if not old_plugin_data_rows:
        return

    # Get New Model and CMS Plugin
    NewModel = apps.get_model(new_app, new_model)
    CMSPluginModel = apps.get_model('cms', 'cmsplugin')
    cms_plugins = CMSPluginModel.objects.all()

    for old_plugin_data_row in old_plugin_data_rows:
        # Get original cms plugin
        cms_plugin_id = old_plugin_data_row['cmsplugin_ptr_id']
        cms_plugin = cms_plugins.get(id=cms_plugin_id)

        # Create New Model
        new_plugin = NewModel()

        # CMS Plugin Existing Values:
        new_plugin.cmsplugin_ptr = cms_plugin
        new_plugin.placeholder_id = cms_plugin.placeholder_id
        new_plugin.parent_id = cms_plugin.parent_id
        new_plugin.position = cms_plugin.position
        new_plugin.language = cms_plugin.language
        new_plugin.creation_date = cms_plugin.creation_date
        new_plugin.changed_date = cms_plugin.changed_date

        # Set new plugin type
        new_plugin.plugin_type = new_plugin_type

        # MP Node Values:
        new_plugin.path = cms_plugin.path
        new_plugin.depth = cms_plugin.depth
        new_plugin.numchild = cms_plugin.numchild

        # Model Specific Fields
        add_fields_to_model(apps, new_plugin, old_plugin_data_row,
                            fields_to_migrate, old_value_mapping, new_value_mapping)

        # Save new plugin
        new_plugin.save()
