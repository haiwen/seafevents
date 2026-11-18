from seafevents.repo_metadata.backup import (
    FILE_IDENTITY_COLUMN_KEYS,
    MAIN_TABLE_ID,
    PAGE_SIZE,
    SYSTEM_COLUMN_KEYS,
    MetadataBackupError,
)


def restore_metadata_backup(metadata_server_api, repo_id, payload):
    metadata = metadata_server_api.get_metadata(repo_id)
    tables = metadata.get('tables') or []
    main = next((table for table in tables if table.get('id') == MAIN_TABLE_ID), None)
    if not main:
        raise MetadataBackupError('Metadata table not found')

    _replace_main_columns(
        metadata_server_api, repo_id, main,
        payload['columns'], payload['preserved_column_keys']
    )
    main_row_ids = _replace_main_rows(
        metadata_server_api, repo_id, main, payload['columns'], payload['rows'],
        payload['conditional_column_keys']
    )

    for table in tables:
        if table['id'] != MAIN_TABLE_ID:
            metadata_server_api.delete_table(repo_id, table['id'], permanently=True)

    table_ids = {MAIN_TABLE_ID: MAIN_TABLE_ID}
    row_ids = {MAIN_TABLE_ID: main_row_ids}
    for table in payload['tables']:
        response = metadata_server_api.create_table(repo_id, table['name'])
        table_id = response['id']
        table_ids[table['id']] = table_id
        if table['columns']:
            metadata_server_api.add_columns(repo_id, table_id, table['columns'])
        row_ids[table['id']] = _insert_rows(
            metadata_server_api, repo_id, table_id, table['columns'], table['rows']
        )

    for link in payload['links']:
        table_id = table_ids.get(link['t1_id'])
        other_table_id = table_ids.get(link['t2_id'])
        if not table_id or not other_table_id:
            continue
        metadata_server_api.add_link_column(
            repo_id, link['link_id'], table_id, other_table_id,
            link['t1_column'], link['t2_column']
        )
        source_ids = row_ids.get(link['t1_id'], {})
        target_ids = row_ids.get(link['t2_id'], {})
        relations = {
            source_ids[row_id]: [target_ids[other_id] for other_id in other_ids if other_id in target_ids]
            for row_id, other_ids in link['row_id_map'].items()
            if row_id in source_ids
        }
        relations = {row_id: other_ids for row_id, other_ids in relations.items() if other_ids}
        for batch in _batches(list(relations.items())):
            metadata_server_api.insert_link(
                repo_id, link['link_id'], table_id, dict(batch)
            )


def _replace_main_columns(metadata_server_api, repo_id, main, backup_columns, preserved_keys):
    backup_by_key = {column['key']: column for column in backup_columns}
    unchanged = {
        column['key'] for column in main.get('columns') or []
        if column['key'] in backup_by_key and _same_column(column, backup_by_key[column['key']])
    }
    preserved = SYSTEM_COLUMN_KEYS | set(preserved_keys) | {'_id'}
    deleted_links = set()
    for column in main.get('columns') or []:
        if column['key'] in preserved or column['key'] in unchanged:
            continue
        if column.get('type') == 'link':
            link_id = (column.get('data') or {}).get('link_id')
            if link_id and link_id in deleted_links:
                continue
            if link_id:
                deleted_links.add(link_id)
        metadata_server_api.delete_column(
            repo_id, MAIN_TABLE_ID, column['key'], permanently=True
        )

    columns = [column for column in backup_columns if column['key'] not in unchanged]
    if columns:
        metadata_server_api.add_columns(repo_id, MAIN_TABLE_ID, columns)


def _replace_main_rows(metadata_server_api, repo_id, main, columns, backup_rows,
                       conditional_keys):
    current_rows = _query_main_rows(metadata_server_api, repo_id, main['name'])
    backup_by_id = {row['_id']: row for row in backup_rows}
    backup_by_identity = {}
    for row in backup_rows:
        identity = _file_identity(row)
        if identity is not None:
            backup_by_identity[identity] = row
    conditional_keys = set(conditional_keys)
    row_ids = {}
    updates = []
    for current in current_rows:
        row_id = current['_id']
        backup = backup_by_id.get(row_id)
        if backup is None:
            backup = backup_by_identity.get(_file_identity(current))
        if backup:
            row_ids[backup['_id']] = row_id
        if not columns:
            continue
        update = {'_id': row_id}
        for column in columns:
            value = backup.get(column['key']) if backup else None
            if column['key'] in conditional_keys and (
                    not backup or current.get('_obj_id') != backup.get('_obj_id')):
                value = None
            update[column['name']] = value
        updates.append(update)

    for batch in _batches(updates):
        metadata_server_api.update_rows(repo_id, MAIN_TABLE_ID, batch)
    return row_ids


def _query_main_rows(metadata_server_api, repo_id, table_name):
    rows = []
    escaped_name = table_name.replace('`', '``')
    while True:
        result = metadata_server_api.query_rows(
            repo_id,
            f'SELECT `_id`, `_obj_id`, `_parent_dir`, `_name`, `_is_dir` '
            f'FROM `{escaped_name}` LIMIT {len(rows)}, {PAGE_SIZE}'
        ).get('results') or []
        rows.extend(result)
        if len(result) < PAGE_SIZE:
            return rows


def _file_identity(row):
    identity = tuple(row.get(key) for key in FILE_IDENTITY_COLUMN_KEYS)
    return identity if all(value is not None for value in identity) else None


def _insert_rows(metadata_server_api, repo_id, table_id, columns, rows):
    names = {column['key']: column['name'] for column in columns}
    row_ids = {}
    for batch in _batches(rows):
        response = metadata_server_api.insert_rows(repo_id, table_id, [
            {names[key]: value for key, value in row.items() if key in names}
            for row in batch
        ])
        inserted_ids = response.get('row_ids') or []
        if len(inserted_ids) != len(batch):
            raise MetadataBackupError('Failed to restore metadata rows')
        row_ids.update({row['_id']: row_id for row, row_id in zip(batch, inserted_ids)})
    return row_ids


def _same_column(current, backup):
    return all(current.get(key) == backup.get(key) for key in ('key', 'name', 'type', 'data'))


def _batches(items):
    for index in range(0, len(items), PAGE_SIZE):
        yield items[index:index + PAGE_SIZE]
