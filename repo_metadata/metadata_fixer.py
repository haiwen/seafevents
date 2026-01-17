"""
Metadata fixer module for fixing metadata consistency issues.
This module handles:
1. Scanning repository to get all objects (files and directories)
2. Comparing with existing metadata records
3. Deleting obsolete metadata records
4. Adding missing metadata records
"""
import hashlib
import logging
import posixpath
from sqlalchemy.sql import text

from seafobj import fs_mgr, commit_mgr
from seaserv import seafile_api

from seafevents.utils import timestamp_to_isoformat_timestr
from seafevents.repo_metadata.constants import METADATA_TABLE, METADATA_OP_LIMIT
from seafevents.repo_metadata.utils import gen_fileext_type_map, FILEEXT_TYPE_MAP

logger = logging.getLogger(__name__)

# Directories to skip during scanning
SKIP_DIRS = ['/images', '/_Internal']


def get_obj_path_hash(repo_id, path):
    """Generate MD5 hash of repo_id + path for unique identification."""
    return hashlib.md5((repo_id + path).encode()).hexdigest()


def get_repo_obj_infos(repo_id, commit_id):
    """
    Recursively scan the repository to get all objects info.
    
    Returns:
        dict: {
            'file_count': int,
            'dir_count': int,
            'items': {
                'md5_hash': {
                    'path': str,
                    'parent_dir': str,
                    'name': str,
                    'obj_id': str,
                    'size': int,
                    'mtime': int,
                    'modifier': str,
                    'is_dir': bool
                },
                ...
            }
        }
    """
    result = {
        'file_count': 0,
        'dir_count': 0,
        'items': {}
    }
    
    try:
        commit = commit_mgr.load_commit(repo_id, 1, commit_id)
        if not commit:
            logger.error(f'Failed to load commit {commit_id} for repo {repo_id}')
            return result
        
        root = fs_mgr.load_seafdir(repo_id, 1, commit.root_id)
        if not root:
            logger.error(f'Failed to load root dir for repo {repo_id}')
            return result
    except Exception as e:
        logger.error(f'Error loading repo {repo_id}: {e}')
        return result

    def scan_dir(seafdir, parent_path):
        """Recursively scan directory."""
        try:
            # Process files
            for entry in seafdir.get_files_list():
                file_path = posixpath.join(parent_path, entry.name)
                result['file_count'] += 1
                path_hash = get_obj_path_hash(repo_id, file_path)
                result['items'][path_hash] = {
                    'path': file_path,
                    'parent_dir': parent_path,
                    'name': entry.name,
                    'obj_id': entry.id,
                    'size': entry.size,
                    'mtime': entry.mtime,
                    'modifier': getattr(entry, 'modifier', ''),
                    'is_dir': False
                }
            
            # Process directories
            for entry in seafdir.get_subdirs_list():
                dir_name = entry.name
                dir_path = posixpath.join(parent_path, dir_name)
                
                # Skip specified directories
                if dir_path in SKIP_DIRS:
                    continue
                
                result['dir_count'] += 1
                path_hash = get_obj_path_hash(repo_id, dir_path)
                result['items'][path_hash] = {
                    'path': dir_path,
                    'parent_dir': parent_path,
                    'name': dir_name,
                    'obj_id': entry.id,
                    'mtime': getattr(entry, 'mtime', 0),
                    'modifier': '',
                    'is_dir': True
                }
                
                # Recursively scan subdirectory
                try:
                    subdir = fs_mgr.load_seafdir(repo_id, 1, entry.id)
                    if subdir:
                        scan_dir(subdir, dir_path)
                except Exception as e:
                    logger.error(f'Error scanning subdir {dir_path}: {e}')
        except Exception as e:
            logger.error(f'Error scanning directory {parent_path}: {e}')
    
    scan_dir(root, '/')
    return result


def build_metadata_row(item):
    """Build metadata row data from object info."""
    suffix = ''
    file_type = ''
    
    if not item['is_dir']:
        name = item['name']
        if '.' in name:
            suffix = name.rsplit('.', 1)[-1].lower()
            file_type = FILEEXT_TYPE_MAP.get(suffix, '')
    
    row = {
        METADATA_TABLE.columns.parent_dir.name: item['parent_dir'],
        METADATA_TABLE.columns.file_name.name: item['name'],
        METADATA_TABLE.columns.obj_id.name: item['obj_id'],
        METADATA_TABLE.columns.is_dir.name: item['is_dir'],
        METADATA_TABLE.columns.file_mtime.name: timestamp_to_isoformat_timestr(item['mtime']),
        METADATA_TABLE.columns.file_ctime.name: timestamp_to_isoformat_timestr(item['mtime']),
    }
    
    if not item['is_dir']:
        row[METADATA_TABLE.columns.size.name] = item.get('size', 0)
        row[METADATA_TABLE.columns.file_modifier.name] = item.get('modifier', '')
        row[METADATA_TABLE.columns.file_creator.name] = item.get('modifier', '')
        if suffix:
            row[METADATA_TABLE.columns.suffix.name] = suffix
        if file_type:
            row[METADATA_TABLE.columns.file_type.name] = file_type
    
    return row


def reset_is_fixing_status(repo_id, db_session_class):
    """Reset is_fixing status to False after fix completes."""
    try:
        with db_session_class() as session:
            sql = "UPDATE repo_metadata SET is_fixing = 0 WHERE repo_id = :repo_id"
            session.execute(text(sql), {'repo_id': repo_id})
            session.commit()
    except Exception as e:
        logger.error(f'Error resetting is_fixing status for repo {repo_id}: {e}')


def fix_repo_metadata(repo_id, metadata_server_api, db_session_class):
    """
    Main fix logic:
    1. Get repository object info
    2. Batch query metadata for comparison
    3. Delete obsolete metadata
    4. Add missing metadata
    5. Reset is_fixing status
    """
    logger.info(f'Starting metadata fix for repo {repo_id}')
    
    try:
        # Get current head commit
        repo = seafile_api.get_repo(repo_id)
        if not repo:
            logger.error(f'Repo {repo_id} not found')
            reset_is_fixing_status(repo_id, db_session_class)
            return
        
        commit_id = repo.head_cmmt_id
        
        # 1. Get repository object info
        repo_obj_infos = get_repo_obj_infos(repo_id, commit_id)
        
        logger.info(f'Repo {repo_id}: {repo_obj_infos["file_count"]} files, {repo_obj_infos["dir_count"]} dirs')
        
        # 2. Batch query metadata for comparison
        start = 0
        limit = 1000
        records_to_delete = []
        
        while True:
            sql = f'SELECT `{METADATA_TABLE.columns.id.name}`, `{METADATA_TABLE.columns.parent_dir.name}`, `{METADATA_TABLE.columns.file_name.name}` FROM `{METADATA_TABLE.name}` LIMIT {start}, {limit}'
            try:
                result = metadata_server_api.query_rows(repo_id, sql)
                rows = result.get('results', [])
            except Exception as e:
                logger.error(f'Error querying metadata for repo {repo_id}: {e}')
                break
            
            if not rows:
                break
            
            for row in rows:
                record_id = row.get(METADATA_TABLE.columns.id.name)
                parent_dir = row.get(METADATA_TABLE.columns.parent_dir.name, '/')
                filename = row.get(METADATA_TABLE.columns.file_name.name, '')
                
                # Construct object path
                if parent_dir == '/':
                    obj_path = '/' + filename
                else:
                    obj_path = posixpath.join(parent_dir, filename)
                
                path_hash = get_obj_path_hash(repo_id, obj_path)
                
                if path_hash in repo_obj_infos['items']:
                    # Match found, remove from items dict
                    repo_obj_infos['items'].pop(path_hash)
                else:
                    # Metadata exists but object doesn't exist in repo, mark for deletion
                    records_to_delete.append(record_id)
            
            if len(rows) < limit:
                break
            start += limit
        
        # 3. Batch delete obsolete metadata
        deleted_count = len(records_to_delete)
        if records_to_delete:
            logger.info(f'Deleting {deleted_count} obsolete metadata records for repo {repo_id}')
            for i in range(0, len(records_to_delete), METADATA_OP_LIMIT):
                batch = records_to_delete[i:i + METADATA_OP_LIMIT]
                try:
                    metadata_server_api.delete_rows(repo_id, METADATA_TABLE.id, batch)
                except Exception as e:
                    logger.error(f'Error deleting metadata batch for repo {repo_id}: {e}')
        
        # 4. Batch add missing metadata
        added_count = len(repo_obj_infos['items'])
        if repo_obj_infos['items']:
            logger.info(f'Adding {added_count} missing metadata records for repo {repo_id}')
            rows_to_add = []
            for path_hash, item in repo_obj_infos['items'].items():
                row = build_metadata_row(item)
                rows_to_add.append(row)
            
            for i in range(0, len(rows_to_add), METADATA_OP_LIMIT):
                batch = rows_to_add[i:i + METADATA_OP_LIMIT]
                try:
                    metadata_server_api.insert_rows(repo_id, METADATA_TABLE.id, batch)
                except Exception as e:
                    logger.error(f'Error inserting metadata batch for repo {repo_id}: {e}')
        
        # 5. Reset is_fixing status
        reset_is_fixing_status(repo_id, db_session_class)
        
        logger.info(f'Metadata fix completed for repo {repo_id}: deleted {deleted_count}, added {added_count} records')
    
    except Exception as e:
        logger.exception(f'Error during metadata fix for repo {repo_id}: {e}')
        reset_is_fixing_status(repo_id, db_session_class)
