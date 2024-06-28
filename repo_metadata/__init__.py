import logging

from seafevents.repo_data import repo_data
# from seafes.indexes import RepoStatusIndex, RepoFilesIndex
# from seafes.connection import es_get_conn

logger = logging.getLogger(__name__)

# 暂时不用
def clear_deleted_repos_metadata():
    repo_list = repo_data.get_all_repo_list()
    trash_repo_list = repo_data.get_all_trash_repo_list()
    repo_exist = [i['repo_id'] for i in repo_list]
    repo_exist.extend([i['repo_id'] for i in trash_repo_list])
    del repo_list
    del trash_repo_list

    # 删除数据库中的记录，删除md-server 中的数据

    try:
        status_index = RepoStatusIndex(es_get_conn())
        files_index = RepoFilesIndex(es_get_conn())
    except Exception as e:
        logger.error('Error:%s' % e)
        return
    repo_all = [r.get('id') for r in status_index.get_all_repos_from_index()]
    repo_deleted = set(repo_all) - set(repo_exist)
    logger.info("%d repos need to be deleted." % len(repo_deleted))
    for repo in repo_deleted:
        status_index.delete_repo(repo)
        files_index.delete_repo(repo)
        logger.info('repo %s has been removed' % repo)
    logger.info('Deleted repo removed success')