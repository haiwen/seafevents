from sqlalchemy.sql import text


def get_mtime_by_repo_ids(session, repo_ids):
    with session() as session:
        if len(repo_ids) == 1:
            cmd = """SELECT repo_id, update_time FROM RepoInfo WHERE repo_id = '%s'""" % repo_ids[0]
        else:
            cmd = """SELECT repo_id, update_time FROM RepoInfo WHERE repo_id IN {}""".format(tuple(repo_ids))
        res = session.execute(text(cmd)).fetchall()
        return res


def get_face_recognition_enabled_repo_list(session, start, count):
    with session() as session:
        cmd = """SELECT repo_id, last_face_cluster_time FROM repo_metadata WHERE face_recognition_enabled = True limit :start, :count"""
        res = session.execute(text(cmd), {'start': start, 'count': count}).fetchall()

    return res


def update_face_cluster_time(session, repo_id, update_time):
    with session() as session:
        cmd = """UPDATE repo_metadata SET last_face_cluster_time = :update_time WHERE repo_id = :repo_id"""
        session.execute(text(cmd), {'update_time': update_time, 'repo_id': repo_id})
        session.commit()
