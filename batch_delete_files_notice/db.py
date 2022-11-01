# -*- coding: utf-8 -*-
import datetime

from .models import DeletedFilesCount


def save_deleted_files_count(session, repo_id, files_count, deleted_time):
    count = DeletedFilesCount(repo_id, files_count, deleted_time)
    session.add(count)
    session.commit()


def get_deleted_files_total_count(session, repo_id, deleted_time):
    counts = session.query(DeletedFilesCount).filter(DeletedFilesCount.repo_id == repo_id).\
        filter(DeletedFilesCount.deleted_time == deleted_time)
    total_count = 0
    for count in counts:
        total_count += count.files_count

    return total_count


def delete_deleted_files_count(session, repo_id):
    today = datetime.date.today()

    session.query(DeletedFilesCount).filter(DeletedFilesCount.repo_id == repo_id).\
        filter(DeletedFilesCount.deleted_time == today).delete()
    session.commit()
