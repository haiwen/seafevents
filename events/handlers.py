# coding: utf-8
import datetime
from datetime import timedelta

from sqlalchemy import desc, func, select

from .models import FileTrash


def get_delete_records(session, repo_id, show_day, start, limit):
    if show_day == 0:
        return [], 0
    elif show_day == -1:
        stmt = select(FileTrash).where(FileTrash.repo_id == repo_id)
        count_stmt = select(func.count(FileTrash.id)).where(FileTrash.repo_id == repo_id)
    else:
        timestamp = datetime.datetime.now() - timedelta(days=show_day)
        stmt = select(FileTrash).where(
            FileTrash.repo_id == repo_id,
            FileTrash.delete_time > timestamp,
        )
        count_stmt = select(func.count(FileTrash.id)).where(
            FileTrash.repo_id == repo_id,
            FileTrash.delete_time > timestamp,
        )

    stmt = stmt.order_by(desc(FileTrash.delete_time))
    total_count = session.scalar(count_stmt)
    stmt = stmt.slice(start, start + limit)
    records = session.scalars(stmt).all()

    return records, total_count
