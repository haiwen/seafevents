import logging

from sqlalchemy import select

from .models import ContentScanResult


# 这个函数从数据库中检索内容扫描结果的列表，支持可选的分页。它接受一个数据库会话并返回一个字典列表，每个字典代表一个内容扫描结果。结果按 `repo_id` 排序。如果提供了 `start` 和 `limit` ，函数返回结果列表的子集。
def get_content_scan_results(session, start=-1, limit=-1):
    ret = []
    try:
        stmt = select(ContentScanResult).order_by(ContentScanResult.repo_id)
        if start >= 0 and limit > 0:
            stmt = stmt.slice(start, start + limit)
        rows = session.scalars(stmt).all()
        for row in rows:
            d = row.__dict__
            d.pop('_sa_instance_state')
            d.pop('id')
            ret.append(d)
    except Exception as e:
        logging.warning('Failed to get content-scan results: %s.', e)
    finally:
        session.close()

    return ret
