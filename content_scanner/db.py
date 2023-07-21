import logging

from sqlalchemy import select

from .models import ContentScanResult


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
