import logging

from .models import ContentScanResult
from seafevents.app.config import appconfig

def get_content_scan_results(start=-1, limit=-1):
    ret = []
    try:
        session = appconfig.session_cls()
    
        q = session.query(ContentScanResult).order_by(ContentScanResult.repo_id)
        if start >= 0 and limit > 0:
            q = q.slice(start, start + limit)
        rows = q.all()
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
    
