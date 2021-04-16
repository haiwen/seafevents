
import logging
import uuid
import os

from seafevents.app.config import appconfig, load_config
from seafevents.db import GroupIdLDAPUuidPair

logger = logging.getLogger(__name__)


if not appconfig.get('session_cls'):
    if 'SEAFILE_CENTRAL_CONF_DIR' in os.environ:
        load_config(os.path.join(os.environ['SEAFILE_CENTRAL_CONF_DIR'], 'seafevents.conf'))


def bytes2str(data):
    if isinstance(data, bytes):
        try:
            return data.decode()
        except UnicodeDecodeError:
            return str(uuid.UUID(bytes=data))
    elif isinstance(data, dict):       return dict(map(bytes2str, data.items()))
    elif isinstance(data, tuple):      return tuple(map(bytes2str, data))
    elif isinstance(data, list):       return list(map(bytes2str, data))
    elif isinstance(data, set):        return set(map(bytes2str, data))
    else:
        return data


def get_group_uuid_pairs():
    session = appconfig.session_cls()
    q = session.query(GroupIdLDAPUuidPair)
    res = []
    for item in q:
        data = {}
        data['group_id'] = item.group_id
        data['group_uuid'] = item.group_uuid
        res.append(data)

    return res


def add_group_uuid_pair(group_id, group_uuid):
    session = appconfig.session_cls()

    res = session.query(GroupIdLDAPUuidPair).filter_by(group_id=group_id).first()
    if res:
        return

    new_pair = GroupIdLDAPUuidPair({'group_id':group_id, 'group_uuid':group_uuid})
    try:
        session.add(new_pair)
    except Exception as e:
        logger.error('add group_id:group_uuid pair failed. \n{}'.format(e))
        return

    session.commit()


def remove_group_uuid_pair_by_id(group_id):
    session = appconfig.session_cls()

    try:
        session.query(GroupIdLDAPUuidPair).filter_by(group_id=group_id).delete()
    except Exception as e:
        logger.error('remote group_id:group_uuid pair failed. \n{}'.format(e))
        return

    session.commit()


def remove_useless_group_uuid_pairs(group_ids):
    session = appconfig.session_cls()
    try:
        session.query(GroupIdLDAPUuidPair).filter(
            GroupIdLDAPUuidPair.group_id.not_in(group_ids)).delete(synchronize_session=False)
    except Exception as e:
        logger.error('remote group_id:group_uuid pair failed. \n{}'.format(e))
        return
    session.commit()
