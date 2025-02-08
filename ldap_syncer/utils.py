import logging
import uuid

from sqlalchemy import select, delete

from seafevents.db import GroupIdLDAPUuidPair

logger = logging.getLogger(__name__)

# 转换 bytes 到 str
def bytes2str(data):
    if isinstance(data, bytes):
        try:
            return data.decode()
        except UnicodeDecodeError:
            return str(uuid.UUID(bytes=data))
    elif isinstance(data, dict):
        return dict(map(bytes2str, data.items()))
    elif isinstance(data, tuple):
        return tuple(map(bytes2str, data))
    elif isinstance(data, list):
        return list(map(bytes2str, data))
    elif isinstance(data, set):
        return set(map(bytes2str, data))
    else:
        return data


# 这个函数从数据库会话中检索所有 GroupIdLDAPUuidPair 对象，将每个对象转换为一个包含 group_id 和 group_uuid 键的字典，并返回这些字典的列表。
def get_group_uuid_pairs(session):
    q = session.scalars(select(GroupIdLDAPUuidPair)).all()
    res = []
    for item in q:
        data = dict()
        data['group_id'] = item.group_id
        data['group_uuid'] = item.group_uuid
        res.append(data)

    session.close()
    return res


# 这个函数添加一个新的 GroupIdLDAPUuidPair 对象到数据库会话中，表示一个新的组 ID 和 UUID 的映射关系。
def add_group_uuid_pair(session, group_id, group_uuid):
    res = session.scalars(select(GroupIdLDAPUuidPair).where(GroupIdLDAPUuidPair.group_id == group_id).limit(1)).first()
    if res:
        session.close()
        return

    new_pair = GroupIdLDAPUuidPair({'group_id': group_id, 'group_uuid': group_uuid})
    try:
        session.add(new_pair)
        session.commit()
    except Exception as e:
        logger.error('add group_id:group_uuid pair failed. \n{}'.format(e))
    finally:
        session.close()

# 移除一个组 ID 对应的 group_uuid
def remove_group_uuid_pair_by_id(session, group_id):
    try:
        stmt = delete(GroupIdLDAPUuidPair).where(GroupIdLDAPUuidPair.group_id == group_id)
        session.execute(stmt)
        session.commit()
    except Exception as e:
        logger.error('remote group_id:group_uuid pair failed. \n{}'.format(e))
    finally:
        session.close()

# 移除多个不再使用的组 ID 对应的 group_uuid
def remove_useless_group_uuid_pairs(session, group_ids):
    try:
        stmt = delete(GroupIdLDAPUuidPair).where(GroupIdLDAPUuidPair.group_id.not_in(group_ids)).\
            execution_options(synchronize_session=False)
        session.execute(stmt)
        session.commit()
    except Exception as e:
        logger.error('remote group_id:group_uuid pair failed. \n{}'.format(e))
    finally:
        session.close()
