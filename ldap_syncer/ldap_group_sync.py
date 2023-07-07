# -*- coding: utf-8 -*-
import logging

from seaserv import get_ldap_groups, get_group_members, group_add_member, group_remove_member, \
        remove_group, get_super_users, ccnet_api, seafile_api
from ldap import SCOPE_SUBTREE, SCOPE_BASE, SCOPE_ONELEVEL
from .ldap_conn import LdapConn
from .ldap_sync import LdapSync
from .utils import bytes2str, add_group_uuid_pair, get_group_uuid_pairs, remove_group_uuid_pair_by_id, \
        remove_useless_group_uuid_pairs


logger = logging.getLogger('ldap_sync')
logger.setLevel(logging.DEBUG)


class LdapGroup(object):
    def __init__(self, cn, creator, members, parent_uuid=None, group_id=0, is_department=False):
        self.cn = cn
        self.creator = creator
        self.members = members
        self.parent_uuid = parent_uuid
        self.group_id = group_id
        self.is_department = is_department


class LdapGroupSync(LdapSync):
    def __init__(self, settings):
        LdapSync.__init__(self, settings)
        self.agroup = 0
        self.ugroup = 0
        self.dgroup = 0
        self.sort_list = []
        self.login_attr_email_map = dict()

    def show_sync_result(self):
        logger.info('LDAP group sync result: add [%d]group, update [%d]group, delete [%d]group' %
                     (self.agroup, self.ugroup, self.dgroup))

    def get_department_name(self, config, attrs, default_name):
        if config.department_name_attr != '' and config.department_name_attr in attrs and \
           attrs[config.department_name_attr] and \
           len(attrs[config.department_name_attr][0]) <= 255:
            department_name = attrs[config.department_name_attr][0]
        else:
            department_name = default_name
        return department_name

    def get_data_from_db(self):
        grp_data_db = None
        groups = get_ldap_groups(-1, -1)
        if groups is None:
            logger.warning('get ldap groups from db failed.')
            return grp_data_db

        # remove not exist group's uuid_pair
        group_ids = [int(group.id) for group in groups]
        session = self.settings.db_session()
        remove_useless_group_uuid_pairs(session, group_ids)

        providers = list()
        for config in self.settings.ldap_configs:
            providers.append(config.ldap_provider)
        try:
            self.cursor.execute("SELECT username,uid FROM social_auth_usersocialauth WHERE `provider` IN %s",
                                [providers])
            ldap_users = self.cursor.fetchall()
        except Exception as e:
            logger.error('get ldap users from db failed: %s' % e)
            return grp_data_db

        # get login_attr email map
        for user in ldap_users:
            self.login_attr_email_map[user[1]] = user[0]

        grp_data_db = {}
        for group in groups:
            members = get_group_members(group.id)
            if members is None:
                logger.warning('get members of group %d from db failed.' %
                                group.id)
                grp_data_db = None
                break

            nor_members = []
            for member in members:
                nor_members.append(member.user_name)

            if (group.parent_group_id == 0):
                grp_data_db[group.id] = LdapGroup(group.group_name, group.creator_name, sorted(nor_members))
            else:
                grp_data_db[group.id] = LdapGroup(group.group_name, group.creator_name, sorted(nor_members), None, 0, True)

        return grp_data_db

    def get_data_from_ldap_by_server(self, config):
        ldap_conn = LdapConn(config.host, config.user_dn, config.passwd, config.follow_referrals)
        ldap_conn.create_conn()
        if not ldap_conn.conn:
            return None

        # these three variables are dict of {group dn: LdapGroup} pairs
        ret_data_ldap = {}
        department_data_ldap = {}
        group_data_ldap = {}

        # search all groups on base dn

        if config.sync_department_from_ou:
            department_data_ldap = self.get_ou_data(ldap_conn, config)

        if config.enable_group_sync:
            if config.group_object_class == 'posixGroup':
                group_data_ldap = self.get_posix_group_data(ldap_conn, config)
            else:
                group_data_ldap = self.get_common_group_data(ldap_conn, config)

        ret_data_ldap = department_data_ldap.copy()
        ret_data_ldap.update(group_data_ldap)

        ldap_conn.unbind_conn()

        return ret_data_ldap

    def get_common_group_data(self, ldap_conn, config):
        grp_data_ldap = {}

        if config.group_filter != '':
            search_filter = '(&(objectClass=%s)(%s))' % \
                             (config.group_object_class,
                              config.group_filter)
        else:
            search_filter = '(objectClass=%s)' % config.group_object_class

        sort_list = []
        base_dns = config.base_dn.split(';')
        for base_dn in base_dns:
            if base_dn == '':
                continue
            results = None
            scope = SCOPE_SUBTREE
            if config.use_page_result:
                results = ldap_conn.paged_search(base_dn, scope,
                                                search_filter,
                                                [config.group_member_attr, 'cn'])
            else:
                results = ldap_conn.search(base_dn, scope,
                                          search_filter,
                                          [config.group_member_attr, 'cn'])
            if not results:
                continue
            results = bytes2str(results)

            for result in results:
                group_dn, attrs = result
                if not isinstance(attrs, dict):
                    continue
                self.get_group_member_from_ldap(config, ldap_conn, group_dn, grp_data_ldap, sort_list, parent_uuid=None, depth=1)

        self.sort_list.extend(list(grp_data_ldap.items()))

        return grp_data_ldap

    def get_group_member_from_ldap(self, config, ldap_conn, base_dn, grp_data, sort_list, parent_uuid, depth=1):
        if depth > 50:
            logger.error('50 recursion depth exceeded, this group is unusual.')
            return
        depth += 1

        all_mails = []
        search_filter = '(|(objectClass=%s)(objectClass=%s))' % \
                         (config.group_object_class,
                          config.user_object_class)
        result = ldap_conn.search(base_dn, SCOPE_BASE, search_filter,
                                  [config.login_attr, 'cn',
                                   config.group_uuid_attr,
                                   config.department_name_attr])
        if not result:
            return []
        result = bytes2str(result)

        dn, attrs = result[0]
        if not isinstance(attrs, dict):
            return all_mails

        group_uuid = attrs[config.group_uuid_attr][0]
        if group_uuid in grp_data:
            if not grp_data[group_uuid].parent_uuid:
                grp_data[group_uuid].parent_uuid = parent_uuid
            return grp_data[group_uuid].members

        if config.use_group_member_range_query:
            # with range search, attrs' key will also become dirty
            # e.g. attrs = {'member;range=0-99': [...], ...}
            # so after all search, we add a new k-v pair to fix this problem
            # notice that last attr member key is like member;range=200-*, last character is *
            range = 100
            count = 0
            all_members = []
            while True:
                start = count * range + 1
                end = start + range - 1
                count += 1
                member_attr_key_with_range = config.group_member_attr + ';range={}-{}'.format(start-1, end-1)
                member_attr_key_with_range_last_one = member_attr_key_with_range.split('-')[0] + '-*'
                result = ldap_conn.search(base_dn, SCOPE_BASE, search_filter,
                                        [member_attr_key_with_range])
                result = bytes2str(result)
                current_loop_attrs = result[0][1]

                if member_attr_key_with_range in current_loop_attrs:
                    all_members += current_loop_attrs[member_attr_key_with_range]
                elif member_attr_key_with_range_last_one in current_loop_attrs:

                    all_members += current_loop_attrs[member_attr_key_with_range_last_one]
                    break
                else:
                    break

            # only group member need add member list if attrs
            # all_members == [], means it's a user member
            if all_members != []:
                attrs[config.group_member_attr] = all_members
        else:
            result = ldap_conn.search(base_dn, SCOPE_BASE, search_filter,
                                      [config.group_member_attr])
            result = bytes2str(result)
            if config.group_member_attr in result[0][1]:
                attrs[config.group_member_attr] = result[0][1][config.group_member_attr]

        # group member
        if config.group_member_attr in attrs and attrs[config.group_member_attr] != ['']:
            for member in attrs[config.group_member_attr]:
                mails = self.get_group_member_from_ldap(config, ldap_conn, member, grp_data, sort_list, group_uuid, depth)
                if not mails:
                    continue
                all_mails.extend(mails)
        # user member
        elif config.login_attr in attrs:
            for mail in attrs[config.login_attr]:
                all_mails.append(mail.lower())
                return all_mails

        if config.sync_group_as_department:
            name = self.get_department_name(config, attrs, attrs['cn'][0])
        else:
            name = attrs['cn'][0]
        grp_data[group_uuid] = LdapGroup(name, None, sorted(set(all_mails)), parent_uuid, 0, config.sync_group_as_department)

        return all_mails

    def get_posix_group_data(self, ldap_conn, config):
        grp_data_ldap = {}

        if config.group_filter != '':
            search_filter = '(&(objectClass=%s)(%s))' % \
                             (config.group_object_class,
                             config.group_filter)
        else:
            search_filter = '(objectClass=%s)' % config.group_object_class

        sort_list = []
        base_dns = config.base_dn.split(';')
        for base_dn in base_dns:
            if base_dn == '':
                continue
            results = None
            if config.use_page_result:
                results = ldap_conn.paged_search(base_dn, SCOPE_SUBTREE,
                                                search_filter,
                                                [config.group_member_attr, 'cn', config.group_uuid_attr,
                                                 config.department_name_attr])
            else:
                results = ldap_conn.search(base_dn, SCOPE_SUBTREE,
                                          search_filter,
                                          [config.group_member_attr, 'cn', config.group_uuid_attr,
                                           config.department_name_attr])
            if not results:
                continue
            results = bytes2str(results)

            for result in results:
                group_dn, attrs = result
                if not isinstance(attrs, dict):
                    continue
                group_uuid = attrs[config.group_uuid_attr][0]

                # empty group
                if config.group_member_attr not in attrs:
                    grp_data_ldap[group_uuid] = LdapGroup(attrs['cn'][0], None, [], None, 0, config.sync_group_as_department)
                    continue
                if group_uuid in grp_data_ldap:
                    if config.sync_group_as_department:
                        name = self.get_department_name(config, attrs, attrs['cn'][0])
                    else:
                        name = attrs['cn'][0]
                    grp_data_ldap[group_uuid] = LdapGroup(name, None, [], None, 0, config.sync_group_as_department)
                    continue
                all_mails = []
                for member in attrs[config.group_member_attr]:
                    mails = self.get_posix_group_member_from_ldap(config, ldap_conn, base_dn, member)
                    if not mails:
                        continue
                    all_mails.extend(mails)

                grp_data_ldap[group_uuid] = LdapGroup(attrs['cn'][0], None,
                                                    sorted(set(all_mails)), None, 0, config.sync_group_as_department)
                sort_list.append((group_uuid, grp_data_ldap[group_uuid]))

        self.sort_list.extend(sort_list)
        return grp_data_ldap

    def get_posix_group_member_from_ldap(self, config, ldap_conn, base_dn, member):
        all_mails = []
        search_filter = '(&(objectClass=%s)(%s=%s))' % \
                        (config.user_object_class,
                         config.user_attr_in_memberUid,
                         member)

        results = ldap_conn.search(base_dn, SCOPE_SUBTREE,
                                   search_filter,
                                   [config.login_attr, 'cn'])
        results = bytes2str(results)
        if not results:
            return []

        for result in results:
            dn, attrs = result
            if not isinstance(attrs, dict):
                continue
            if config.login_attr in attrs:
                for mail in attrs[config.login_attr]:
                    all_mails.append(mail.lower())

        return all_mails

    def get_ou_data(self, ldap_conn, config):
        if config.group_filter != '':
            search_filter = '(&(|(objectClass=organizationalUnit)(objectClass=%s))(%s))' % \
                             (config.user_object_class,
                              config.group_filter)
        else:
            search_filter = '(|(objectClass=organizationalUnit)(objectClass=%s))' % config.user_object_class

        base_dns = config.base_dn.split(';')
        sort_list = []
        grp_data_ou={}
        for base_dn in base_dns:
            if base_dn == '':
                continue
            s_idx = base_dn.find('=') + 1
            e_idx = base_dn.find(',')
            if e_idx == -1:
                e_idx = len(base_dn)
            ou_name = base_dn[s_idx:e_idx]

            result = ldap_conn.search(base_dn, SCOPE_BASE,
                                      search_filter,
                                      ['ou', config.group_uuid_attr, config.department_name_attr])
            result = bytes2str(result)
            dn, attrs = result[0]
            group_uuid = attrs[config.group_uuid_attr][0]

            if not result:
                name = ou_name
            else:
                dn, attrs = result[0]
                if 'ou' in attrs:
                    name = self.get_department_name (config, attrs, attrs['ou'][0])
                else:
                    name = ou_name

            self.get_ou_member (config, ldap_conn, base_dn, search_filter, sort_list, name, group_uuid, None, grp_data_ou)
        sort_list.reverse()
        self.sort_list.extend(sort_list)

        return grp_data_ou

    def get_ou_member(self, config, ldap_conn, base_dn, search_filter, sort_list, ou_name, group_uuid, parent_uuid, grp_data_ou):
        if config.use_page_result:
            results = ldap_conn.paged_search(base_dn, SCOPE_ONELEVEL,
                                             search_filter,
                                             [config.login_attr, 'ou', config.group_uuid_attr,
                                              config.department_name_attr])
        else:
            results = ldap_conn.search(base_dn, SCOPE_ONELEVEL,
                                       search_filter,
                                       [config.login_attr, 'ou', config.group_uuid_attr,
                                        config.department_name_attr])
        results = bytes2str(results)
        # empty ou
        if not results:
            group = LdapGroup(ou_name, None, [], parent_uuid, 0, True)
            sort_list.append((group_uuid, group))
            grp_data_ou[group_uuid] = group
            return
        results = bytes2str(results)

        mails = []
        member_dn=''
        for pair in results:
            member_dn, attrs = pair
            if not isinstance(attrs, dict):
                continue
            # member
            if config.login_attr in attrs and ('ou=' in base_dn or 'OU=' in base_dn):
                mails.append(attrs[config.login_attr][0].lower())
                continue
            # ou
            if 'ou' in attrs:
                name = self.get_department_name (config, attrs, attrs['ou'][0])
                this_group_uuid = attrs[config.group_uuid_attr][0]
                self.get_ou_member (config, ldap_conn, member_dn, search_filter,
                                    sort_list, name,
                                    this_group_uuid,
                                    group_uuid,
                                    grp_data_ou)

        group = LdapGroup(ou_name, None, sorted(set(mails)), parent_uuid, 0, True)
        sort_list.append((group_uuid, group))
        grp_data_ou[group_uuid] = group

        return grp_data_ou


    def create_and_add_group_to_db(self, group_uuid, group, grp_uuid_pairs, grp_data_ldap):
        if group.is_department and group_uuid in grp_uuid_pairs:
            return grp_uuid_pairs[group_uuid]

        super_user= None
        if group.is_department:
            super_user = 'system admin'
        else:
            super_user = LdapGroupSync.get_super_user()

        parent_id = 0
        if not group.is_department:
            parent_id = 0
        else:
            if not group.parent_uuid:
                parent_id = -1
            elif group.parent_uuid in grp_uuid_pairs:
                parent_id =  grp_uuid_pairs[group.parent_uuid]
            else:
                parent_group = grp_data_ldap[group.parent_uuid]
                parent_id = self.create_and_add_group_to_db (group.parent_uuid, parent_group, grp_uuid_pairs, grp_data_ldap)

        try:
            group_id = ccnet_api.create_group(group.cn, super_user, 'LDAP', parent_id)
        except Exception as e:
            logger.warning('create ldap group [%s] failed. Error: %s' % (group.cn, e))
            return

        if group_id < 0:
            logger.warning('create ldap group [%s] failed.' % group.cn)
            return

        session = self.settings.db_session()
        try:
            add_group_uuid_pair(session, group_id, group_uuid)
        except Exception:
            logger.warning('add group uuid pair %d<->%s failed.' % (group_id, group_uuid))
            # admin should remove created group manually in web
            return
        logger.debug('create group %d, and add uuid pair %s<->%d success.' %
                      (group_id, group_uuid, group_id))
        self.agroup += 1
        group.group_id = group_id
        if group.is_department:
            if group.config.default_department_quota > 0:
                quota_to_set = group.config.default_department_quota * 1000000
            else:
                quota_to_set = group.config.default_department_quota
            ret = seafile_api.set_group_quota(group_id, quota_to_set)
            if ret < 0:
                logger.warning('Failed to set group [%s] quota.' % group.cn)
            if group.config.create_department_library:
                ret = seafile_api.add_group_owned_repo(group_id, group.cn,
                                                       group.config.department_repo_permission)
                if not ret:
                    logger.warning('Failed to create group owned repo for %s.' % group.cn)

        for member in group.members:
            ret = group_add_member(group_id, super_user, member)
            if ret < 0:
                logger.warning('add member %s to group %d failed.' %
                                (member, group_id))
                return
            logger.debug('add member %s to group %d success.' %
                          (member, group_id))

        grp_uuid_pairs[group_uuid] = group_id

        return group_id

    def sync_data(self, data_db, data_ldap):
        session = self.settings.db_session()
        uuid_pairs = get_group_uuid_pairs(session)
        if uuid_pairs is None:
            logger.warning('get group uuid pairs from db failed.')
            return

        grp_uuid_pairs = {}
        group_uuid_db = {}

        for pair in uuid_pairs:
            grp_uuid_pairs[pair['group_uuid']] = pair['group_id']
            group_uuid_db[pair['group_uuid']] = pair['group_id']

        # sync deleted group in ldap to db
        # delete in reversed group id order, first delete sub department, then delete parent department
        reversed_dict_by_grp_id = {k: v for k, v in reversed(sorted(grp_uuid_pairs.items(), key=lambda item: item[1]))}
        for k in reversed_dict_by_grp_id:
            if grp_uuid_pairs[k] in data_db and k not in data_ldap:
                deleted_group_id = grp_uuid_pairs[k]
                if (not data_db[deleted_group_id].is_department and self.settings.del_group_if_not_found) or \
                   (data_db[deleted_group_id].is_department and self.settings.del_department_if_not_found):
                    group_uuid_db.pop(k)
                    ret = remove_group(grp_uuid_pairs[k], '')
                    session = self.settings.db_session()
                    remove_group_uuid_pair_by_id(session, grp_uuid_pairs[k])
                    if ret < 0:
                        logger.warning('remove group %d failed.' % grp_uuid_pairs[k])
                        continue
                    logger.debug('remove group %d success.' % grp_uuid_pairs[k])
                    self.dgroup += 1

        # sync undeleted group in ldap to db
        super_user = None

        # ldap_tups = [('uuid', LdapGroup)...]
        ldap_tups = self.sort_list

        for k, v in ldap_tups:
            if k in grp_uuid_pairs:
                v.group_id = grp_uuid_pairs[k]
                # group data lost in db
                if grp_uuid_pairs[k] not in data_db:
                    continue

                group_id = grp_uuid_pairs[k]
                # update group name
                if v.cn != data_db[group_id].cn:
                    ret = ccnet_api.set_group_name(group_id, v.cn)
                    if ret < 0:
                        logger.warning('rename group %d failed.' % group_id)
                        continue
                    logger.debug('rename group %d success.' % group_id)

                # Convert login_attr of group members in LDAP to email of Seafile
                # according to uid(LDAP_LOGIN_ATTR) in social_auth_usersocialauth
                email_list = list()
                for login_attr in v.members:
                    email = self.login_attr_email_map.get(login_attr, '')
                    if email:
                        email_list.append(email)
                v.members = sorted(email_list)

                add_list, del_list = LdapGroupSync.diff_members(data_db[group_id].members,
                                                                v.members)
                if len(add_list) > 0 or len(del_list) > 0:
                    self.ugroup += 1

                for member in del_list:
                    ret = group_remove_member(group_id, data_db[group_id].creator, member)
                    if ret < 0:
                        logger.warning('remove member %s from group %d failed.' %
                                        (member, group_id))
                        continue
                    logger.debug('remove member %s from group %d success.' %
                                  (member, group_id))

                for member in add_list:
                    ret = group_add_member(group_id, data_db[group_id].creator, member)
                    if ret < 0:
                        logger.warning('add member %s to group %d failed.' %
                                        (member, group_id))
                        continue
                    logger.debug('add member %s to group %d success.' %
                                  (member, group_id))
            else:
                self.create_and_add_group_to_db(k, v, group_uuid_db, data_ldap)

        self.close_seahub_db()

    @staticmethod
    def get_super_user():
        super_users = get_super_users()
        if super_users is None or len(super_users) == 0:
            super_user = 'system admin'
        else:
            super_user = super_users[0].email
        return super_user

    @staticmethod
    def diff_members(members_db, members_ldap):
        i = 0
        j = 0
        dlen = len(members_db)
        llen = len(members_ldap)
        add_list = []
        del_list = []

        while i < dlen and j < llen:
            if members_db[i] == members_ldap[j]:
                i += 1
                j += 1
            elif members_db[i] > members_ldap[j]:
                add_list.append(members_ldap[j])
                j += 1
            else:
                del_list.append(members_db[i])
                i += 1

        del_list.extend(members_db[i:])
        add_list.extend(members_ldap[j:])

        return add_list, del_list
