#coding: utf-8

import logging
logger = logging.getLogger('ldap_sync')
logger.setLevel(logging.DEBUG)

from seaserv import get_ldap_groups, get_group_members, add_group_dn_pair, \
        get_group_dn_pairs, create_group, group_add_member, group_remove_member, \
        get_group, remove_group, get_super_users, ccnet_api, seafile_api
from ldap import SCOPE_SUBTREE, SCOPE_BASE, SCOPE_ONELEVEL
from .ldap_conn import LdapConn
from .ldap_sync import LdapSync

class LdapGroup(object):
    def __init__(self, cn, creator, members, parent_dn=None, group_id=0, is_department=False):
        self.cn = cn
        self.creator = creator
        self.members = members
        self.parent_dn = parent_dn
        self.group_id = group_id
        self.is_department = is_department

class LdapGroupSync(LdapSync):
    def __init__(self, settings):
        LdapSync.__init__(self, settings)
        self.agroup = 0
        self.ugroup = 0
        self.dgroup = 0
        self.sort_list = []

    def show_sync_result(self):
        logger.info('LDAP group sync result: add [%d]group, update [%d]group, delete [%d]group' %
                     (self.agroup, self.ugroup, self.dgroup))

    def get_data_from_db(self):
        grp_data_db = None
        groups = get_ldap_groups(-1, -1)
        if groups is None:
            logger.warning('get ldap groups from db failed.')
            return grp_data_db

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
                grp_data_db[group.id] = LdapGroup(None, group.creator_name, sorted(nor_members))
            else:
                grp_data_db[group.id] = LdapGroup(None, group.creator_name, sorted(nor_members), None, 0, True)

        return grp_data_db

    def get_data_from_ldap_by_server(self, config):
        ldap_conn = LdapConn(config.host, config.user_dn, config.passwd, config.follow_referrals)
        ldap_conn.create_conn()
        if not ldap_conn.conn:
            return None

        # group dn <-> LdapGroup
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

            for result in results:
                group_dn, attrs = result
                if not isinstance(attrs, dict):
                    continue
                self.get_group_member_from_ldap(config, ldap_conn, group_dn, grp_data_ldap, sort_list, None)

        self.sort_list.extend(list(grp_data_ldap.items()))

        return grp_data_ldap

    def get_group_member_from_ldap(self, config, ldap_conn, base_dn, grp_data, sort_list, parent_dn):
        if base_dn in grp_data:
            if not grp_data[base_dn].parent_dn:
                grp_data[base_dn].parent_dn = parent_dn
            return grp_data[base_dn].members

        all_mails = []
        search_filter = '(|(objectClass=%s)(objectClass=%s))' % \
                         (config.group_object_class,
                          config.user_object_class)
        result = ldap_conn.search(base_dn, SCOPE_BASE, search_filter,
                                  [config.group_member_attr,
                                   config.login_attr, 'cn'])
        if not result:
            return []

        dn, attrs = result[0]
        if not isinstance(attrs, dict):
            return all_mails
        # group member
        if config.group_member_attr in attrs and attrs[config.group_member_attr] != ['']:
            for member in attrs[config.group_member_attr]:
                mails = self.get_group_member_from_ldap(config, ldap_conn, member, grp_data, sort_list, base_dn)
                if not mails:
                    continue
                all_mails.extend(mails)
        # user member
        elif config.login_attr in attrs:
            for mail in attrs[config.login_attr]:
                all_mails.append(mail.lower())
                return all_mails

        grp_data[dn] = LdapGroup(attrs['cn'][0], None, sorted(set(all_mails)), parent_dn, 0, config.sync_group_as_department)

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
                                                [config.group_member_attr, 'cn'])
            else:
                results = ldap_conn.search(base_dn, SCOPE_SUBTREE,
                                          search_filter,
                                          [config.group_member_attr, 'cn'])
            if not results:
                continue

            for result in results:
                group_dn, attrs = result
                if not isinstance(attrs, dict):
                    continue
                # empty group
                if config.group_member_attr not in attrs:
                    grp_data_ldap[group_dn] = LdapGroup(attrs['cn'][0], None, [], None, 0, config.sync_group_as_department)
                    continue
                if group_dn in grp_data_ldap:
                    continue
                all_mails = []
                for member in attrs[config.group_member_attr]:
                    mails = self.get_posix_group_member_from_ldap(config, ldap_conn, base_dn, member)
                    if not mails:
                        continue
                    all_mails.extend(mails)

                grp_data_ldap[group_dn] = LdapGroup(attrs['cn'][0], None,
                                                    sorted(set(all_mails)), None, 0, config.sync_group_as_department)
                sort_list.append((group_dn, grp_data_ldap[group_dn]))

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
            self.get_ou_member (config, ldap_conn, base_dn, search_filter, sort_list, ou_name, None, grp_data_ou)
        sort_list.reverse()
        self.sort_list.extend(sort_list)

        return grp_data_ou

    def get_ou_member(self, config, ldap_conn, base_dn, search_filter, sort_list, ou_name, parent_dn, grp_data_ou):
        if config.use_page_result:
            results = ldap_conn.paged_search(base_dn, SCOPE_ONELEVEL,
                                             search_filter,
                                             [config.login_attr, 'ou'])
        else:
            results = ldap_conn.search(base_dn, SCOPE_ONELEVEL,
                                       search_filter,
                                       [config.login_attr, 'ou'])
        # empty ou
        if not results:
            group = LdapGroup(ou_name, None, [], parent_dn, 0, True)
            sort_list.append((base_dn, group))
            grp_data_ou[base_dn] = group
            return

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
                self.get_ou_member (config, ldap_conn, member_dn, search_filter,
                                    sort_list, attrs['ou'][0],
                                    base_dn,
                                    grp_data_ou)

        group = LdapGroup(ou_name, None, sorted(set(mails)), parent_dn, 0, True)
        sort_list.append((base_dn, group))
        grp_data_ou[base_dn] = group

        return grp_data_ou

    def create_and_add_group_to_db(self, dn_name, group, group_dn_db, group_data_ldap):
        if group.is_department and dn_name in group_dn_db:
           return group_dn_db[dn_name]

        super_user= None
        if group.is_department:
            super_user = 'system admin'
        else:
            super_user = LdapGroupSync.get_super_user()

        parent_id = 0
        if not group.is_department:
            parent_id = 0
        else:
            if not group.parent_dn:
                parent_id = -1
            elif group.parent_dn in group_dn_db:
                parent_id =  group_dn_db[group.parent_dn]
            else:
                parent_group = group_data_ldap[group.parent_dn]
                parent_id = self.create_and_add_group_to_db (group.parent_dn, parent_group, group_dn_db, group_data_ldap)

        group_id = ccnet_api.create_group(group.cn, super_user, 'LDAP', parent_id)
        if group_id < 0:
            logger.warning('create ldap group [%s] failed.' % group.cn)
            return

        ret = add_group_dn_pair(group_id, dn_name)
        if ret < 0:
            logger.warning('add group dn pair %d<->%s failed.' % (group_id, dn_name))
            # admin should remove created group manually in web
            return
        logger.debug('create group %d, and add dn pair %s<->%d success.' %
                      (group_id, dn_name, group_id))
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
                ret = seafile_api.add_group_owned_repo(group_id, group.cn, 'rw')
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

        group_dn_db[dn_name] = group_id

        return group_id

    def sync_data(self, data_db, data_ldap):
        dn_pairs = get_group_dn_pairs()
        if dn_pairs is None:
            logger.warning('get group dn pairs from db failed.')
            return

        # grp_dn_pairs['dn_name'] = group_id
        grp_dn_pairs = {}
        # grp_dn_db['dn_name'] = group_id
        group_dn_db = {}

        for grp_dn in dn_pairs:
            grp_dn_pairs[grp_dn.dn.encode('utf-8')] = grp_dn.group_id
            group_dn_db[grp_dn.dn.encode('utf-8')] = grp_dn.group_id

        # sync deleted group in ldap to db
        for k in grp_dn_pairs.keys():
            if k not in data_ldap:
                deleted_group_id = grp_dn_pairs[k]
                if (not data_db[deleted_group_id].is_department and self.settings.del_group_if_not_found) or \
                   (data_db[deleted_group_id].is_department and self.settings.del_department_if_not_found):
                    grp_dn_db.pop(k)
                    ret = remove_group(grp_dn_pairs[k], '')
                    if ret < 0:
                        logger.warning('remove group %d failed.' % grp_dn_pairs[k])
                        continue
                    logger.debug('remove group %d success.' % grp_dn_pairs[k])
                    self.dgroup += 1

        # sync undeleted group in ldap to db
        super_user = None

        # ldap_tups = [('dn_name', LdapGroup)...]
        ldap_tups = self.sort_list

        for k, v in ldap_tups:
            if k in grp_dn_pairs:
                v.group_id = grp_dn_pairs[k]
                # group data lost in db
                if grp_dn_pairs[k] not in data_db:
                    continue
                group_id = grp_dn_pairs[k]
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
                self.create_and_add_group_to_db(k, v, group_dn_db, data_ldap)

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
