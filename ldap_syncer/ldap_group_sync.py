#coding: utf-8

import logging

from seaserv import get_ldap_groups, get_group_members, add_group_dn_pair, \
        get_group_dn_pairs, create_group, group_add_member, group_remove_member, \
        remove_group, get_super_users, ccnet_api, seafile_api
from ldap import SCOPE_SUBTREE, SCOPE_BASE, SCOPE_ONELEVEL
from ldap_conn import LdapConn
from ldap_sync import LdapSync

class LdapGroup(object):
    def __init__(self, cn, creator, members, parent_dn=None, group_id=0):
        self.cn = cn
        self.creator = creator
        self.members = members
        self.parent_dn = parent_dn
        self.group_id = group_id

class LdapGroupSync(LdapSync):
    def __init__(self, settings):
        LdapSync.__init__(self, settings)
        self.agroup = 0
        self.ugroup = 0
        self.dgroup = 0

    def show_sync_result(self):
        logging.info('LDAP group sync result: add [%d]group, update [%d]group, delete [%d]group' %
                     (self.agroup, self.ugroup, self.dgroup))

    def get_data_from_db(self):
        grp_data_db = None
        groups = get_ldap_groups(-1, -1)
        if groups is None:
            logging.warning('get ldap groups from db failed.')
            return grp_data_db

        grp_data_db = {}
        for group in groups:
            members = get_group_members(group.id)
            if members is None:
                logging.warning('get members of group %d from db failed.' %
                                group.id)
                grp_data_db = None
                break

            nor_members = []
            for member in members:
                nor_members.append(member.user_name)

            grp_data_db[group.id] = LdapGroup(None, group.creator_name, sorted(nor_members))

        return grp_data_db

    def get_data_from_ldap_by_server(self, config):
        ldap_conn = LdapConn(config.host, config.user_dn, config.passwd, self.settings.follow_referrals)
        ldap_conn.create_conn()
        self.config = config
        if not ldap_conn.conn:
            return None

        # group dn <-> LdapGroup
        grp_data_ldap = {}
        # search all groups on base dn

        if self.settings.group_by_ou:
            grp_data_ldap = self.get_ou_data(ldap_conn, config)
        elif self.settings.group_object_class == 'posixGroup':
            grp_data_ldap = self.get_posix_group_data(ldap_conn, config)
        else:
            grp_data_ldap = self.get_common_group_data(ldap_conn, config)

        ldap_conn.unbind_conn()

        return grp_data_ldap

    def get_common_group_data(self, ldap_conn, config):
        grp_data_ldap = {}

        # Importing structure from ldap group is not supported for now.
        self.settings.import_group_structure = False

        if config.group_filter != '':
            search_filter = '(&(objectClass=%s)(%s))' % \
                             (self.settings.group_object_class,
                              config.group_filter)
        else:
            search_filter = '(objectClass=%s)' % self.settings.group_object_class

        sort_list = []
        base_dns = config.base_dn.split(';')
        for base_dn in base_dns:
            if base_dn == '':
                continue
            results = None
            scope = SCOPE_ONELEVEL if self.settings.import_group_structure else SCOPE_SUBTREE
            if self.settings.use_page_result:
                results = ldap_conn.paged_search(base_dn, scope,
                                                search_filter,
                                                [self.settings.group_member_attr, 'cn'])
            else:
                results = ldap_conn.search(base_dn, scope,
                                          search_filter,
                                          [self.settings.group_member_attr, 'cn'])
            if not results:
                continue

            for result in results:
                group_dn, attrs = result
                if type(attrs) != dict:
                    continue
                # empty group
                if not attrs.has_key(self.settings.group_member_attr):
                    grp_data_ldap[group_dn] = LdapGroup(attrs['cn'][0], None, [])
                    sort_list.append((group_dn, grp_data_ldap[group_dn]))
                    continue
                if grp_data_ldap.has_key(group_dn):
                    continue
                self.get_group_member_from_ldap(ldap_conn, group_dn, grp_data_ldap, sort_list, None)
        sort_list.reverse()
        self.sort_list = sort_list

        return grp_data_ldap

    def get_group_member_from_ldap(self, ldap_conn, base_dn, grp_data, sort_list, parent_dn):
        all_mails = []
        search_filter = '(|(objectClass=%s)(objectClass=%s))' % \
                         (self.settings.group_object_class,
                          self.settings.user_object_class)
        result = ldap_conn.search(base_dn, SCOPE_BASE, search_filter,
                                  [self.settings.group_member_attr,
                                   self.settings.login_attr, 'cn'])
        if not result:
            return []

        dn, attrs = result[0]
        if type(attrs) != dict:
            return all_mails
        # group member
        if attrs.has_key(self.settings.group_member_attr):
            if grp_data.has_key(dn):
                return None
            for member in attrs[self.settings.group_member_attr]:
                mails = self.get_group_member_from_ldap(ldap_conn, member, grp_data, sort_list, base_dn)
                if not mails:
                    continue
                for mail in mails:
                    all_mails.append(mail.lower())
            grp_data[dn] = LdapGroup(attrs['cn'][0], None, sorted(set(all_mails)), parent_dn)
            sort_list.append((dn, grp_data[dn]))
            if self.settings.import_group_structure:
                return []
            else:
                return all_mails
        # user member
        elif attrs.has_key(self.settings.login_attr):
            for mail in attrs[self.settings.login_attr]:
                all_mails.append(mail.lower())

        return all_mails

    def get_posix_group_data(self, ldap_conn, config):
        grp_data_ldap = {}

        # Importing structure from ldap group is not supported for now.
        self.settings.import_group_structure = False

        if config.group_filter != '':
            search_filter = '(&(objectClass=%s)(%s))' % \
                             (self.settings.group_object_class,
                             config.group_filter)
        else:
            search_filter = '(objectClass=%s)' % self.settings.group_object_class

        base_dns = config.base_dn.split(';')
        for base_dn in base_dns:
            if base_dn == '':
                continue
            results = None
            if self.settings.use_page_result:
                results = ldap_conn.paged_search(base_dn, SCOPE_SUBTREE,
                                                search_filter,
                                                [self.settings.group_member_attr, 'cn'])
            else:
                results = ldap_conn.search(base_dn, SCOPE_SUBTREE,
                                          search_filter,
                                          [self.settings.group_member_attr, 'cn'])
            if not results:
                continue

            for result in results:
                group_dn, attrs = result
                if type(attrs) != dict:
                    continue
                # empty group
                if not attrs.has_key(self.settings.group_member_attr):
                    grp_data_ldap[group_dn] = LdapGroup(attrs['cn'][0], None, [])
                    continue
                if grp_data_ldap.has_key(group_dn):
                    continue
                all_mails = []
                for member in attrs[self.settings.group_member_attr]:
                    mails = self.get_posix_group_member_from_ldap(ldap_conn, base_dn)
                    if not mails:
                        continue
                    for mail in mails:
                        all_mails.append(mail)

                grp_data_ldap[group_dn] = LdapGroup(attrs['cn'][0], None,
                                                    sorted(set(all_mails)))
        return grp_data_ldap

    def get_posix_group_member_from_ldap(self, ldap_conn, base_dn, member):
        all_mails = []
        search_filter = '(&(objectClass=%s)(%s=%s))' % \
                        (self.settings.user_object_class,
                         self.settings.user_attr_in_memberUid,
                         member)

        results = ldap_conn.search(base_dn, SCOPE_SUBTREE,
                                   search_filter,
                                   [self.settings.login_attr,'cn'])
        if not results:
            return []

        for result in results:
            dn, attrs = result
            if type(attrs) != dict:
                continue
            if attrs.has_key(self.settings.login_attr):
                for mail in attrs[self.settings.login_attr]:
                    all_mails.append(mail.lower())

        return all_mails

    def get_ou_data(self, ldap_conn, config):
        if config.group_filter != '':
            search_filter = '(&(|(objectClass=organizationalUnit)(objectClass=%s))(%s))' % \
                             (self.settings.user_object_class,
                              config.group_filter)
        else:
            search_filter = '(|(objectClass=organizationalUnit)(objectClass=%s))' % self.settings.user_object_class

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
            self.get_ou_member (ldap_conn, base_dn, search_filter, sort_list, ou_name, None, grp_data_ou)
        sort_list.reverse()
        self.sort_list = sort_list

        return grp_data_ou

    def get_ou_member(self, ldap_conn, base_dn, search_filter, sort_list, ou_name, parent_dn, grp_data_ou):
        if self.settings.use_page_result:
            results = ldap_conn.paged_search(base_dn, SCOPE_ONELEVEL,
                                             search_filter,
                                             [self.settings.login_attr, 'ou'])
        else:
            results = ldap_conn.search(base_dn, SCOPE_ONELEVEL,
                                       search_filter,
                                       [self.settings.login_attr, 'ou'])
        # empty ou
        if not results:
            group = LdapGroup(ou_name, None, [], parent_dn)
            sort_list.append((base_dn, group))
            grp_data_ou[base_dn] = group
            return

        mails = []
        member_dn=''
        for pair in results:
            member_dn, attrs = pair
            if type(attrs) != dict:
                continue
            # member
            if attrs.has_key(self.settings.login_attr) and ('ou=' in base_dn or 'OU=' in base_dn):
                mails.append(attrs[self.settings.login_attr][0])
                continue
            # ou
            if attrs.has_key('ou'):
                self.get_ou_member (ldap_conn, member_dn, search_filter,
                                    sort_list, attrs['ou'][0],
                                    base_dn,
                                    grp_data_ou)

        group = LdapGroup(ou_name, None, sorted(set(mails)), parent_dn)
        sort_list.append((base_dn, group))
        grp_data_ou[base_dn] = group

        return grp_data_ou

    def sync_data(self, data_db, data_ldap):
        dn_pairs = get_group_dn_pairs()
        if dn_pairs is None:
            logging.warning('get group dn pairs from db failed.')
            return
        grp_dn_pairs = {}
        for grp_dn in dn_pairs:
            grp_dn_pairs[grp_dn.dn.encode('utf-8')] = grp_dn.group_id

        # sync deleted group in ldap to db
        for k in grp_dn_pairs.iterkeys():
            if not data_ldap.has_key(k) and self.settings.del_group_if_not_found:
                ret = remove_group(grp_dn_pairs[k], '')
                if ret < 0:
                    logging.warning('remove group %d failed.' % grp_dn_pairs[k])
                    continue
                logging.debug('remove group %d success.' % grp_dn_pairs[k])
                self.dgroup += 1

        # sync undeleted group in ldap to db
        super_user = None
        ldap_tup = data_ldap.iteritems()
        if self.settings.import_group_structure:
            ldap_tup = self.sort_list

        for k, v in ldap_tup:
            if grp_dn_pairs.has_key(k):
                # group data lost in db
                if not data_db.has_key(grp_dn_pairs[k]):
                    continue
                group_id = grp_dn_pairs[k]
                add_list, del_list = LdapGroupSync.diff_members(data_db[group_id].members,
                                                                v.members)
                if len(add_list) > 0 or len(del_list) > 0:
                    self.ugroup += 1

                for member in del_list:
                    ret = group_remove_member(group_id, data_db[group_id].creator, member)
                    if ret < 0:
                        logging.warning('remove member %s from group %d failed.' %
                                        (member, group_id))
                        continue
                    logging.debug('remove member %s from group %d success.' %
                                  (member, group_id))

                for member in add_list:
                    ret = group_add_member(group_id, data_db[group_id].creator, member)
                    if ret < 0:
                        logging.warning('add member %s to group %d failed.' %
                                        (member, group_id))
                        continue
                    logging.debug('add member %s to group %d success.' %
                                  (member, group_id))
            else:
                # add ldap group to db
                if super_user is None:
                    if self.settings.import_group_structure:
                        super_user = 'system admin'
                    else:
                        super_user = LdapGroupSync.get_super_user()
                parent_id = 0
                if self.settings.import_group_structure:
                    parent_id = -1
                    if v.parent_dn:
                        parent_id = data_ldap[v.parent_dn].group_id
                group_id = ccnet_api.create_group(v.cn, super_user, 'LDAP', parent_id)
                if group_id < 0:
                    logging.warning('create ldap group [%s] failed.' % v.cn)
                    continue

                ret = add_group_dn_pair(group_id, k)
                if ret < 0:
                    logging.warning('add group dn pair %d<->%s failed.' % (group_id, k))
                    # admin should remove created group manually in web
                    continue
                logging.debug('create group %d, and add dn pair %s<->%d success.' %
                              (group_id, k, group_id))
                self.agroup += 1
                v.group_id = group_id
                if self.settings.create_group_repo and self.settings.import_group_structure:
                    ret = seafile_api.set_group_quota(group_id, -2)
                    if ret < 0:
                        logging.warning('Failed to set group [%s] quota.' % v.cn)
                    ret = seafile_api.add_group_owned_repo(group_id, v.cn, None, 'rw')
                    if not ret:
                        logging.warning('Failed to create group owned repo for %s.' % v.cn)

                for member in v.members:
                    ret = group_add_member(group_id, super_user, member)
                    if ret < 0:
                        logging.warning('add member %s to group %d failed.' %
                                        (member, group_id))
                        continue
                    logging.debug('add member %s to group %d success.' %
                                  (member, group_id))

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
