#coding: utf-8

import logging

from seaserv import get_ldap_groups, get_group_members, add_group_dn_pair, \
        get_group_dn_pairs, create_group, group_add_member, group_remove_member, \
        remove_group, get_super_users
from ldap import SCOPE_SUBTREE, SCOPE_BASE
from ldap_conn import LdapConn
from ldap_sync import LdapSync

class LdapGroup(object):
    def __init__(self, cn, creator, members):
        self.cn = cn
        self.creator = creator
        self.members = members

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
            data = self.get_data_by_base_dn(ldap_conn, base_dn, search_filter)
            if data is None:
                continue
            grp_data_ldap.update(data)

        ldap_conn.unbind_conn()

        return grp_data_ldap

    def get_data_by_base_dn(self, ldap_conn, base_dn, search_filter):
        grp_data_ldap = {}

        if self.settings.use_page_result:
            groups = ldap_conn.paged_search(base_dn, SCOPE_SUBTREE,
                                            search_filter,
                                            [self.settings.group_member_attr, 'cn'])
        else:
            groups = ldap_conn.search(base_dn, SCOPE_SUBTREE,
                                      search_filter,
                                      [self.settings.group_member_attr, 'cn'])
        if groups is None:
            return None

        for pair in groups:
            group_dn, attrs = pair
            if type(attrs) != dict:
                continue
            if not attrs.has_key(self.settings.group_member_attr):
                grp_data_ldap[group_dn] = LdapGroup(attrs['cn'][0], None, [])
                continue
            if grp_data_ldap.has_key(group_dn):
                continue
            all_mails = []
            for member in attrs[self.settings.group_member_attr]:
                mails = []
                if self.settings.group_object_class == 'posixGroup':
                    mails = self.get_posix_group_member_from_ldap(ldap_conn, member)
                else:
                    mails = self.get_group_member_from_ldap(ldap_conn, member, grp_data_ldap)

                if mails is None:
                    return None
                for mail in mails:
                    all_mails.append(mail)
            grp_data_ldap[group_dn] = LdapGroup(attrs['cn'][0], None,
                                                sorted(set(all_mails)))

        return grp_data_ldap

    def get_posix_group_member_from_ldap(self, ldap_conn, member):
        all_mails = []
        search_filter = '(&(objectClass=%s)(%s=%s))' % \
                        (self.settings.user_object_class,
                         self.settings.user_attr_in_memberUid,
                         member)

        base_dns = self.config.base_dn.split(';')
        for base_dn in base_dns:
            results = ldap_conn.search(base_dn, SCOPE_SUBTREE,
                                       search_filter,
                                       [self.settings.login_attr,'cn'])

            for result in results:
                dn, attrs = result
                if type(attrs) != dict:
                    continue
                if attrs.has_key(self.settings.login_attr):
                    for mail in attrs[self.settings.login_attr]:
                        all_mails.append(mail.lower())

        return all_mails

    def get_group_member_from_ldap(self, ldap_conn, base_dn, grp_data):
        all_mails = []
        search_filter = '(|(objectClass=%s)(objectClass=%s))' % \
                         (self.settings.group_object_class,
                          self.settings.user_object_class)
        result = ldap_conn.search(base_dn, SCOPE_BASE, search_filter,
                                  [self.settings.group_member_attr,
                                   self.settings.login_attr, 'cn'])
        if result is None:
            return None
        elif not result:
            return all_mails

        dn, attrs = result[0]
        if type(attrs) != dict:
            return all_mails
        # group member
        if attrs.has_key(self.settings.group_member_attr):
            if grp_data.has_key(dn):
                for mail in grp_data[dn].members:
                    all_mails.append(mail)
                    continue
            for member in attrs[self.settings.group_member_attr]:
                mails = self.get_group_member_from_ldap(ldap_conn, member, grp_data)
                if mails is None:
                    return None
                for mail in mails:
                    all_mails.append(mail.lower())
            grp_data[dn] = LdapGroup(attrs['cn'][0], None, sorted(set(all_mails)))
        # user member
        elif attrs.has_key(self.settings.login_attr):
            for mail in attrs[self.settings.login_attr]:
                all_mails.append(mail.lower())

        return all_mails

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
            if not data_ldap.has_key(k):
                ret = remove_group(grp_dn_pairs[k], '')
                if ret < 0:
                    logging.warning('remove group %d failed.' % grp_dn_pairs[k])
                    continue
                logging.debug('remove group %d success.' % grp_dn_pairs[k])
                self.dgroup += 1

        # sync undeleted group in ldap to db
        super_user = None
        for k, v in data_ldap.iteritems():
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
                    super_user = LdapGroupSync.get_super_user()
                group_id = create_group(v.cn, super_user, 'LDAP')
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
