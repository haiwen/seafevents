from seafevents.ldap_syncer.ldap_group_sync import LdapGroupSync
from seafevents.ldap_syncer.utils import get_group_uuid_pairs, remove_group_uuid_pair_by_id
from seaserv import seafile_api, ccnet_api
from seafevents.tests.utils import LDAPSyncerTest
from seafevents.tests.utils.utils import randstring
from seafevents.tests.utils.ldap_sync_test_helper import LDAPSyncTestHelper

class LDAPGroupSyncerTest(LDAPSyncerTest):
    def setUp(self):
        super().setUp()
        self.ldap_helper = LDAPSyncTestHelper(self.config)
        self.ldap_helper.add_ou(self.test_base_dn)
        for config in self.settings.ldap_configs:
            config.base_dn = self.test_base_dn

    def tearDown(self):
        super().tearDown()
        base_dn_dpt_id = self.grp_name2id[self.dn2name(self.test_base_dn)]
        self.ldap_helper.delete_ou(self.test_base_dn)
        ccnet_api.remove_group(base_dn_dpt_id)
        session = self.settings.db_session()
        remove_group_uuid_pair_by_id(session, base_dn_dpt_id)

    def gen_test_user_cn_and_email(self):
        test_cn_suffix = 'a'
        test_cn = 'test_tmp_' + test_cn_suffix
        test_email = test_cn + '@seafile.ren'
        while self.ldap_helper.is_user_exist(cn=test_cn):
            test_cn_suffix = chr(ord(test_cn_suffix) + 1)
            test_cn = 'test_tmp_' + test_cn_suffix
            test_email = test_cn + '@seafile.ren'
        return test_cn, test_email

    def build_name2id_dict(self):
        # prepare group_name to group_id dict for db query
        # get_group_uuid_pairs
        uuid_pairs = get_group_uuid_pairs(self.settings.db_session())

        self.grp_name2id = {}
        for item in reversed(uuid_pairs):
            grp_id = item['group_id']
            g = ccnet_api.get_group(grp_id)
            if not g:
                continue
            name = g.group_name
            if (name not in self.grp_name2id.keys()) or \
                    (name in self.grp_name2id.keys() and grp_id > self.grp_name2id[name]):
                self.grp_name2id[name] = grp_id

    def sync(self):
        l_group_thread = LdapGroupSync(self.settings)
        l_group_thread.start()
        l_group_thread.join()

    def dn2name(self, dn):
        names = dn.split(',')
        return names[0][3:]

    def sync_group_with_user(self, g1_id, g2_id, g3_id, grp3_dn):
        # test user with group
        # test add user
        user_cn, email = self.gen_test_user_cn_and_email()
        user_dn = 'CN=' + user_cn + ',' + self.test_base_dn
        self.ldap_helper.add_user(dn=user_dn, email=email)
        self.ldap_helper.add_user_to_grp(user_dn, grp3_dn)
        self.sync()
        g3_members = ccnet_api.get_group_members(g3_id)
        g2_members = ccnet_api.get_group_members(g2_id)
        g1_members = ccnet_api.get_group_members(g1_id)

        assert len(g3_members) == 1
        for member in g3_members:
            assert member.group_id == g3_id
            assert member.user_name == email
        assert len(g2_members) == 1
        for member in g2_members:
            assert member.group_id == g2_id
            assert member.user_name == email
        assert len(g1_members) == 1
        for member in g1_members:
            assert member.group_id == g1_id
            assert member.user_name == email

        # test delete user
        self.ldap_helper.delete_user(dn=user_dn)
        self.sync()
        members = ccnet_api.get_group_members(g3_id)
        assert len(members) == 0

    def sync_ou_with_user(self, g3_id, ou3_dn):
        user_cn, email = self.gen_test_user_cn_and_email()
        user_dn = 'CN=' + user_cn + ',' + ou3_dn
        self.ldap_helper.add_user(dn=user_dn, email=email)
        self.sync()
        g3_members = ccnet_api.get_group_members(g3_id)
        assert len(g3_members) == 1
        for member in g3_members:
            assert member.group_id == g3_id
            assert member.user_name == email

        # test delete user
        self.ldap_helper.delete_user(dn=user_dn)
        self.sync()
        members = ccnet_api.get_group_members(g3_id)
        assert len(members) == 0

    def test_sync_group_as_group1(self):
        """
        DEL_GROUP_IF_NOT_FOUND = true
        SYNC_GROUP_AS_DEPARTMENT = false
        """
        self.settings.del_group_if_not_found = True
        for config in self.settings.ldap_configs:
            config.sync_group_as_department = False

        # g3 is sub to g2, g2 is sub to g1
        grp1_name = 'test_grp_' + randstring(10) + '_a'
        grp2_name = 'test_grp_' + randstring(10) + '_b'
        grp3_name = 'test_grp_' + randstring(10) + '_c'
        grp1_dn = 'CN=' + grp1_name + ',' + self.test_base_dn
        grp2_dn = 'CN=' + grp2_name + ',' + self.test_base_dn
        grp3_dn = 'CN=' + grp3_name + ',' + self.test_base_dn

        # add group -> sync -> test
        self.ldap_helper.add_grp(grp1_dn)
        self.ldap_helper.add_grp(grp2_dn)
        self.ldap_helper.add_grp(grp3_dn)
        self.ldap_helper.add_grp_to_grp(sub_grp_dn=grp2_dn, parent_grp_dn=grp1_dn)
        self.ldap_helper.add_grp_to_grp(sub_grp_dn=grp3_dn, parent_grp_dn=grp2_dn)
        self.sync()
        self.build_name2id_dict()
        g1_id = self.grp_name2id[grp1_name]
        g2_id = self.grp_name2id[grp2_name]
        g3_id = self.grp_name2id[grp3_name]
        g1 = ccnet_api.get_group(g1_id)
        g2 = ccnet_api.get_group(g2_id)
        g3 = ccnet_api.get_group(g3_id)
        assert g1.group_name == grp1_name
        assert g2.group_name == grp2_name
        assert g3.group_name == grp3_name
        assert g3.parent_group_id != g2.id
        assert g2.parent_group_id != g1.id
        assert g1.source == g2.source == g3.source == 'LDAP'

        # # test user with group
        self.sync_group_with_user(g1_id, g2_id, g3_id, grp3_dn)

        # delete group -> sync -> test
        # delete parent in ldap, sub group will not be deleted
        self.ldap_helper.delete_grp(grp3_dn)
        self.ldap_helper.delete_grp(grp2_dn)
        self.ldap_helper.delete_grp(grp1_dn)
        self.sync()
        g1 = ccnet_api.get_group(g1_id)
        g2 = ccnet_api.get_group(g2_id)
        g3 = ccnet_api.get_group(g3_id)
        assert g1 == g2 == g3 == None

    def test_sync_group_as_group2(self):
        """
        DEL_GROUP_IF_NOT_FOUND = false
        SYNC_GROUP_AS_DEPARTMENT = false
        """
        self.settings.del_group_if_not_found = False
        for config in self.settings.ldap_configs:
            config.sync_group_as_department = False

        grp1_name = 'test_grp_' + randstring(10) + '_a'
        grp2_name = 'test_grp_' + randstring(10) + '_b'
        grp3_name = 'test_grp_' + randstring(10) + '_c'
        grp1_dn = 'CN=' + grp1_name + ',' + self.test_base_dn
        grp2_dn = 'CN=' + grp2_name + ',' + self.test_base_dn
        grp3_dn = 'CN=' + grp3_name + ',' + self.test_base_dn

        # add group -> sync -> test
        self.ldap_helper.add_grp(grp1_dn)
        self.ldap_helper.add_grp(grp2_dn)
        self.ldap_helper.add_grp(grp3_dn)
        self.ldap_helper.add_grp_to_grp(sub_grp_dn=grp2_dn, parent_grp_dn=grp1_dn)
        self.ldap_helper.add_grp_to_grp(sub_grp_dn=grp3_dn, parent_grp_dn=grp2_dn)
        self.sync()
        self.build_name2id_dict()
        g1_id = self.grp_name2id[grp1_name]
        g2_id = self.grp_name2id[grp2_name]
        g3_id = self.grp_name2id[grp3_name]
        g1 = ccnet_api.get_group(g1_id)
        g2 = ccnet_api.get_group(g2_id)
        g3 = ccnet_api.get_group(g3_id)
        assert g1.group_name == grp1_name
        assert g2.group_name == grp2_name
        assert g3.group_name == grp3_name
        assert g3.parent_group_id != g2.id
        assert g2.parent_group_id != g1.id
        assert g1.source == g2.source == g3.source == 'LDAP'

        # test user with group
        self.sync_group_with_user(g1_id, g2_id, g3_id, grp3_dn)

        # delete group -> sync -> test
        # delete parent in ldap, sub group will not be deleted
        self.ldap_helper.delete_grp(grp3_dn)
        self.ldap_helper.delete_grp(grp2_dn)
        self.ldap_helper.delete_grp(grp1_dn)
        self.sync()
        g1 = ccnet_api.get_group(g1_id)
        g2 = ccnet_api.get_group(g2_id)
        g3 = ccnet_api.get_group(g3_id)
        assert g1.group_name == grp1_name
        assert g2.group_name == grp2_name
        assert g3.group_name == grp3_name
        assert g1.source == g2.source == g3.source == 'LDAP'

        ccnet_api.remove_group(g1_id)
        session = self.settings.db_session()
        remove_group_uuid_pair_by_id(session, g1_id)
        ccnet_api.remove_group(g2_id)
        session = self.settings.db_session()
        remove_group_uuid_pair_by_id(session, g2_id)
        ccnet_api.remove_group(g3_id)
        session = self.settings.db_session()
        remove_group_uuid_pair_by_id(session, g3_id)

    def test_sync_group_as_group_rename_name(self):
        """
        DEL_GROUP_IF_NOT_FOUND = true
        SYNC_GROUP_AS_DEPARTMENT = false
        """
        self.settings.del_group_if_not_found = True
        for config in self.settings.ldap_configs:
            config.sync_group_as_department = False


        # g3 is sub to g2, g2 is sub to g1
        grp1_name = 'test_grp_' + randstring(10) + '_a'
        grp1_new_name = 'test_grp_' + randstring(10) + '_b'
        grp1_dn = 'CN=' + grp1_name + ',' + self.test_base_dn
        grp1_new_dn = 'CN=' + grp1_new_name + ',' + self.test_base_dn

        # add group -> sync -> test
        self.ldap_helper.add_grp(grp1_dn)
        self.sync()
        self.build_name2id_dict()
        g1_id = self.grp_name2id[grp1_name]
        g1 = ccnet_api.get_group(g1_id)
        assert g1.group_name == grp1_name
        assert g1.source == 'LDAP'

        # rename group -> sync -> test
        self.ldap_helper.rename_grp(grp1_dn, grp1_new_name, self.test_base_dn)
        self.sync()
        self.build_name2id_dict()
        g1_id = self.grp_name2id[grp1_new_name]
        g1 = ccnet_api.get_group(g1_id)
        assert g1.group_name == grp1_new_name
        assert g1.source == 'LDAP'

        # delete group -> sync -> test
        self.ldap_helper.delete_grp(grp1_new_dn)
        self.sync()
        g1 = ccnet_api.get_group(g1_id)
        assert g1 == None


    def test_sync_group_as_departmennt1(self):
        """
        DEL_DEPARTMENT_IF_NOT_FOUND = True
        SYNC_GROUP_AS_DEPARTMENT = True
        """
        self.settings.del_department_if_not_found = True
        for config in self.settings.ldap_configs:
            config.sync_group_as_department = True

        # g3 is sub to g2, g2 is sub to g1
        # add group -> sync -> test
        grp1_name = 'test_grp_' + randstring(10) + '_a'
        grp2_name = 'test_grp_' + randstring(10) + '_b'
        grp3_name = 'test_grp_' + randstring(10) + '_c'
        grp1_dn = 'CN=' + grp1_name + ',' + self.test_base_dn
        grp2_dn = 'CN=' + grp2_name + ',' + self.test_base_dn
        grp3_dn = 'CN=' + grp3_name + ',' + self.test_base_dn

        # add group -> sync -> test
        self.ldap_helper.add_grp(grp3_dn)
        self.ldap_helper.add_grp(grp2_dn)
        self.ldap_helper.add_grp(grp1_dn)
        self.ldap_helper.add_grp_to_grp(sub_grp_dn=grp2_dn, parent_grp_dn=grp1_dn)
        self.ldap_helper.add_grp_to_grp(sub_grp_dn=grp3_dn, parent_grp_dn=grp2_dn)
        self.sync()
        self.build_name2id_dict()
        g1_id = self.grp_name2id[grp1_name]
        g2_id = self.grp_name2id[grp2_name]
        g3_id = self.grp_name2id[grp3_name]
        g1 = ccnet_api.get_group(g1_id)
        g2 = ccnet_api.get_group(g2_id)
        g3 = ccnet_api.get_group(g3_id)
        assert g1.group_name == grp1_name
        assert g2.group_name == grp2_name
        assert g3.group_name == grp3_name
        assert g3.parent_group_id == g2.id
        assert g2.parent_group_id == g1.id
        assert g1.source == g2.source == g3.source == 'LDAP'

        # test user with group
        self.sync_group_with_user(g1_id, g2_id, g3_id, grp3_dn)

        # delete group -> sync -> test
        # delete parent in ldap, sub group will not be deleted
        self.ldap_helper.delete_grp(grp3_dn)
        self.ldap_helper.delete_grp(grp2_dn)
        self.ldap_helper.delete_grp(grp1_dn)
        self.sync()
        g1 = ccnet_api.get_group(g1_id)
        g2 = ccnet_api.get_group(g2_id)
        g3 = ccnet_api.get_group(g3_id)
        assert g1 == g2 == g3 == None

    def test_sync_group_as_departmennt2(self):
        """
        DEL_DEPARTMENT_IF_NOT_FOUND = false
        SYNC_GROUP_AS_DEPARTMENT = true
        """
        self.settings.del_department_if_not_found = False
        for config in self.settings.ldap_configs:
            config.sync_group_as_department = True
        # g3 is sub to g2, g2 is sub to g1
        # add group -> sync -> test
        grp1_name = 'test_grp_' + randstring(10) + '_a'
        grp2_name = 'test_grp_' + randstring(10) + '_b'
        grp3_name = 'test_grp_' + randstring(10) + '_c'
        grp1_dn = 'CN=' + grp1_name + ',' + self.test_base_dn
        grp2_dn = 'CN=' + grp2_name + ',' + self.test_base_dn
        grp3_dn = 'CN=' + grp3_name + ',' + self.test_base_dn

        # add group -> sync -> test
        self.ldap_helper.add_grp(grp1_dn)
        self.ldap_helper.add_grp(grp2_dn)
        self.ldap_helper.add_grp(grp3_dn)
        self.ldap_helper.add_grp_to_grp(sub_grp_dn=grp2_dn, parent_grp_dn=grp1_dn)
        self.ldap_helper.add_grp_to_grp(sub_grp_dn=grp3_dn, parent_grp_dn=grp2_dn)
        self.sync()
        self.build_name2id_dict()
        g1_id = self.grp_name2id[grp1_name]
        g2_id = self.grp_name2id[grp2_name]
        g3_id = self.grp_name2id[grp3_name]
        g1 = ccnet_api.get_group(g1_id)
        g2 = ccnet_api.get_group(g2_id)
        g3 = ccnet_api.get_group(g3_id)
        assert g1.group_name == grp1_name
        assert g2.group_name == grp2_name
        assert g3.group_name == grp3_name
        assert g3.parent_group_id == g2.id
        assert g2.parent_group_id == g1.id
        assert g1.source == g2.source == g3.source == 'LDAP'

        # test user with group
        self.sync_group_with_user(g1_id, g2_id, g3_id, grp3_dn)

        # delete group -> sync -> test
        # delete parent in ldap, sub group will not be deleted
        self.ldap_helper.delete_grp(grp3_dn)
        self.ldap_helper.delete_grp(grp2_dn)
        self.ldap_helper.delete_grp(grp1_dn)
        self.sync()
        g1 = ccnet_api.get_group(g1_id)
        g2 = ccnet_api.get_group(g2_id)
        g3 = ccnet_api.get_group(g3_id)
        assert g1.group_name == grp1_name
        assert g2.group_name == grp2_name
        assert g3.group_name == grp3_name
        assert g1.source == g2.source == g3.source == 'LDAP'

        ccnet_api.remove_group(g3_id)
        session = self.settings.db_session()
        remove_group_uuid_pair_by_id(session, g3_id)
        ccnet_api.remove_group(g2_id)
        session = self.settings.db_session()
        remove_group_uuid_pair_by_id(session, g2_id)
        ccnet_api.remove_group(g1_id)
        session = self.settings.db_session()
        remove_group_uuid_pair_by_id(session, g1_id)

    def test_sync_ou_as_department1(self):
        """
        DEL_DEPARTMENT_IF_NOT_FOUND = true
        """
        self.settings.del_department_if_not_found = True

        ou1_name = 'test_ou_' + randstring(10) + '_a'
        ou2_name = 'test_ou_' + randstring(10) + '_b'
        ou3_name = 'test_ou_' + randstring(10) + '_c'
        ou1_dn = 'OU=' + ou1_name + ',' + self.test_base_dn
        ou2_dn = 'OU=' + ou2_name + ',' + ou1_dn
        ou3_dn = 'OU=' + ou3_name + ',' + ou2_dn

        # add group -> sync -> test
        ou1_dn = self.ldap_helper.add_ou(dn=ou1_dn)
        ou2_dn = self.ldap_helper.add_ou(dn=ou2_dn)
        ou3_dn = self.ldap_helper.add_ou(dn=ou3_dn)
        self.sync()
        self.build_name2id_dict()
        g1_id = self.grp_name2id[ou1_name]
        g2_id = self.grp_name2id[ou2_name]
        g3_id = self.grp_name2id[ou3_name]
        g1 = ccnet_api.get_group(g1_id)
        g2 = ccnet_api.get_group(g2_id)
        g3 = ccnet_api.get_group(g3_id)
        assert g1.group_name == ou1_name
        assert g2.group_name == ou2_name
        assert g3.group_name == ou3_name
        assert g3.parent_group_id == g2.id
        assert g2.parent_group_id == g1.id
        assert g1.source == g2.source == g3.source == 'LDAP'

        # test user with group
        self.sync_ou_with_user(g3_id, ou3_dn)

        # delete ou and test
        self.ldap_helper.delete_ou(ou3_dn)
        self.ldap_helper.delete_ou(ou2_dn)
        self.ldap_helper.delete_ou(ou1_dn)
        self.sync()
        g1 = ccnet_api.get_group(g1_id)
        g2 = ccnet_api.get_group(g2_id)
        g3 = ccnet_api.get_group(g3_id)
        assert g1 == g2 == g3 == None

    def test_sync_ou_as_department2(self):
        """
        DEL_DEPARTMENT_IF_NOT_FOUND = false
        """
        self.settings.del_department_if_not_found = False

        ou1_name = 'test_ou_' + randstring(10) + '_a'
        ou2_name = 'test_ou_' + randstring(10) + '_b'
        ou3_name = 'test_ou_' + randstring(10) + '_c'
        ou1_dn = 'OU=' + ou1_name + ',' + self.test_base_dn
        ou2_dn = 'OU=' + ou2_name + ',' + ou1_dn
        ou3_dn = 'OU=' + ou3_name + ',' + ou2_dn

        # add group -> sync -> test
        ou1_dn = self.ldap_helper.add_ou(dn=ou1_dn)
        ou2_dn = self.ldap_helper.add_ou(dn=ou2_dn)
        ou3_dn = self.ldap_helper.add_ou(dn=ou3_dn)
        self.sync()
        self.build_name2id_dict()
        g1_id = self.grp_name2id[ou1_name]
        g2_id = self.grp_name2id[ou2_name]
        g3_id = self.grp_name2id[ou3_name]
        g1 = ccnet_api.get_group(g1_id)
        g2 = ccnet_api.get_group(g2_id)
        g3 = ccnet_api.get_group(g3_id)
        assert g1.group_name == ou1_name
        assert g2.group_name == ou2_name
        assert g3.group_name == ou3_name
        assert g3.parent_group_id == g2.id
        assert g2.parent_group_id == g1.id
        assert g1.source == g2.source == g3.source == 'LDAP'

        # test user with group
        self.sync_ou_with_user(g3_id, ou3_dn)

        # delete ou and test
        self.ldap_helper.delete_ou(ou3_dn)
        self.ldap_helper.delete_ou(ou2_dn)
        self.ldap_helper.delete_ou(ou1_dn)
        self.sync()
        g1 = ccnet_api.get_group(g1_id)
        g2 = ccnet_api.get_group(g2_id)
        g3 = ccnet_api.get_group(g3_id)
        assert g1.group_name == ou1_name
        assert g2.group_name == ou2_name
        assert g3.group_name == ou3_name
        assert g1.source == g2.source == g3.source == 'LDAP'

        ccnet_api.remove_group(g3_id)
        session = self.settings.db_session()
        remove_group_uuid_pair_by_id(session, g3_id)
        ccnet_api.remove_group(g2_id)
        session = self.settings.db_session()
        remove_group_uuid_pair_by_id(session, g2_id)
        ccnet_api.remove_group(g1_id)
        session = self.settings.db_session()
        remove_group_uuid_pair_by_id(session, g1_id)
