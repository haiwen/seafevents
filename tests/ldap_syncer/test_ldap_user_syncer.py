from seafevents.ldap_syncer.ldap_user_sync import LdapUserSync
from seaserv import seafile_api, ccnet_api, get_group_dn_pairs
from seafevents.tests.utils import LDAPSyncerTest
from seafevents.tests.utils.utils import randstring
from seafevents.tests.utils.ldap_sync_test_helper import LDAPSyncTestHelper

class LDAPUserSyncerTest(LDAPSyncerTest):
    def setUp(self):
        super().setUp()
        self.ldap_helper = LDAPSyncTestHelper(self.config)
        self.ldap_helper.add_ou(self.test_base_dn)
        for config in self.settings.ldap_configs:
            config.base_dn = self.test_base_dn

    def tearDown(self):
        super().tearDown()
        self.ldap_helper.delete_ou(self.test_base_dn)

    def gen_test_user_cn_and_email(self):
        test_cn_suffix = 'a'
        test_cn = 'test_tmp_user_' + test_cn_suffix
        test_email = test_cn + '@seafile.ren'
        while self.ldap_helper.is_user_exist(cn=test_cn):
            test_cn_suffix = chr(ord(test_cn_suffix) + 1)
            test_cn = 'test_tmp_user_' + test_cn_suffix
            test_email = test_cn + '@seafile.ren'
        return test_cn, test_email

    def update_sync_test(self, dn, email):
        new_first_name = randstring()
        new_last_name = randstring()
        new_role = randstring()
        new_contact_email = randstring() + '@seafile.com'
        new_department = randstring()
        self.ldap_helper.update_user(
            dn=dn,
            first_name=new_first_name,
            last_name=new_last_name,
            role=new_role,
            contact_email=new_contact_email,
            department=new_department,
        )
        self.sync()
        user = ccnet_api.get_emailuser(email)

        sql = 'select nickname, contact_email from profile_profile where user="{}"'.format(email)
        self.cursor.execute(sql)
        profile_r = self.cursor.fetchone()
        sql = 'select department from profile_detailedprofile where user="{}"'.format(email)
        self.cursor.execute(sql)
        detailedprofile_r = self.cursor.fetchone()
        assert new_first_name + ' ' + new_last_name == profile_r[0]
        assert new_contact_email == profile_r[1]
        assert new_department == detailedprofile_r[0]
        assert new_role == user.role

        # set role manually in seafile, ldap role should not be synced
        ccnet_api.update_role_emailuser(email, 'Guest')
        self.ldap_helper.update_user(
            dn=dn,
            role=randstring(),
        )
        self.sync()
        user = ccnet_api.get_emailuser(email)
        assert user.role == 'Guest'

    def sync(self):
        l_user_thread = LdapUserSync(self.settings)
        l_user_thread.start()
        l_user_thread.join()

    def test_sync_user1(self):
        """
        DEACTIVE_USER_IF_NOTFOUND = true
        ACTIVATE_USER_WHEN_IMPORT = true
        """
        self.settings.enable_deactive_user = True  # DEACTIVE_USER_IF_NOTFOUND
        self.settings.activate_user = True         # ACTIVATE_USER_WHEN_IMPORT

        # generate a new ldap user
        cn, email = self.gen_test_user_cn_and_email()
        dn = 'CN=' + cn + ',' + self.test_base_dn

        # if sync to ccnet, then delete
        user = ccnet_api.get_emailuser(email)
        if user and user.source == 'LDAPImport':
            # remove_emailuser param is 'LDAP', but actualy delete LDAPImport user
            ccnet_api.remove_emailuser('LDAP', email)

        # add -> sync -> ccnet api test
        self.ldap_helper.add_user(dn=dn, email=email)
        self.sync()
        user = ccnet_api.get_emailuser(email)
        assert user.email == email
        assert user.source == 'LDAPImport'
        assert user.is_active == True

        # update -> sync -> test
        self.update_sync_test(dn, email)

        # delete -> sync -> ccnet api test
        self.ldap_helper.delete_user(dn=dn)
        self.sync()
        user = ccnet_api.get_emailuser(email)
        assert user.is_active == False

    def test_sync_user2(self):
        """
        DEACTIVE_USER_IF_NOTFOUND = true
        ACTIVATE_USER_WHEN_IMPORT = false
        """
        self.settings.enable_deactive_user = True  # DEACTIVE_USER_IF_NOTFOUND
        self.settings.activate_user = False        # ACTIVATE_USER_WHEN_IMPORT

        # generate a new ldap user
        cn, email = self.gen_test_user_cn_and_email()
        dn = 'CN=' + cn + ',' + self.test_base_dn

        # if sync to ccnet, then delete
        user = ccnet_api.get_emailuser(email)
        if user and user.source == 'LDAPImport':
            # remove_emailuser param is 'LDAP', but actualy delete LDAPImport user
            ccnet_api.remove_emailuser('LDAP', email)

        # add -> sync -> ccnet api test
        self.ldap_helper.add_user(dn=dn, email=email)
        self.sync()
        user = ccnet_api.get_emailuser(email)
        assert user.email == email
        assert user.source == 'LDAPImport'
        assert user.is_active == False

        # update -> sync -> test
        self.update_sync_test(dn, email)

        # delete -> sync -> ccnet api test
        self.ldap_helper.delete_user(dn=dn)
        self.sync()
        user = ccnet_api.get_emailuser(email)
        assert user.is_active == False



    def test_sync_user3(self):
        """
        DEACTIVE_USER_IF_NOTFOUND = false
        ACTIVATE_USER_WHEN_IMPORT = true
        """
        self.settings.enable_deactive_user = False # DEACTIVE_USER_IF_NOTFOUND
        self.settings.activate_user = True         # ACTIVATE_USER_WHEN_IMPORT

        # generate a new ldap user
        cn, email = self.gen_test_user_cn_and_email()
        dn = 'CN=' + cn + ',' + self.test_base_dn

        # if sync to ccnet, then delete
        user = ccnet_api.get_emailuser(email)
        if user and user.source == 'LDAPImport':
            # remove_emailuser param is 'LDAP', but actualy delete LDAPImport user
            ccnet_api.remove_emailuser('LDAP', email)

        # add -> sync -> ccnet api test
        self.ldap_helper.add_user(dn=dn, email=email)
        self.sync()
        user = ccnet_api.get_emailuser(email)
        assert user.email == email
        assert user.source == 'LDAPImport'
        assert user.is_active == True

        # update -> sync -> test
        self.update_sync_test(dn, email)

        # delete -> sync -> ccnet api test
        self.ldap_helper.delete_user(dn=dn)
        self.sync()
        user = ccnet_api.get_emailuser(email)
        assert user.is_active == True

    def test_sync_user4(self):
        """
        DEACTIVE_USER_IF_NOTFOUND = false
        ACTIVATE_USER_WHEN_IMPORT = false
        """
        self.settings.enable_deactive_user = False # DEACTIVE_USER_IF_NOTFOUND
        self.settings.activate_user = False        # ACTIVATE_USER_WHEN_IMPORT

        # generate a new ldap user
        cn, email = self.gen_test_user_cn_and_email()
        dn = 'CN=' + cn + ',' + self.test_base_dn

        # if sync to ccnet, then delete
        user = ccnet_api.get_emailuser(email)
        if user and user.source == 'LDAPImport':
            # remove_emailuser param is 'LDAP', but actualy delete LDAPImport user
            ccnet_api.remove_emailuser('LDAP', email)

        # add -> sync -> ccnet api test
        self.ldap_helper.add_user(dn=dn, email=email)
        self.sync()
        user = ccnet_api.get_emailuser(email)
        assert user.email == email
        assert user.source == 'LDAPImport'
        assert user.is_active == False

        # update -> sync -> test
        self.update_sync_test(dn, email)

        # delete -> sync -> ccnet api test
        self.ldap_helper.delete_user(dn=dn)
        self.sync()
        user = ccnet_api.get_emailuser(email)
        assert user.is_active == False
