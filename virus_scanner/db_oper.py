#coding: utf-8

import logging

class DBOper(object):
    def __init__(self, settings):
        self.edb_conn = None
        self.edb_cursor = None
        self.sdb_conn = None
        self.sdb_cursor = None
        self.is_enable = False
        self.init_db(settings)

    def init_db(self, settings):
        try:
            import MySQLdb
        except ImportError:
            logging.info('Failed to import MySQLdb module, stop virus scan.')
            return

        try:
            self.edb_conn = MySQLdb.connect(host=settings.edb_host, port=settings.edb_port,
                                            user=settings.edb_user, passwd=settings.edb_passwd,
                                            db=settings.edb_name, charset=settings.edb_charset)
            self.edb_conn.autocommit(True)
            self.edb_cursor = self.edb_conn.cursor()

            self.sdb_conn = MySQLdb.connect(host=settings.sdb_host, port=settings.sdb_port,
                                            user=settings.sdb_user, passwd=settings.sdb_passwd,
                                            db=settings.sdb_name, charset=settings.sdb_charset)
            self.sdb_conn.autocommit(True)
            self.sdb_cursor = self.sdb_conn.cursor()

            self.is_enable = True
        except Exception as e:
            logging.info('Failed to init mysql db: %s, stop virus scan.' %  e)
            if self.edb_cursor:
                self.edb_cursor.close()
            if self.edb_conn:
                self.edb_conn.close()
            if self.sdb_cursor:
                self.sdb_cursor.close()
            if self.sdb_conn:
                self.sdb_conn.close()

    def is_enabled(self):
        return self.is_enable

    def close_db(self):
        if self.is_enable:
            self.edb_cursor.close()
            self.edb_conn.close()
            self.sdb_cursor.close()
            self.sdb_conn.close()

    def get_repo_list(self):
        repo_list = []
        try:
            self.sdb_cursor.execute('select repo_id, commit_id from Branch '
                                    'where name="master" and repo_id not in '
                                    '(select repo_id from VirtualRepo)')
            rows = self.sdb_cursor.fetchall()
            for row in rows:
                repo_id, commit_id = row

                self.edb_cursor.execute('select scan_commit_id from VirusScanRecord '
                                        'where repo_id = %s', (repo_id))
                scan_commit_id = None
                if self.edb_cursor.rowcount == 1:
                    scan_commit_id = self.edb_cursor.fetchone()[0]

                repo_list.append((repo_id, commit_id, scan_commit_id))
        except Exception as e:
            logging.warning('Failed to fetch repo list from db: %s.', e)
            repo_list = None

        return repo_list

    def update_vscan_record(self, repo_id, scan_commit_id):
        try:
            self.edb_cursor.execute('replace into VirusScanRecord values (%s, %s)',
                                    (repo_id, scan_commit_id))
        except Exception as e:
            logging.warning('Failed to update virus scan record from db: %s.', e)
