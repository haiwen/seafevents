#coding: utf-8

import logging
import os
import tempfile
import subprocess
from seafobj import commit_mgr, fs_mgr, block_mgr
from db_oper import DBOper
from commit_differ import CommitDiffer

class VirusScan(object):
    def __init__(self, settings):
        self.settings = settings
        self.db_oper = DBOper(settings)

    def start(self):
        if not self.db_oper.is_enabled():
            return

        repo_list = self.db_oper.get_repo_list()
        if repo_list is None:
            self.db_oper.close_db()
            return

        for row in repo_list:
            repo_id, head_commit_id, scan_commit_id = row

            if head_commit_id == scan_commit_id:
                logging.debug('No change occur for repo %.8s, skip virus scan.',
                              repo_id)
                continue
            self.scan_virus(repo_id, head_commit_id, scan_commit_id)

        self.db_oper.close_db()

    def scan_virus(self, repo_id, head_commit_id, scan_commit_id):
        try:
            sroot_id = None
            hroot_id = None

            if scan_commit_id:
                sroot_id = commit_mgr.get_commit_root_id(repo_id, 1, scan_commit_id)
            if head_commit_id:
                hroot_id = commit_mgr.get_commit_root_id(repo_id, 1, head_commit_id)

            differ = CommitDiffer(repo_id, 1, sroot_id, hroot_id)
            scan_files = differ.diff()

            if len(scan_files) == 0:
                logging.debug('No change occur for repo %.8s, skip virus scan.',
                              repo_id)
                self.db_oper.update_vscan_record(repo_id, head_commit_id)
                return
            else:
                logging.info('Start to scan virus for repo %.8s.', repo_id)

            vnum = 0
            nvnum = 0
            nfailed = 0
            vrecords = []

            for scan_file in scan_files:
                fpath, fid = scan_file
                ret = self.scan_file_virus(repo_id, fid, fpath)

                if ret == 0:
                    logging.debug('File %s virus scan by %s: OK.',
                                  fpath, self.settings.scan_cmd)
                    nvnum += 1
                elif ret == 1:
                    logging.info('File %s virus scan by %s: Found virus.',
                                 fpath, self.settings.scan_cmd)
                    vnum += 1
                    vrecords.append((repo_id, head_commit_id, fpath))
                else:
                    logging.debug('File %s virus scan by %s: Failed.',
                                  fpath, self.settings.scan_cmd)
                    nfailed += 1

            if nfailed == 0:
                ret = 0
                if len(vrecords) > 0:
                    ret = self.db_oper.add_virus_record(vrecords)
                if ret == 0:
                    self.db_oper.update_vscan_record(repo_id, head_commit_id)

            logging.info('Virus scan for repo %.8s finished: %d virus, %d non virus, %d failed.',
                         repo_id, vnum, nvnum, nfailed)

        except Exception as e:
            logging.warning('Failed to scan virus for repo %.8s: %s.',
                            repo_id, e)

    def scan_file_virus(self, repo_id, file_id, file_path):
        try:
            tfd, tpath = tempfile.mkstemp()
            seafile = fs_mgr.load_seafile(repo_id, 1, file_id)
            for blk_id in seafile.blocks:
                os.write(tfd, block_mgr.load_block(repo_id, 1, blk_id))

            with open(os.devnull, 'w') as devnull:
                ret_code = subprocess.call([self.settings.scan_cmd, tpath],
                                           stdout=devnull, stderr=devnull)

            return self.parse_scan_result(ret_code)

        except Exception as e:
            logging.warning('Virus scan for file %s encounter error: %s.',
                            file_path, e)
            return -1
        finally:
            if tfd > 0:
                os.unlink(tpath)

    def parse_scan_result(self, ret_code):
        rcode_str = str(ret_code)

        for code in self.settings.nonvir_codes:
            if rcode_str == code:
                return 0

        for code in self.settings.vir_codes:
            if rcode_str == code:
                return 1

        return ret_code
