#coding: UTF-8

import posixpath

from seafobj import fs_mgr

ZERO_OBJ_ID = '0000000000000000000000000000000000000000'

class CommitDiffer(object):
    def __init__(self, repo_id, version, root1, root2, virus_files=None):
        self.repo_id = repo_id
        self.version = version
        self.root1 = root1
        self.root2 = root2
        self.virus_files = {
            (normalize_path(file_path), file_id)
            for file_path, file_id in (virus_files or ())
        }
        # Precompute ancestor directories for constant-time subtree checks.
        self.virus_dirs = set()
        for file_path, _ in self.virus_files:
            dirname = posixpath.dirname(file_path)
            while dirname:
                self.virus_dirs.add(dirname)
                if dirname == '/':
                    break
                dirname = posixpath.dirname(dirname)

    def diff(self):
        scan_files = []
        new_dirs = [] # (path, dir_id)
        queued_dirs = [] # (path, dir_id1, dir_id2)

        if ZERO_OBJ_ID == self.root1:
            self.root1 = None
        if ZERO_OBJ_ID == self.root2:
            self.root2 = None

        if self.root1 == self.root2:

            # empty repo or no deleted virus
            if not self.root1 or not self.virus_files:
                return scan_files

            # The directory trees are identical, but deleted virus files
            # re-uploaded unchanged still need to be detected.
            queued_dirs.append(('/', self.root1, self.root2))
        elif not self.root1:
            new_dirs.append(('/', self.root2))
        elif self.root2:
            queued_dirs.append(('/', self.root1, self.root2))

        while True:
            path = old_id = new_id = None
            try:
                path, old_id, new_id = queued_dirs.pop(0)
            except IndexError:
                break

            dir1 = fs_mgr.load_seafdir(self.repo_id, self.version, old_id)
            dir2 = fs_mgr.load_seafdir(self.repo_id, self.version, new_id)

            for dent in dir1.get_files_list():
                new_dent = dir2.lookup_dent(dent.name)
                if new_dent and new_dent.type == dent.type:
                    dir2.remove_entry(dent.name)
                    file_path = make_path(path, dent.name)
                    is_deleted_virus = (new_dent.id == dent.id and
                                        (file_path, new_dent.id) in self.virus_files)
                    if new_dent.id != dent.id or is_deleted_virus:
                        scan_files.append((file_path, new_dent.id, new_dent.size))

            scan_files.extend([(make_path(path, dent.name), dent.id, dent.size)
                               for dent in dir2.get_files_list()])

            for dent in dir1.get_subdirs_list():
                new_dent = dir2.lookup_dent(dent.name)
                if new_dent and new_dent.type == dent.type:
                    dir2.remove_entry(dent.name)
                    dir_path = make_path(path, dent.name)
                    if new_dent.id != dent.id or dir_path in self.virus_dirs:
                        queued_dirs.append((dir_path, dent.id, new_dent.id))

            new_dirs.extend([(make_path(path, dent.name), dent.id)
                             for dent in dir2.get_subdirs_list()])

        while True:
            # Process newly added dirs and its sub-dirs, all files under
            # these dirs should be marked as added.
            path = obj_id = None
            try:
                path, obj_id = new_dirs.pop(0)
            except IndexError:
                break
            d = fs_mgr.load_seafdir(self.repo_id, self.version, obj_id)
            scan_files.extend([(make_path(path, dent.name), dent.id, dent.size)
                               for dent in d.get_files_list()])

            new_dirs.extend([(make_path(path, dent.name), dent.id)
                             for dent in d.get_subdirs_list()])

        return scan_files

def make_path(dirname, filename):
    if dirname == '/':
        return dirname + filename
    else:
        return '/'.join((dirname, filename))

def normalize_path(path):
    return posixpath.normpath('/' + path.lstrip('/'))
