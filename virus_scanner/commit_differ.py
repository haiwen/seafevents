#coding: UTF-8

from seafobj import fs_mgr

ZERO_OBJ_ID = '0000000000000000000000000000000000000000'

# compute the differences between two commits to get scan files
class CommitDiffer(object):

    # para：repo_id, version, root1, root2
    def __init__(self, repo_id, version, root1, root2):
        self.repo_id = repo_id
        self.version = version
        self.root1 = root1
        self.root2 = root2

    # Compares the directory structures of root1 and root2 and returns a list of files that have been added, modified, or deleted. 
    # The method uses a breadth-first search approach to traverse the directory trees and identify changes.
    # 该方法使用广度优先搜索方法遍历目录树并识别更改。
    def diff(self):
        # scan_files 表示需要扫描的文件，就是 diff tree1 and tree2 中的文件
        scan_files = []
        new_dirs = [] # (path, dir_id)
        queued_dirs = [] # (path, dir_id1, dir_id2)

        if ZERO_OBJ_ID == self.root1:
            self.root1 = None
        if ZERO_OBJ_ID == self.root2:
            self.root2 = None

        # 如果两个根节点相同，直接返回空扫描文件
        if self.root1 == self.root2:
            return scan_files
        elif not self.root1:
            new_dirs.append(('/', self.root2))
        elif self.root2:
            queued_dirs.append(('/', self.root1, self.root2))

        while True:
            # 初始化
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
                    if new_dent.id != dent.id:
                        scan_files.append((make_path(path, dent.name), new_dent.id,
                                           new_dent.size))

            scan_files.extend([(make_path(path, dent.name), dent.id, dent.size)
                               for dent in dir2.get_files_list()])

            for dent in dir1.get_subdirs_list():
                new_dent = dir2.lookup_dent(dent.name)
                if new_dent and new_dent.type == dent.type:
                    dir2.remove_entry(dent.name)
                    if new_dent.id != dent.id:
                        queued_dirs.append((make_path(path, dent.name), dent.id, new_dent.id))

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

# 拼接文件路径
def make_path(dirname, filename):
    if dirname == '/':
        return dirname + filename
    else:
        return '/'.join((dirname, filename))
