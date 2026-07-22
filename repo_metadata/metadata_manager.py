from seafevents.repo_metadata.index_master import RepoMetadataIndexMaster
from seafevents.repo_metadata.slow_task_handler import SlowMetadataTaskHandler


class MetadataManager(object):
    def __init__(self, config):
        self._index_master = RepoMetadataIndexMaster(config)
        self._slow_task_handler = SlowMetadataTaskHandler(config)

    def start(self):
        self._index_master.start()
        self._slow_task_handler.start()
