from seafevents.app.mq_handler import EventsHandler, init_message_handlers
from seafevents.tasks import IndexUpdater, SeahubEmailSender, LdapSyncer,\
        VirusScanner, Statistics, CountUserActivity, CountTrafficInfo, ContentScanner,\
        WorkWinxinNoticeSender, FileUpdatesSender, RepoOldFileAutoDelScanner,\
        DeletedFilesCountCleaner
from seafevents.semantic_search.semantic_search import SemanticSearch

from seafevents.repo_metadata.index_master import RepoMetadataIndexMaster
from seafevents.repo_metadata.index_worker import RepoMetadataIndexWorker
from seafevents.seafevent_server.seafevent_server import SeafEventServer
from seafevents.app.config import ENABLE_METADATA_MANAGEMENT, ENABLE_SEAFILE_AI


class App(object):
    def __init__(self, config, ccnet_config, seafile_config,
                 foreground_tasks_enabled=True,
                 background_tasks_enabled=True):
        self._fg_tasks_enabled = foreground_tasks_enabled
        self._bg_tasks_enabled = background_tasks_enabled

        if self._fg_tasks_enabled:
            init_message_handlers(config)
            self._events_handler = EventsHandler(config)
            self._count_traffic_task = CountTrafficInfo(config)
            self._update_login_record_task = CountUserActivity(config)
            self._seafevent_server = SeafEventServer(self, config)

        if self._bg_tasks_enabled:
            self._index_updater = IndexUpdater(config)
            self._seahub_email_sender = SeahubEmailSender(config)
            self._ldap_syncer = LdapSyncer(config, ccnet_config)
            self._virus_scanner = VirusScanner(config, seafile_config)
            self._statistics = Statistics(config, seafile_config)
            self._content_scanner = ContentScanner(config)
            self._work_weixin_notice_sender = WorkWinxinNoticeSender(config)
            self._file_updates_sender = FileUpdatesSender()
            self._repo_old_file_auto_del_scanner = RepoOldFileAutoDelScanner(config)
            self._deleted_files_count_cleaner = DeletedFilesCountCleaner(config)
            if ENABLE_METADATA_MANAGEMENT:
                self._index_master = RepoMetadataIndexMaster(config)
                self._index_worker = RepoMetadataIndexWorker(config)
            if ENABLE_SEAFILE_AI:
                self._sem_app = SemanticSearch()

    def serve_forever(self):
        if self._fg_tasks_enabled:
            self._events_handler.start()
            self._update_login_record_task.start()
            self._count_traffic_task.start()
            self._seafevent_server.start()

        if self._bg_tasks_enabled:
            self._file_updates_sender.start()
            self._work_weixin_notice_sender.start()
            self._index_updater.start()
            self._seahub_email_sender.start()
            self._ldap_syncer.start()
            self._virus_scanner.start()
            self._statistics.start()
            self._content_scanner.start()
            self._repo_old_file_auto_del_scanner.start()
            self._deleted_files_count_cleaner.start()
            if ENABLE_METADATA_MANAGEMENT:
                self._index_master.start()
                self._index_worker.start()
