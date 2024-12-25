# 文件树

已整理

## utils 工具函数

```
│   ├── ccnet_db.py 从 ccnet 查询用户和群组信息
│   ├── md2sdoc.py 将 Markdown 字符串转换为 sdoc 对象
│   ├── seafile_db.py 从 seafile 查询资料信息
│   └── seatable_api.py 从 Seatable 获取表格信息（dtable-server 表格行列增删, dtable-db 查询）
```



# 未整理

├── statistics
│   ├── counter.py
│   ├── db.py
│   ├── handlers.py
│   └── models.py


├── run.sh.template
├── seafevents_api.py
├── background_tasks.py
├── db.py
├── events.conf.template
├── main.py
├── mq.py
├── mysql.sql



├── app
│   ├── app.py
│   ├── config.py
│   ├── event_redis.py
│   ├── log.py
│   ├── mq_handler.py
│   └── signal_handler.py


├── batch_delete_files_notice
│   ├── db.py
│   ├── models.py
│   └── utils.py


├── content_scanner
│   ├── ali_scan.py
│   ├── content_scan.py
│   ├── db.py
│   ├── log.py
│   ├── main.py
│   ├── models.py
│   └── thread_pool.py


├── events
│   ├── change_file_path.py
│   ├── db.py
│   ├── handlers.py
│   └── models.py


├── events_publisher
│   └── handlers.py






├── repo_data
│   └── db.py


├── repo_metadata
│   ├── constants.py
│   ├── handlers.py
│   ├── image_embedding_api.py
│   ├── index_master.py
│   ├── index_worker.py
│   ├── metadata_manager.py
│   ├── metadata_server_api.py
│   ├── repo_metadata.py
│   ├── script
│   │   └── update_face_recognition.py
│   ├── slow_task_handler.py
│   ├── utils.py
│   └── view_data_sql.py

├── face_recognition
│   ├── constants.py
│   ├── face_cluster_updater.py
│   ├── face_recognition_manager.py
│   └── utils.py


├── ldap_syncer
│   ├── ldap_conn.py
│   ├── ldap_group_sync.py
│   ├── ldap_settings.py
│   ├── ldap_sync.py
│   ├── ldap_user_sync.py
│   ├── run_ldap_sync.py
│   └── utils.py


├── seafevent_server
│   ├── export_task_manager.py
│   ├── request_handler.py
│   ├── seafevent_server.py
│   ├── task_manager.py
│   └── utils.py


├── seasearch
│   ├── index_store
│   │   ├── index_manager.py
│   │   ├── repo_file_name_index.py
│   │   ├── repo_status_index.py
│   │   ├── wiki_index.py
│   │   └── wiki_status_index.py
│   ├── index_task
│   │   ├── filename_index_updater.py
│   │   ├── index_task_manager.py
│   │   └── wiki_index_updater.py
│   ├── script
│   │   ├── filename_index.sh.template
│   │   ├── portalocker
│   │   │   ├── portalocker.py
│   │   │   └── utils.py
│   │   ├── repo_filename_index_local.py
│   │   ├── wiki_index.sh.template
│   │   └── wiki_index_local.py
│   └── utils
│       ├── commit_differ.py
│       ├── constants.py
│       └── seasearch_api.py


├── tasks
│   ├── content_scanner.py
│   ├── deleted_files_count_cleaner.py
│   ├── es_wiki_index_updater.py
│   ├── face_cluster.py
│   ├── file_updates_sender.py
│   ├── index_updater.py
│   ├── ldap_syncer.py
│   ├── repo_old_file_auto_del_scanner.py
│   ├── seahub_email_sender.py
│   ├── statistics.py
│   ├── virus_scanner.py
│   └── work_weixin_notice_sender.py


├── tests
│   ├── README.md
│   ├── conftest.py
│   ├── db.conf
│   ├── events
│   │   ├── test_activity.py
│   │   ├── test_change_file_path.py
│   │   └── test_filehistory.py
│   ├── generate_table_sql.py
│   ├── ldap_syncer
│   │   ├── test_ldap_group_syncer.py
│   │   └── test_ldap_user_syncer.py
│   ├── raw_table_sql.sql
│   └── utils
│       ├── events_test_helper.py
│       ├── ldap_sync_test_helper.py
│       └── utils.py

└── virus_scanner
    ├── commit_differ.py
    ├── db_oper.py
    ├── models.py
    ├── run_virus_scan.py
    ├── scan_settings.py
    ├── thread_pool.py
    └── virus_scan.py
