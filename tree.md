# 文件树

已整理

## 01 utils 工具函数

```
│   ├── ccnet_db.py 从 ccnet 查询用户和群组信息
│   ├── md2sdoc.py 将 Markdown 字符串转换为 sdoc 对象
│   ├── seafile_db.py 从 seafile 查询资料信息
│   └── seatable_api.py 从 Seatable 获取表格信息（dtable-server 表格行列增删, dtable-db 查询）
```

## 02 statistics 统计流量日志

```
│   ├── counter.py 计数器，获取文件操作，流量统计，存储情况，用户登录情况计数，并写入数据库和日志中（对应 models 中定义的数据库模型）
│   ├── db.py 封装了一系列统计函数（获取用户，系统的流量，进一步调用 models 中的 UserActivityStat 数据库对象）
│   ├── handlers.py 用户事件和文件事件，触发回调函数，把文件操作写入数据库
│   └── models.py 用于存储统计数据的数据库模型，收集、处理和分析统计数据
```

## 03 batch delete files notice 批量删除文件通知
```
│   ├── db.py 保存/获取/清空已删除文件的数量
│   ├── models.py 数据库对象：已删除文件的数量 DeletedFilesCount
│   └── utils.py 计算删除文件数量，获取删除文件数量，每天保存删除文件信息到数据库
```

## 04 events publisher 资料库更新事件发布到 redis
```
├── events_publisher 
│   └── handlers.py 发布资料库更新事件到消息队列（Redis）
```

## 05 content_scanner 内容扫描模块（2018年）
```
│   ├── ali_scan.py 调用阿里云提供的病毒扫描接口
│   ├── content_scan.py 扫描内容（通过计算 repo 的 commit id，进行 diff 操作，判断文件增删改情况，然后调用 API 进行内容扫描）
│   ├── db.py 从数据库中记录获取扫描信息
│   ├── log.py 日志设置
│   ├── main.py: AppArgParser 用于解析内容扫描程序的命令行参数，获取 seafile 配置并开启内容扫描
│   ├── models.py 数据库模型：内容扫描记录和内容扫描结果
│   └── thread_pool.py 线程池
```



# 未整理

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


├── events
│   ├── change_file_path.py
│   ├── db.py
│   ├── handlers.py
│   └── models.py


├── ldap_syncer
│   ├── ldap_conn.py
│   ├── ldap_group_sync.py
│   ├── ldap_settings.py
│   ├── ldap_sync.py
│   ├── ldap_user_sync.py
│   ├── run_ldap_sync.py
│   └── utils.py




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
