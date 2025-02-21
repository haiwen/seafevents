# 文件树

已整理

一共 120 个 python 文件

## 00 seafevent 服务器

```
├── seafevent_server
│   ├── export_task_manager.py 管理事件日志导出和维基转换任务
│   ├── request_handler.py Flask处理HTTP请求（人脸识别，搜索，维基服务，上传进度查询等）
│   ├── seafevent_server.py 服务器入口
│   ├── task_manager.py 任务管理器（开始各种任务-线程池处理任务）
│   └── utils.py 工具函数（导入导出Excel，wiki转换等）
```

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

## 06 ldap_syncer: 实现 LDAP 同步器的功能，包括启动同步过程、处理同步结果、同步用户、同步群组等。
```
│   ├── ldap_sync.py：这是 LDAP 同步器的主程序，负责启动同步过程并处理同步结果。
│   ├── ldap_conn.py：这是 LDAP 连接器，负责建立和管理 LDAP 服务器连接。
│   ├── ldap_user_sync.py：这是用户同步器，负责同步 LDAP 服务器中的用户信息到 Seafile 中。
│   ├── ldap_group_sync.py：这是群组同步器，负责同步 LDAP 服务器中的组信息到 Seafile 中。
│   ├── utils.py：这是工具模块，包含了一些常用的函数和类，例如 bytes2str 函数和 get_group_uuid_pairs 函数。
│   ├── run_ldap_sync.py：这是一个命令行工具，用于启动 LDAP 同步器。
```

## 07 virus_scanner 病毒扫描（基于资料库 commit 做 diff，用线程池进行扫描）
```
├── commit_differ.py：比较两个文档树提交之间的差异，返回哪些文件需要进行病毒扫描。
├── db_oper.py：存储和检索病毒扫描结果
├── models.py：存储病毒扫描记录和病毒文件的数据库模型
├── run_virus_scan.py：病毒扫描程序的入口点
├── scan_settings.py：病毒扫描的配置设置，例如扫描命令、扫描间隔等
├── thread_pool.py：线程池，用于并行执行病毒扫描任务
└── virus_scan.py：病毒扫描的核心逻辑，负责执行病毒扫描和处理扫描结果
```

## 08 单元测试 tests
```
│   ├── README.md 测试步骤
│   ├── conftest.py 测试数据库准备和测试（多个数据库SQL语句生成等）
│   ├── db.conf 测试数据库配置（seahub+seafevent+test DB）
│   ├── generate_table_sql.py 生成表格的SQL函数（执行 raw_table_sql.sql）
│   ├── raw_table_sql.sql 测试SQL
│   ├── events——事件单元测试
│   │   ├── test_activity.py
│   │   ├── test_change_file_path.py
│   │   └── test_filehistory.py
│   ├── ldap_syncer——同步测试
│   │   ├── test_ldap_group_syncer.py
│   │   └── test_ldap_user_syncer.py
│   └── utils——测试工具函数
│       ├── events_test_helper.py
│       ├── ldap_sync_test_helper.py
│       └── utils.py 
```

## 09 seasearch

```
├── index_store 创建、更新和查询搜索索引的代码，以及处理文档存储和检索（资料库文件名，资料库状态，wiki 状态索引的存储）
│   ├── index_manager.py 索引管理入口文件：处理文件和维基的索引增加删除入口
│   ├── repo_file_name_index.py 核心函数：资料库文件和名称索引（文件增删后，索引也需要增删）
│   ├── repo_status_index.py：管理资料库状态索引
│   ├── wiki_index.py 维基索引：文件增删改后重新计算索引，同时搜索维基基于索引进行搜索
│   └── wiki_status_index.py：维基状态索引
├── index_task 索引任务的更新管理
│   ├── filename_index_updater.py 资料库索引更新
│   ├── index_task_manager.py 索引任务管理（转换配置，关键词搜索，维基搜索）
│   └── wiki_index_updater.py 维基索引更新（类似资料库索引更新）
├── script
│   ├── filename_index.sh.template 设置环境变量，根据命令行参数执行不同的操作（bash脚本）。
│   ├── portalocker 一个模块，实现跨平台的文件锁定
│   │   ├── portalocker.py
│   │   └── utils.py
│   ├── repo_filename_index_local.py 开始本地创建资料库文件名索引
│   ├── wiki_index.sh.template  设置环境变量，根据命令行参数执行不同的操作（bash脚本）。
│   └── wiki_index_local.py 开始本地创建维基索引
└── utils
    ├── commit_differ.py 工具函数，从两个 root commit 树中找到差异
    ├── constants.py 常量
    └── seasearch_api.py 文档和索引相关的 API
    └── __init__.py 其他工具函数（提取sdoc文本，处理md5）
```

## 10 app 功能入口、配置、日志、连接 redis 数据库 等
```
├── app
│   ├── app.py 入口函数：启动全部服务（前台服务和后台服务）
│   ├── config.py 获取设置，工具函数判断某个功能是否支持
│   ├── event_redis.py 连接redis数据库
│   ├── log.py 设置日志（不同功能不同日志）
│   ├── mq_handler.py 消息处理器，事件处理器
│   └── signal_handler.py 信号处理函数（未使用）
```

## 11 根目录：设置项，数据库，入口文件
```
├── run.sh.template: 一键启动脚本，设置环境变量，执行 main.py 开启服务
├── seafevents_api.py 处理文件后缀
├── background_tasks.py 运行后台任务的入口
├── db.py 数据库连接和配置
├── events.conf.template 配置文件模板
├── main.py 入口函数，启动 seafevents/app/app.py 文件的全部服务，设置后台任务或者前台任务
├── mq.py 连接 redis 数据库
├── mysql.sql 数据库语句
```

## 12 人脸识别 face recognition

具体调用其他的 AI API，这里只是接口，负责把资料库中的图片获取到，调用API识别结果，然后写入元数据表中

```
│   ├── face_cluster_updater.py 负责更新资料库中的面部聚类
│   ├── face_recognition_manager.py 负责管理人脸识别任务，包括人脸嵌入、聚类和更新人脸聚类。
│   └── utils.py 人脸图像识别相关工具函数
```

## 13 资料库信息 repo data 

```
├── repo_data 这部分代码应该比较早，都是原生实现 SQL 查询
│   ├── __init__.py 获取数据库中检索各种类型的资料库数据
│   └── db.py 链接数据库（判断配置是否正确，测试链接正常）
```

## 14 repo_metadata 资料库元数据处理

```
├── constants.py 不同类型元数据表格的常量（列类型，普通表格，人脸识别表格，图像 OCR 表格等）
├── handlers.py 元数据通过 redis 发布消息
├── image_embedding_api.py 图片服务器嵌入API（人脸识别功能）
├── metadata_server_api.py 元数据服务器 API-表格行列增删改查
├── script
│   └── update_face_recognition.py 更新人脸识别信息（清除旧表格的行，初始化配置，更新信息）
├── index_master.py 元数据的消息处理队列（监听 Redis 消息队列中的元数据更新，处理传入的消息，并根据需要更新待处理任务，例如人脸识别）
├── utils.py 图片视频信息获取，选项标签获取，生成 SQL 信息等
├── index_worker.py redis获取元数据信息，处理快速元数据任务，人脸识别信息提取。
├── slow_task_handler.py Redis 消息队列的处理器，处理慢速元数据任务。
└── view_data_sql.py ——内容很多，处理元数据支持SQL查询（排序过滤转换成sql语句等）不同列类型对应的 SQL 语句构建和查询，应该类似其他已有项目的逻辑，生成元数据视图的 SQL 查询语句。
```

## 15 events 事件处理回调函数（早期代码）

```
│   ├── change_file_path.py 用于处理文件路径在数据库中的变化
│   ├── db.py 事件处理相关数据库操作（获取用户事件，文件事件，查询数据库）
│   ├── handlers.py 核心函数：不同文件事件的处理函数（文件上传下载更新后，把文件活动和用户活动记录到数据库中）
│   └── models.py 数据库建表语句：用于存储和管理用户活动事件、用户事件和用户活动事件统计信息。
```

## 16 tasks 任务（这些模块基本类似，都是入口任务（处理配置，处理定时器）

```
├── __init__.py 模块入口函数（导出全部模块，各种扫描器等）
├── content_scanner.py 内容扫描函数
├── deleted_files_count_cleaner.py 已删除文件数量清理器
├── es_wiki_index_updater.py es维基索引更新（搜索任务）
├── face_cluster.py 人脸识别任务
├── file_updates_sender.py 文件更新发送器
├── index_updater.py 索引更新任务
├── ldap_syncer.py 同步器 ldap syncer
├── repo_old_file_auto_del_scanner.py 资料库旧文件自动删除扫描器
├── seahub_email_sender.py 发送邮件
├── statistics.py 统计（文件活动，用户活动，文件存储，流量控制）
├── virus_scanner.py 病毒扫描器
└── work_weixin_notice_sender.py 企业微信发送通知
```
