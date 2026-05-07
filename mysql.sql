CREATE TABLE IF NOT EXISTS `ContentScanRecord` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `repo_id` varchar(36) NOT NULL,
  `commit_id` varchar(40) NOT NULL,
  `timestamp` datetime NOT NULL,
  PRIMARY KEY (`id`),
  KEY `ix_ContentScanRecord_repo_id` (`repo_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `ContentScanResult` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `repo_id` varchar(36) NOT NULL,
  `path` text NOT NULL,
  `platform` varchar(32) NOT NULL,
  `detail` text NOT NULL,
  PRIMARY KEY (`id`),
  KEY `ix_ContentScanResult_repo_id` (`repo_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `Activity` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `op_type` varchar(128) NOT NULL,
  `op_user` varchar(255) NOT NULL,
  `obj_type` varchar(128) NOT NULL,
  `timestamp` datetime NOT NULL,
  `repo_id` varchar(36) NOT NULL,
  `commit_id` varchar(40) DEFAULT NULL,
  `path` text NOT NULL,
  `detail` text NOT NULL,
  PRIMARY KEY (`id`),
  KEY `ix_Activity_timestamp` (`timestamp`),
  KEY `idx_activity_repo_timestamp` (`repo_id`, `timestamp`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `UserActivity` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `username` varchar(255) NOT NULL,
  `activity_id` bigint(20) DEFAULT NULL,
  `timestamp` datetime NOT NULL,
  PRIMARY KEY (`id`),
  KEY `activity_id` (`activity_id`),
  KEY `ix_UserActivity_timestamp` (`timestamp`),
  KEY `idx_username_timestamp` (`username`,`timestamp`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `FileHistory` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `op_type` varchar(128) NOT NULL,
  `op_user` varchar(255) NOT NULL,
  `timestamp` datetime NOT NULL,
  `repo_id` varchar(36) NOT NULL,
  `commit_id` varchar(40) DEFAULT NULL,
  `file_id` varchar(40) NOT NULL,
  `file_uuid` varchar(40) DEFAULT NULL,
  `path` text NOT NULL,
  `repo_id_path_md5` varchar(32) DEFAULT NULL,
  `size` bigint(20) NOT NULL,
  `old_path` text NOT NULL,
  PRIMARY KEY (`id`),
  KEY `ix_FileHistory_file_uuid` (`file_uuid`),
  KEY `ix_FileHistory_repo_id_path_md5` (`repo_id_path_md5`),
  KEY `ix_FileHistory_timestamp` (`timestamp`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `FileAudit` (
  `eid` bigint(20) NOT NULL AUTO_INCREMENT,
  `timestamp` datetime NOT NULL,
  `etype` varchar(128) NOT NULL,
  `user` varchar(255) NOT NULL,
  `ip` varchar(45) NOT NULL,
  `device` text NOT NULL,
  `org_id` int(11) NOT NULL,
  `repo_id` varchar(36) NOT NULL,
  `file_path` text NOT NULL,
  PRIMARY KEY (`eid`),
  KEY `idx_file_audit_user_orgid_eid` (`user`,`org_id`,`eid`),
  KEY `idx_file_audit_repo_org_eid` (`repo_id`,`org_id`,`eid`),
  KEY `idx_file_audit_orgid_eid` (`org_id`,`eid`),
  KEY `ix_FileAudit_timestamp` (`timestamp`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `FileUpdate` (
  `eid` bigint(20) NOT NULL AUTO_INCREMENT,
  `timestamp` datetime NOT NULL,
  `user` varchar(255) NOT NULL,
  `org_id` int(11) NOT NULL,
  `repo_id` varchar(36) NOT NULL,
  `commit_id` varchar(40) NOT NULL,
  `file_oper` text NOT NULL,
  PRIMARY KEY (`eid`),
  KEY `idx_file_update_user_orgid_eid` (`user`,`org_id`,`eid`),
  KEY `idx_file_update_orgid_eid` (`org_id`,`eid`),
  KEY `ix_FileUpdate_timestamp` (`timestamp`),
  KEY `idx_file_update_repo_org_eid` (`repo_id`,`org_id`,`eid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `PermAudit` (
  `eid` bigint(20) NOT NULL AUTO_INCREMENT,
  `timestamp` datetime NOT NULL,
  `etype` varchar(128) NOT NULL,
  `from_user` varchar(255) NOT NULL,
  `to` varchar(255) NOT NULL,
  `org_id` int(11) NOT NULL,
  `repo_id` varchar(36) NOT NULL,
  `file_path` text NOT NULL,
  `permission` varchar(15) NOT NULL,
  PRIMARY KEY (`eid`),
  KEY `idx_perm_audit_repo_org_eid` (`repo_id`,`org_id`,`eid`),
  KEY `idx_perm_audit_orgid_eid` (`org_id`,`eid`),
  KEY `ix_perm_audit_timestamp` (`timestamp`),
  KEY `idx_perm_audit_user_orgid_eid` (`from_user`,`org_id`,`eid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `TotalStorageStat` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `timestamp` datetime NOT NULL,
  `total_size` bigint(20) NOT NULL,
  `org_id` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_storage_time_org` (`timestamp`,`org_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `FileOpsStat` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `timestamp` datetime NOT NULL,
  `op_type` varchar(16) NOT NULL,
  `number` int(11) NOT NULL,
  `org_id` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_file_ops_org_time` (`org_id`,`timestamp`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `UserActivityStat` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name_time_md5` varchar(32) DEFAULT NULL,
  `username` varchar(255) DEFAULT NULL,
  `timestamp` datetime NOT NULL,
  `org_id` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name_time_md5` (`name_time_md5`),
  KEY `idx_activity_time_org` (`timestamp`,`org_id`),
  KEY `ix_UserActivityStat_timestamp` (`timestamp`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `UserTraffic` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `user` varchar(255) NOT NULL,
  `org_id` int(11) DEFAULT NULL,
  `timestamp` datetime NOT NULL,
  `op_type` varchar(48) NOT NULL,
  `size` bigint(20) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `ix_UserTraffic_org_id` (`org_id`),
  KEY `idx_traffic_time_user` (`timestamp`,`user`,`org_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `MonthlyUserTraffic` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `user` varchar(255) NOT NULL,
  `org_id` int(11) DEFAULT NULL,
  `timestamp` datetime NOT NULL,
  `web_file_upload` bigint(20) NOT NULL,
  `web_file_download` bigint(20) NOT NULL,
  `sync_file_upload` bigint(20) NOT NULL,
  `sync_file_download` bigint(20) NOT NULL,
  `link_file_upload` bigint(20) NOT NULL,
  `link_file_download` bigint(20) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_monthlyusertraffic_time_org_user` (`timestamp`,`user`,`org_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `SysTraffic` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `org_id` int(11) DEFAULT NULL,
  `timestamp` datetime NOT NULL,
  `op_type` varchar(48) NOT NULL,
  `size` bigint(20) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_systraffic_time_org` (`timestamp`,`org_id`),
  KEY `ix_SysTraffic_org_id` (`org_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `MonthlySysTraffic` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `org_id` int(11) DEFAULT NULL,
  `timestamp` datetime NOT NULL,
  `web_file_upload` bigint(20) NOT NULL,
  `web_file_download` bigint(20) NOT NULL,
  `sync_file_upload` bigint(20) NOT NULL,
  `sync_file_download` bigint(20) NOT NULL,
  `link_file_upload` bigint(20) NOT NULL,
  `link_file_download` bigint(20) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_monthlysystraffic_time_org` (`timestamp`,`org_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `VirusScanRecord` (
  `repo_id` varchar(36) NOT NULL,
  `scan_commit_id` varchar(40) NOT NULL,
  PRIMARY KEY (`repo_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `VirusFile` (
  `vid` int(11) NOT NULL AUTO_INCREMENT,
  `repo_id` varchar(36) NOT NULL,
  `commit_id` varchar(40) NOT NULL,
  `file_path` text NOT NULL,
  `has_deleted` tinyint(1) NOT NULL,
  `has_ignored` tinyint(1) NOT NULL,
  PRIMARY KEY (`vid`),
  KEY `ix_VirusFile_repo_id` (`repo_id`),
  KEY `ix_VirusFile_has_ignored` (`has_ignored`),
  KEY `ix_VirusFile_has_deleted` (`has_deleted`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `GroupIdLDAPUuidPair` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `group_id` int(11) NOT NULL,
  `group_uuid` varchar(36) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `group_id` (`group_id`),
  UNIQUE KEY `group_uuid` (`group_uuid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `user_quota_usage` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `username` varchar(255) NOT NULL,
  `org_id` int(11) NOT NULL,
  `quota` bigint(20) DEFAULT NULL,
  `usage` bigint(20) DEFAULT NULL,
  `timestamp` datetime NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_user_quota_usage_username` (`username`),
  KEY `idx_user_quota_usage_timestamp` (`timestamp`),
  KEY `idx_user_quota_usage_org_id` (`org_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

CREATE TABLE IF NOT EXISTS `org_quota_usage` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `org_id` int(11) NOT NULL,
  `quota` bigint(20) DEFAULT NULL,
  `usage` bigint(20) DEFAULT NULL,
  `timestamp` datetime NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_org_quota_usage_org_id` (`org_id`),
  KEY `idx_org_quota_usage_timestamp` (`timestamp`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

CREATE TABLE IF NOT EXISTS `sysadmin_extra_userloginlog` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `username` varchar(255) NOT NULL,
  `login_date` datetime NOT NULL,
  `login_ip` varchar(128) NOT NULL,
  `login_success` tinyint(1) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `sysadmin_extra_userloginlog_username_5748b9e3` (`username`),
  KEY `sysadmin_extra_userloginlog_login_date_c171d790` (`login_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `FileTrash` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `user` varchar(255) NOT NULL,
  `obj_type` varchar(10) NOT NULL,
  `obj_id` varchar(40) NOT NULL,
  `obj_name` varchar(255) NOT NULL,
  `delete_time` datetime NOT NULL,
  `repo_id` varchar(36) NOT NULL,
  `commit_id` varchar(40) DEFAULT NULL,
  `path` text NOT NULL,
  `size` bigint(20) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_filetrash_repo_delete_time` (`repo_id`, `delete_time`),
  KEY `idx_filetrash_delete_time` (`delete_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

CREATE TABLE IF NOT EXISTS `org_last_active_time` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `org_id` int(11) NOT NULL,
  `timestamp` datetime(6) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `org_id` (`org_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

CREATE TABLE IF NOT EXISTS `webhook_jobs` (
`id` int(11) NOT NULL AUTO_INCREMENT,
`webhook_id` int(11) NOT NULL,
`created_at` datetime,
`trigger_at` datetime DEFAULT NULL,
`status` tinyint(1) DEFAULT NULL,
`url` varchar(2000) NOT NULL,
`request_headers` text DEFAULT NULL,
`request_body` text,
`response_status` int(5) DEFAULT NULL,
`response_body` longtext DEFAULT NULL,
PRIMARY KEY (`id`),
KEY `webhook_id_key` (`webhook_id`),
KEY `status_b7n3m0x1_key` (`status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci ;

CREATE TABLE IF NOT EXISTS `webhooks` (
`id` int(11) unsigned NOT NULL AUTO_INCREMENT,
`repo_id` varchar(36) NOT NULL,
`url` varchar(2000) NOT NULL,
`settings` text DEFAULT NULL,
`creator` varchar(255) NOT NULL,
`created_at` datetime,
`is_valid` tinyint(1) DEFAULT 1,
PRIMARY KEY (`id`),
KEY `repo_id_key` (`repo_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
