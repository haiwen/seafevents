## 配置events.conf


参考events.conf.template，数据库选择sqlite3 或者mysql
修改seafesdir为seafes所在的目录

在INDEX FILES 下加上

	enabled=true

### Audit
    Audit monitor is disabled default, if you want to enable this function, add follow option in events.conf.
    ```
    [Audit]
    enable = True
    ```

##运行

	cp main.py seafile_events.py
	cp run.sh.tempalte run.sh
	修改run.sh中的CCNET_CONF_DIR 和SEAFILE_CONF_DIR
