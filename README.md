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

## 测试环境

    # 创建新的python包环境
    virtualenv eevent
    source eevent/bin/activate

    # 安装依赖
    pip install -r tests/requirements.txt

    # 配置环境变量
    配置`tests/SetDev.sh`中的变量，其中PYTHONPATH需要包含seafobj，libevent等目录.

    # 配置数据库
    修改`tests/test.conf`文件中的配置, SEAHUBDB为seahub实际使用的数据库，SEAFEVENTSDB为seafevents实际使用的数据库， TESTDB为测试使用的数据库

    # 开始测试
    cd tests
    pytest -sv 
