# please install etcd before running this
# brew install etcd on Mac
etcdctl put index_collector/local "$(cat conf/app_config.local.json | xargs -0)"
etcdctl get index_collector/local