# please install etcd before running this
# brew install etcd on Mac
etcdctl put order_book/local "$(cat conf/app_config.local.json | xargs -0)"
etcdctl get order_book/local
