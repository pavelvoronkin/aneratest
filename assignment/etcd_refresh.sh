# please install etcd before running this
# brew install etcd on Mac
etcdctl put arb_bot/local "$(cat conf/app_config.local.json | xargs -0)"
etcdctl get arb_bot/local
