# Architecture decision log and thoughts:
1. I decided to stick for tokio async impl to build io intensive app and support as many feeds as possible with least possible CPU
2. Async layer will emit events to a thread safe single thread reactor that will maintain state and do the calculation using plain old HashMap
    Pros: no concurrency
    Pros: we can propagate config changes as single message to reactor that will ensure config changes will be "picked up" in time ordered fashion
    Cons: under very high event frequency we might consider splitting it into multireactor, but still it will be feasible to do partitioning by feeds 
3. As the consequence of 2) i suggest to make reactor a good old thread that will burn core, so no reason stick to tokio here
    Pros: we can do pinning of a plain old thread
4. Using the same message propagation mechanism reactor will emit events to tokio enabled "downstream sender"
5. On startup when we fail to fetch or parse config we retry forever, the idea is we won't expose /health endpoint in this case, so k8s or whoever will decide app is unhealthy and flags it to devops
6. For tracing we can instrument crossbeam sender and receiver and tokio mpsc and export bucket metrics to prometheus endpoint  

# App has the following logical structure:

source1 
    - asset1, asset2, ... -> tokio task key = source1
    ...

source2
    - asset1, asset2, ... -> tokio task key = source2
    ...

Each task lives it's own life and can be controlled via flag enabled/disabled in config, also url and asset substitution within url supported

Within an asset app ensures that w11+w12+...=100 otherwise validation fails and config not applied

# Components:

async poller(tokio) -> reactor(single thread) -> downstream sender (tokio) 
                    -> persistence manager (tokio)

config change poller(tokio) -> propagates config changes if any to all respective components

# How to add new source?
 
You need to extend enum Source
```
#[derive(Debug, Clone, Copy, Deserialize, Display, PartialEq)]
pub enum Source {
Binance,
Uniswap,
}
```
Implement trait PriceFeed 

Ensure it's instantiated in method below:
```
fn new_price_feed(cfg: &OrderBookFeedConfig) -> Box<dyn OrderBookFeed> {
    match cfg.source {
        ...
    }
}
```
# How to build this demo?
Run ```build.sh```

# How to run this demo?

## Prerequisite 
Please install etcdctl to easily update etcd config via script

```
brew install etcdctl 
```

## Run demo
Run ```run_demo.sh```
It will start etcd and app containers and uploads config into it

## Run demo locally
If you prefer to play around with app and run it on your local machine instead 

1. Call ```run_demo.sh```
2. Shutdown order_book_collector container
3. Run ```cargo run``` 

# How to change feed and downstream configs on the fly?

simply edit ```app_config.local.json``` and call ```etcd_refresh.sh```
1. we can add feed by adding this for example
```
"feeds": [
    {
      "source": "Binance",
      "urlPattern": "wss://stream.binance.com/ws/btcusdt@depth/ethusdt@depth",
      "enabled": true
    }
  ]
```
2. we can remove feed 
3. we can disable or enable it by changing "enabled": true to false and vice versa 
4. we can change smoothing and weight
5. we do not support changing url on the flight, but however we support it via removing and adding feed 
6. we support changing downstream url on the flight
