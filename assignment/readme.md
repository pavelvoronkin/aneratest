# Architecture decision log and thoughts:
1. I decided to stick for tokio async impl to build io intensive app and support as many feeds as possible with least possible CPU
2. Async layer will emit events to a thread safe single thread reactor that will maintain state and do the calculation magic using plain old HashMap
    2.1. Pros: no concurrency
    Pros: we can propagate config changes as single message to reactor that will ensure config changes will be "picked up" in time ordered fashion
    Cons: under very high event frequency we might consider splitting it into multireactor, but still it will be feasible to do partitioning by feeds 
3. As the consequence of 2) i suggest to make reactor a good old thread that will burn core, so no reason stick to tokio here
    Pros: we can do pinning of a plain old thread
4. Using the same message propagation mechanism reactor will emit index events to tokio enabled "downstream sender"
5. Async layer will also emit events to "persistence_manager", i've made a stub for it

# App has the following logical structure:

asset1 
    - source1, w11 -> tokio task key = assert1_source1
    - source2, w12 -> tokio task key = assert1_source1
    ...

asset2
    - source1, w21 -> tokio task assert2_source1
    - source2, w22 -> tokio task assert2_source1
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
Coinbase,
Kraken,
}
```
Implement trait PriceFeed 

Ensure it's instantiated in method below:
```
fn new_price_feed(cfg: &PriceFeedConfig) -> Box<dyn PriceFeed> {
    match cfg.source {
        ...
    }
}
```

# How to run the app?

```
cargo run
```

app assumes default ```ENV=local``` meaning that ```app_config.local.json``` will be used

You can specify ```ENV=qa``` and create file accordingly ```app_config.qa.json``` etc.

# How to change price feed and downstream configs on the fly?

simply edit ```app_config.local.json``` app will pick up changes with subsecond delay or another config depending ENV
1. we can add price feed by adding this for example
```
"BTC": [
    {
        "source": "Coinbase",
        "asset": "BTC",
        "smoothing": "EMA",
        "weight": 100,
        "urlPattern": "https://api.coinbase.com/v2/exchange-rates?currency={{asset}}",
        "enabled": true
    }
]
```
2. we can remove feed 
3. we can enable or disable it by changing "enabled": true to false and vice versa 
4. we can change smoothing and weight 
5. we do not support changing url on the flight, but however we support it via removing and adding feed 
6. we support changing downstream url on the flight
