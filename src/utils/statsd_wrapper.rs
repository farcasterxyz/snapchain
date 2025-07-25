use cadence::{Counted, Gauged, StatsdClient, Timed};
use std::sync::Arc;

pub struct StatsdClientWrapper {
    client: Arc<StatsdClient>,
    use_tags: bool,
}

impl Clone for StatsdClientWrapper {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            use_tags: self.use_tags,
        }
    }
}

impl StatsdClientWrapper {
    pub fn new(client: StatsdClient, use_tags: bool) -> Self {
        Self {
            client: Arc::new(client),
            use_tags,
        }
    }

    pub fn count_with_shard(
        &self,
        shard_id: u32,
        key: &str,
        value: u64,
        extra_tags: Vec<(&str, &str)>,
    ) {
        if self.use_tags {
            let shard_id_str = shard_id.to_string();
            let mut metric = self
                .client
                .count_with_tags(key, value)
                .with_tag("shard", &shard_id_str);
            for (key, value) in extra_tags {
                metric = metric.with_tag(key, value);
            }
            metric.send();
        } else {
            let key = format!("shard{}.{}", shard_id, key);
            _ = self.client.count(key.as_str(), value)
        }
    }

    pub fn count(&self, key: &str, value: i64) {
        _ = self.client.count(key, value)
    }

    pub fn gauge_with_shard(&self, shard_id: u32, key: &str, value: u64) {
        if self.use_tags {
            self.client
                .gauge_with_tags(key, value)
                .with_tag("shard", format!("{}", shard_id).as_str())
                .send()
        } else {
            let key = format!("shard{}.{}", shard_id, key);
            _ = self.client.gauge(key.as_str(), value)
        }
    }

    pub fn gauge(&self, key: &str, value: u64) {
        _ = self.client.gauge(key, value)
    }

    pub fn time_with_shard(&self, shard_id: u32, key: &str, value: u64) {
        if self.use_tags {
            self.client
                .time_with_tags(key, value)
                .with_tag("shard", format!("{}", shard_id).as_str())
                .send()
        } else {
            let key = format!("shard{}.{}", shard_id, key);
            _ = self.client.time(key.as_str(), value)
        }
    }

    pub fn time(&self, key: &str, value: u64) {
        _ = self.client.time(key, value)
    }
}
