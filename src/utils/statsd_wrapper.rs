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
        mut tags: Vec<(String, String)>,
    ) {
        if self.use_tags {
            tags.push(("shard".to_string(), format!("{}", shard_id).to_string()));
            let mut count = self.client.count_with_tags(key, value);
            let tags = tags
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str()));
            for (key, value) in tags {
                count = count.with_tag(key, value);
            }
            count.send()
        } else {
            let key = format!("shard{}.{}", shard_id, key);
            _ = self.client.count(key.as_str(), value)
        }
    }

    pub fn count(&self, key: &str, value: u64) {
        _ = self.client.count(key, value)
    }

    pub fn gauge_with_shard(
        &self,
        shard_id: u32,
        key: &str,
        value: u64,
        mut tags: Vec<(String, String)>,
    ) {
        if self.use_tags {
            tags.push(("shard".to_string(), format!("{}", shard_id).to_string()));
            let mut gauge = self.client.gauge_with_tags(key, value);
            let tags = tags
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str()));
            for (key, value) in tags {
                gauge = gauge.with_tag(key, value);
            }
            gauge.send();
        } else {
            let key = format!("shard{}.{}", shard_id, key);
            _ = self.client.gauge(key.as_str(), value)
        }
    }

    pub fn gauge(&self, key: &str, value: u64) {
        _ = self.client.gauge(key, value)
    }

    pub fn time(&self, key: &str, value: u64) {
        _ = self.client.time(key, value)
    }
}
