use crate::{
    consensus::proposer::ProposalSource, proto::Transaction,
    utils::statsd_wrapper::StatsdClientWrapper,
};

pub struct Metrics {
    pub statsd_client: StatsdClientWrapper,
    pub shard_id: u32,
}

struct TransactionCounts {
    transactions: u64,
    user_messages: u64,
    system_messages: u64,
}

impl Metrics {
    // statsd
    pub fn count(&self, key: &str, count: u64, extra_tags: Vec<(&str, &str)>) {
        let key = format!("engine.{}", key);
        self.statsd_client
            .count_with_shard(self.shard_id, key.as_str(), count, extra_tags);
    }

    // statsd
    pub fn gauge(&self, key: &str, value: u64) {
        let key = format!("engine.{}", key);
        self.statsd_client
            .gauge_with_shard(self.shard_id, key.as_str(), value);
    }

    pub fn time_with_shard(&self, key: &str, value: u64) {
        let key = format!("engine.{}", key);
        self.statsd_client
            .time_with_shard(self.shard_id, key.as_str(), value);
    }

    pub fn make_count_fn(&self) -> impl Fn(&str, u64, Vec<(&str, &str)>) {
        let statsd_client = self.statsd_client.clone();
        let shard_id = self.shard_id.clone();
        move |key: &str, count: u64, extra_tags: Vec<(&str, &str)>| {
            let key = format!("engine.{}", key);
            statsd_client.count_with_shard(shard_id, &key, count, extra_tags);
        }
    }

    pub fn proposal_source_tags(source: ProposalSource) -> Vec<(&'static str, &'static str)> {
        let source = match source {
            ProposalSource::Simulate => "simulate",
            ProposalSource::Propose => "propose",
            ProposalSource::Validate => "validate",
            ProposalSource::Commit => "commit",
            ProposalSource::Replication => "replication",
        };

        vec![("source", source)]
    }

    pub fn publish_transaction_counts(
        &self,
        txns: &[Transaction],
        proposal_source: ProposalSource,
    ) {
        let (user_count, system_count) =
            txns.iter().fold((0, 0), |(user_count, system_count), tx| {
                (
                    user_count + tx.user_messages.len(),
                    system_count + tx.system_messages.len(),
                )
            });

        let count = TransactionCounts {
            transactions: txns.len() as u64,
            user_messages: user_count as u64,
            system_messages: system_count as u64,
        };

        self.count(
            "transactions",
            count.transactions,
            Self::proposal_source_tags(proposal_source),
        );
        self.count(
            "user_messages",
            count.user_messages,
            Self::proposal_source_tags(proposal_source),
        );
        self.count(
            "system_messages",
            count.system_messages,
            Self::proposal_source_tags(proposal_source),
        );
    }
}
