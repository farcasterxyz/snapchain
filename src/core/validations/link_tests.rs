mod tests {
    use serde::Deserialize;

    use crate::{core::validations, proto::link_body};

    #[derive(Deserialize)]
    struct Message {
        data: MessageData,
    }

    #[derive(Deserialize)]
    struct MessageData {
        #[serde(rename = "linkBody")]
        link_body: LinkBody,
    }

    #[derive(Deserialize)]
    struct LinkBody {
        #[serde(rename = "targetFid")]
        target_fid: u64,
        #[serde(rename = "type")]
        link_type: String,
    }

    #[derive(Deserialize)]
    struct PagedResponse {
        messages: Vec<Message>,
    }

    // Committed sample of a `/v1/linksByFid` response. Previously this test fetched a random fid from
    // live production every run, which made it non-deterministic and flaky: it depended on prod
    // uptime, CI egress, and whatever link data that random fid happened to have. The fixture pins a
    // representative mix of real links (`follow` plus a FIP-263 `block`) so the test exercises the
    // validator itself rather than the network. See NEYN-12730.
    const LINKS_FIXTURE: &str = include_str!("testdata/links_by_fid.json");

    #[test]
    fn test_link_validation() {
        let page = serde_json::from_str::<PagedResponse>(LINKS_FIXTURE).unwrap();
        assert!(
            !page.messages.is_empty(),
            "fixture should contain links to validate"
        );
        for msg in page.messages {
            let link = crate::proto::LinkBody {
                display_timestamp: None,
                r#type: msg.data.link_body.link_type,
                target: Some(link_body::Target::TargetFid(msg.data.link_body.target_fid)),
            };
            assert!(validations::link::validate_link_body(&link).is_ok())
        }
    }

    // FIP-263: "block" is valid by absence of restriction, not special-casing —
    // validate_link_type has no allowlist and accepts any 1-8 byte string.
    #[test]
    fn test_validate_link_type_accepts_block() {
        assert!(validations::link::validate_link_type("block").is_ok());
    }

    #[test]
    fn test_validate_link_body_accepts_block() {
        let block = crate::proto::LinkBody {
            display_timestamp: None,
            r#type: "block".to_string(),
            target: Some(link_body::Target::TargetFid(456)),
        };
        assert!(validations::link::validate_link_body(&block).is_ok());
    }
}
