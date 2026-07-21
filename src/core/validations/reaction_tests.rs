mod tests {
    use serde::Deserialize;

    use crate::core::validations;

    #[derive(Deserialize)]
    struct Message {
        data: MessageData,
    }

    #[derive(Deserialize)]
    struct MessageData {
        #[serde(rename = "reactionBody")]
        reaction_body: ReactionBody,
    }

    #[derive(Deserialize)]
    struct ReactionBody {
        #[serde(rename = "targetCastId")]
        target_cast_id: Option<CastId>,
        // Reactions can target a URL instead of a cast (e.g. likes on frames/links). This must be
        // deserialized too: dropping it makes a valid URL-target reaction look like it has no target,
        // which then trips `validate_reaction_body`'s `TargetIsMissing` branch. See NEYN-12728.
        #[serde(rename = "targetUrl")]
        target_url: Option<String>,
        #[serde(rename = "type")]
        reaction_type: String,
    }

    #[derive(Deserialize)]
    struct CastId {
        fid: u64,
        hash: String,
    }

    #[derive(Deserialize)]
    struct PagedResponse {
        messages: Vec<Message>,
    }

    // Committed sample of a `/v1/reactionsByFid` response. Previously this test fetched a random fid
    // from live production every run, which made it non-deterministic and flaky: it depended on prod
    // uptime, CI egress, and whatever reaction data that random fid happened to have. The fixture
    // pins a representative mix (cast-target and URL-target, LIKE and RECAST) so the test exercises
    // the validator itself rather than the network. See NEYN-12728.
    const REACTIONS_FIXTURE: &str = include_str!("testdata/reactions_by_fid.json");

    #[test]
    fn test_reaction_validation() {
        let page = serde_json::from_str::<PagedResponse>(REACTIONS_FIXTURE).unwrap();
        assert!(
            !page.messages.is_empty(),
            "fixture should contain reactions to validate"
        );
        for msg in page.messages {
            let body = msg.data.reaction_body;
            let target = match (body.target_cast_id, body.target_url) {
                (Some(cast_id), _) => Some(crate::proto::reaction_body::Target::TargetCastId(
                    crate::proto::CastId {
                        fid: cast_id.fid,
                        hash: hex::decode(cast_id.hash.replace("0x", "")).unwrap(),
                    },
                )),
                (None, Some(url)) => Some(crate::proto::reaction_body::Target::TargetUrl(url)),
                (None, None) => None,
            };
            let reaction = crate::proto::ReactionBody {
                // Map the JSON reaction-type string to its real proto enum value
                // (NONE=0, LIKE=1, RECAST=2). An earlier version hardcoded `LIKE => 0, else => 1`,
                // which mislabeled every type (LIKE validated as NONE, RECAST as LIKE) and never fed
                // RECAST to the validator at all. See #982.
                r#type: match body.reaction_type.as_str() {
                    "REACTION_TYPE_NONE" => crate::proto::ReactionType::None as i32,
                    "REACTION_TYPE_LIKE" => crate::proto::ReactionType::Like as i32,
                    "REACTION_TYPE_RECAST" => crate::proto::ReactionType::Recast as i32,
                    other => panic!("unexpected reaction type in fixture: {other}"),
                },
                target,
            };
            let result = validations::reaction::validate_reaction_body(&reaction);
            assert!(
                result.is_ok(),
                "validate_reaction_body failed: {:?}",
                result.unwrap_err()
            )
        }
    }
}
