mod tests {
    use rand::Rng;
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

    #[tokio::test]
    async fn test_reaction_validation() {
        let n: u32 = rand::thread_rng().gen::<u32>() % 10000;
        let resp = reqwest::get(format!(
            "https://hoyt.farcaster.xyz:2281/v1/reactionsByFid?fid={}",
            n
        ))
        .await;
        assert!(!resp.is_err());

        let response = resp.unwrap();
        let resp_json = response.text().await;

        let json = serde_json::from_str::<PagedResponse>(&resp_json.unwrap());
        let page = json.unwrap();
        for msg in page.messages {
            let reaction = crate::proto::ReactionBody {
                r#type: if msg.data.reaction_body.reaction_type == "REACTION_TYPE_LIKE" {
                    0
                } else {
                    1
                },
                target: msg.data.reaction_body.target_cast_id.map(|p| {
                    crate::proto::reaction_body::Target::TargetCastId(crate::proto::CastId {
                        fid: p.fid,
                        hash: hex::decode(p.hash.replace("0x", "")).unwrap(),
                    })
                }),
            };
            assert!(validations::reaction::validate_reaction_body(&reaction).is_ok())
        }
    }
}
