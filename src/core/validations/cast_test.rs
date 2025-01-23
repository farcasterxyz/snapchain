mod tests {
    use rand::Rng;
    use serde::Deserialize;

    use crate::{
        core::validations,
        proto::{cast_add_body, embed, Embed},
    };

    #[derive(Deserialize)]
    struct Message {
        data: MessageData,
    }

    #[derive(Deserialize)]
    struct MessageData {
        #[serde(rename = "castAddBody")]
        cast_add_body: CastAddBody,
    }

    #[derive(Deserialize)]
    struct CastAddBody {
        #[serde(rename = "embedsDeprecated")]
        embeds_deprecated: Vec<String>,
        mentions: Vec<u64>,
        #[serde(rename = "parentCastId")]
        parent_cast_id: Option<ParentCastId>,
        text: String,
        #[serde(rename = "mentionsPositions")]
        mentions_positions: Vec<u64>,
        embeds: Vec<String>,
        #[serde(rename = "type")]
        cast_type: String,
    }

    #[derive(Deserialize)]
    struct ParentCastId {
        fid: u64,
        hash: String,
    }

    #[derive(Deserialize)]
    struct PagedResponse {
        messages: Vec<Message>,
    }

    #[tokio::test]
    async fn test_cast_validation() {
        let n: u32 = rand::thread_rng().gen::<u32>() % 10000;
        let resp = reqwest::get(format!(
            "https://hoyt.farcaster.xyz:2281/v1/castsByFid?fid={}",
            n
        ))
        .await;
        assert!(!resp.is_err());

        let response = resp.unwrap();
        let resp_json = response.text().await;

        let json = serde_json::from_str::<PagedResponse>(&resp_json.unwrap());
        let page = json.unwrap();
        for msg in page.messages {
            let cast = crate::proto::CastAddBody {
                embeds_deprecated: msg.data.cast_add_body.embeds_deprecated,
                mentions: msg.data.cast_add_body.mentions,
                embeds: msg
                    .data
                    .cast_add_body
                    .embeds
                    .into_iter()
                    .map(|e| Embed {
                        embed: Some(embed::Embed::Url(e)),
                    })
                    .collect(),
                text: msg.data.cast_add_body.text,
                mentions_positions: msg
                    .data
                    .cast_add_body
                    .mentions_positions
                    .iter()
                    .map(|p| *p as u32)
                    .collect(),
                r#type: if msg.data.cast_add_body.cast_type == "CAST" {
                    0
                } else {
                    1
                },
                parent: msg.data.cast_add_body.parent_cast_id.map(|p| {
                    cast_add_body::Parent::ParentCastId(crate::proto::CastId {
                        fid: p.fid,
                        hash: hex::decode(p.hash.replace("0x", "")).unwrap(),
                    })
                }),
            };
            assert!(validations::cast::validate_cast_add_body(&cast, true).is_ok())
        }
    }
}
