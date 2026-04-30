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
        cast_add_body: Option<CastAddBody>,
    }

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum EmbedUrlOrCastId {
        Url(EmbedUrl),
        CastId(EmbedCastId),
    }

    #[derive(Deserialize)]
    struct EmbedUrl {
        url: String,
    }

    #[derive(Deserialize)]
    struct EmbedCastId {
        #[serde(rename = "castId")]
        cast_id: CastId,
    }

    #[derive(Deserialize)]
    struct CastAddBody {
        #[serde(rename = "embedsDeprecated")]
        embeds_deprecated: Vec<String>,
        mentions: Vec<u64>,
        #[serde(rename = "parentCastId")]
        parent_cast_id: Option<CastId>,
        text: String,
        embeds: Vec<EmbedUrlOrCastId>,
        #[serde(rename = "mentionsPositions")]
        mentions_positions: Vec<u64>,
        #[serde(rename = "type")]
        cast_type: String,
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
    async fn test_cast_validation() {
        let n: u32 = rand::thread_rng().gen::<u32>() % 10000;
        let resp = reqwest::get(format!(
            "https://snap.farcaster.xyz:3381/v1/castsByFid?fid={}",
            n
        ))
        .await;
        assert!(!resp.is_err());

        let response = resp.unwrap();
        let resp_json = response.text().await.unwrap();

        let json = serde_json::from_str::<PagedResponse>(&resp_json);
        let page = json.unwrap();
        for msg in page.messages {
            match msg.data.cast_add_body {
                Some(body) => {
                    let cast = crate::proto::CastAddBody {
                        embeds_deprecated: body.embeds_deprecated,
                        mentions: body.mentions,
                        embeds: body
                            .embeds
                            .into_iter()
                            .map(|e| match e {
                                EmbedUrlOrCastId::Url(url) => Embed {
                                    embed: Some(embed::Embed::Url(url.url)),
                                },
                                EmbedUrlOrCastId::CastId(cast_id) => Embed {
                                    embed: Some(embed::Embed::CastId(crate::proto::CastId {
                                        fid: cast_id.cast_id.fid,
                                        hash: hex::decode(&cast_id.cast_id.hash[2..]).unwrap(),
                                    })),
                                },
                            })
                            .collect(),
                        text: body.text,
                        mentions_positions: body
                            .mentions_positions
                            .iter()
                            .map(|p| *p as u32)
                            .collect(),
                        r#type: match body.cast_type.as_str() {
                            "CAST" => 0,
                            "LONG_CAST" => 1,
                            "TEN_K_CAST" => 2,
                            other => panic!("unknown cast type from API: {}", other),
                        },
                        parent: body.parent_cast_id.map(|p| {
                            cast_add_body::Parent::ParentCastId(crate::proto::CastId {
                                fid: p.fid,
                                hash: hex::decode(p.hash.replace("0x", "")).unwrap(),
                            })
                        }),
                    };
                    // Assume pro user is true to avoid failures on casts with 10k characters or 4 embeds.
                    if let Err(err) = validations::cast::validate_cast_add_body(&cast, true, true) {
                        panic!(
                            "Failed to validate cast for fid={}: {:?} \
                             (text_len={}, embeds={}, embeds_deprecated={}, mentions={}, type={}, has_parent={})",
                            n,
                            err,
                            cast.text.len(),
                            cast.embeds.len(),
                            cast.embeds_deprecated.len(),
                            cast.mentions.len(),
                            cast.r#type,
                            cast.parent.is_some(),
                        );
                    }
                }
                None => {}
            }
        }
    }
}
