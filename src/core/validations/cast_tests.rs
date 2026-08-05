mod tests {
    use serde::Deserialize;

    use crate::{
        core::validations,
        proto::{cast_add_body, embed, Embed},
        version::version::EngineVersion,
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

    // Committed sample of a `/v1/castsByFid` response. Previously this test fetched a random fid from
    // live production every run, which made it non-deterministic and flaky: it depended on prod
    // uptime, CI egress, and whatever cast data that random fid happened to have. The fixture pins a
    // representative mix of real casts — reply (parentCastId), channel cast (parentUrl), URL embed,
    // CastId embed, a plain root cast, mentions, and all three cast types (CAST, LONG_CAST,
    // TEN_K_CAST) — so the test exercises the validator itself rather than the network. See
    // NEYN-12730.
    const CASTS_FIXTURE: &str = include_str!("testdata/casts_by_fid.json");

    #[test]
    fn test_cast_validation() {
        let page = serde_json::from_str::<PagedResponse>(CASTS_FIXTURE).unwrap();
        assert!(
            !page.messages.is_empty(),
            "fixture should contain casts to validate"
        );
        for msg in page.messages {
            let Some(body) = msg.data.cast_add_body else {
                continue;
            };
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
                mentions_positions: body.mentions_positions.iter().map(|p| *p as u32).collect(),
                r#type: match body.cast_type.as_str() {
                    "CAST" => 0,
                    "LONG_CAST" => 1,
                    "TEN_K_CAST" => 2,
                    _ => 1,
                },
                parent: body.parent_cast_id.map(|p| {
                    cast_add_body::Parent::ParentCastId(crate::proto::CastId {
                        fid: p.fid,
                        hash: hex::decode(p.hash.replace("0x", "")).unwrap(),
                    })
                }),
            };
            // Assume pro user is true to avoid failures on casts with 10k characters or 4 embeds.
            // The fixtures predate the embed-limit change, so validate them at V19 — the version
            // that was live when they were signed.
            if let Err(err) =
                validations::cast::validate_cast_add_body(&cast, true, true, EngineVersion::V19)
            {
                panic!(
                    "Failed to validate cast: {:?} \
                     (text_len={}, embeds={}, embeds_deprecated={}, mentions={}, type={}, has_parent={})",
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
    }

    fn cast_with_embeds(count: usize) -> crate::proto::CastAddBody {
        crate::proto::CastAddBody {
            text: "hello".to_string(),
            embeds: (0..count)
                .map(|i| Embed {
                    embed: Some(embed::Embed::Url(format!("https://example.com/{i}"))),
                })
                .collect(),
            ..Default::default()
        }
    }

    fn validate_embeds(
        count: usize,
        is_pro_user: bool,
        version: EngineVersion,
    ) -> Result<(), validations::error::ValidationError> {
        validations::cast::validate_cast_add_body(
            &cast_with_embeds(count),
            false,
            is_pro_user,
            version,
        )
    }

    /// Pins both sides of the `IncreaseEmbedLimitForAllUsers` boundary. The pre-V20 row is what
    /// makes replay of existing history safe: a non-Pro cast with 3 embeds was invalid when it
    /// would have been signed, and must stay invalid forever at that version.
    #[test]
    fn embed_limit_is_four_for_everyone_from_v20() {
        use validations::error::ValidationError;

        // Below the boundary: Pro gets 4, everyone else gets 2.
        assert!(validate_embeds(2, false, EngineVersion::V19).is_ok());
        assert!(matches!(
            validate_embeds(3, false, EngineVersion::V19),
            Err(ValidationError::EmbedsExceedsLimit)
        ));
        assert!(validate_embeds(4, true, EngineVersion::V19).is_ok());
        assert!(matches!(
            validate_embeds(5, true, EngineVersion::V19),
            Err(ValidationError::EmbedsExceedsLimit)
        ));

        // At and above the boundary: 4 for everyone, Pro or not.
        for is_pro_user in [false, true] {
            assert!(validate_embeds(4, is_pro_user, EngineVersion::V20).is_ok());
            assert!(matches!(
                validate_embeds(5, is_pro_user, EngineVersion::V20),
                Err(ValidationError::EmbedsExceedsLimit)
            ));
            assert!(validate_embeds(4, is_pro_user, EngineVersion::latest()).is_ok());
        }
    }
}
