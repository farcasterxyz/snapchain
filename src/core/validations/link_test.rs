mod tests {
    use rand::Rng;
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

    #[tokio::test]
    async fn test_link_validation() {
        let n: u32 = rand::thread_rng().gen::<u32>() % 10000;
        let resp = reqwest::get(format!(
            "https://hoyt.farcaster.xyz:2281/v1/linksByFid?fid={}",
            n
        ))
        .await;
        assert!(!resp.is_err());

        let response = resp.unwrap();
        let resp_json = response.text().await;

        let json = serde_json::from_str::<PagedResponse>(&resp_json.unwrap());
        let page = json.unwrap();
        for msg in page.messages {
            let link = crate::proto::LinkBody {
                display_timestamp: None,
                r#type: msg.data.link_body.link_type,
                target: Some(link_body::Target::TargetFid(msg.data.link_body.target_fid)),
            };
            assert!(validations::link::validate_link_body(&link).is_ok())
        }
    }
}
