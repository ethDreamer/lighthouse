use crate::{
    DEFAULT_USER_AGENT, Error, JSON_ACCEPT_VALUE, PREFERENCE_ACCEPT_VALUE, content_type_from_header,
};
use bls::PublicKeyBytes;
use eth2::types::{
    BuilderPreferencesRequestV1, ContentType, EthSpec, ExecutionBlockHash, ForkVersionedResponse,
    Hash256, SignedBeaconBlock, SignedExecutionPayloadBid, SignedRequestAuthV1, Slot,
};
use eth2::{
    CONSENSUS_VERSION_HEADER, CONTENT_TYPE_HEADER, JSON_CONTENT_TYPE_HEADER,
    SSZ_CONTENT_TYPE_HEADER, ok_or_error, success_or_error,
};
use reqwest::StatusCode;
use reqwest::header::{ACCEPT, HeaderMap, HeaderName, HeaderValue};
use sensitive_url::SensitiveUrl;
use ssz::{Decode, Encode};
use std::time::Duration;
use tracing::warn;

/// This is a whole rabbithole.. see discussion:
/// https://discord.com/channels/595666850260713488/874767108809031740/1529125867484348577
pub const DEFAULT_GET_EXECUTION_PAYLOAD_BID_TIMEOUT_MILLIS: u64 = 400;

/// Default timeout for builder submit requests (preferences and signed block).
pub const DEFAULT_SUBMIT_TIMEOUT_MILLIS: u64 = 1000;

/// Header advertising the proposer's request timeout (in milliseconds) to the builder.
const X_TIMEOUT_MS: HeaderName = HeaderName::from_static("x-timeout-ms");
/// Header carrying the Unix send-time (in milliseconds) so the builder can measure latency.
const DATE_MILLISECONDS: HeaderName = HeaderName::from_static("date-milliseconds");

/// A client for the Gloas (ePBS) Builder API.
///
/// This client is **not** bound to a single builder URL and holds **no** per-connection state:
/// every request takes the target `builder_url` as a parameter, so one instance can fan out to any
/// number of builders. SSZ negotiation is done per-request rather than cached, because in Gloas the
/// bid request and the signed-block submission are separated by a full VC round-trip
/// (produce -> sign -> publish) and so cannot share instance state.
#[derive(Clone)]
pub struct BuilderHttpClient {
    client: reqwest::Client,
    user_agent: String,
    /// Only use json for all request/response types.
    disable_ssz: bool,
}

/// The successful response from a builder's `getExecutionPayloadBid` endpoint.
pub struct GloasBidResponse<E: EthSpec> {
    /// The signed bid returned by the builder.
    pub bid: SignedExecutionPayloadBid<E>,
    /// Whether the builder served the bid encoded as SSZ.
    ///
    /// Carry this into the winning-builder provenance so the follow-up
    /// [`submit_signed_beacon_block`](BuilderHttpClient::submit_signed_beacon_block) can reuse the
    /// encoding the builder just demonstrated it supports, rather than probing again.
    pub ssz_response: bool,
}

impl BuilderHttpClient {
    pub fn new(user_agent: Option<String>, disable_ssz: bool) -> Result<Self, Error> {
        let user_agent = user_agent.unwrap_or_else(|| DEFAULT_USER_AGENT.to_string());
        let client = reqwest::Client::builder().user_agent(&user_agent).build()?;
        Ok(Self {
            client,
            user_agent,
            disable_ssz,
        })
    }

    pub fn get_user_agent(&self) -> &str {
        &self.user_agent
    }

    /// Build the HTTP headers sent with a `getExecutionPayloadBid` request.
    ///
    /// Sets three headers:
    /// - `Accept`: requests SSZ (with JSON fallback) for the response, or JSON only when
    ///   `disable_ssz` is set. This governs the (larger) bid response encoding only.
    /// - `X-Timeout-Ms`: advertises our request timeout so the builder can bound its own work.
    /// - `Date-Milliseconds`: the Unix ms send time, letting the builder measure one-way latency.
    ///
    /// A header that cannot be constructed is logged and skipped rather than failing the request,
    /// since all three are optional per the builder spec.
    fn compute_get_execution_payload_bid_headers(&self) -> HeaderMap {
        let mut headers = HeaderMap::new();

        let accept_value = if self.disable_ssz {
            JSON_ACCEPT_VALUE
        } else {
            PREFERENCE_ACCEPT_VALUE
        };

        match HeaderValue::from_str(accept_value) {
            Ok(accept_header) => {
                headers.insert(ACCEPT, accept_header);
            }
            Err(e) => {
                warn!("Invalid accept value: {}", e);
            }
        }

        // Advertise our timeout to the builder so it can bound its own work.
        headers.insert(
            X_TIMEOUT_MS,
            HeaderValue::from(DEFAULT_GET_EXECUTION_PAYLOAD_BID_TIMEOUT_MILLIS),
        );

        // Timestamp the request (Unix ms) so the builder can measure one-way latency.
        match std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
        {
            Ok(now_millis) => {
                headers.insert(DATE_MILLISECONDS, HeaderValue::from(now_millis));
            }
            Err(e) => {
                warn!("Failed to compute date header: {}", e);
            }
        }

        headers
    }

    /// `POST /eth/v1/builder/execution_payload_bid/{slot}/{parent_hash}/{parent_root}/{proposer_pubkey}`
    ///
    /// Request a bid from a single builder. Returns `Ok(None)` if the builder has no bid available
    /// (HTTP 204).
    ///
    /// The optional `SignedRequestAuthV1` body is small and always sent as JSON; SSZ is only
    /// negotiated for the (larger) response via the `Accept` header. The response's encoding is
    /// reported via [`GloasBidResponse::ssz_response`].
    #[allow(clippy::too_many_arguments)]
    pub async fn get_execution_payload_bid<E: EthSpec>(
        &self,
        builder_url: &SensitiveUrl,
        slot: Slot,
        parent_hash: ExecutionBlockHash,
        parent_root: Hash256,
        proposer_pubkey: &PublicKeyBytes,
        signed_request_auth: Option<&SignedRequestAuthV1>,
    ) -> Result<Option<GloasBidResponse<E>>, Error> {
        let mut path = builder_url.expose_full().clone();

        path.path_segments_mut()
            .map_err(|()| Error::InvalidUrl(builder_url.clone()))?
            .push("eth")
            .push("v1")
            .push("builder")
            .push("execution_payload_bid")
            .push(slot.to_string().as_str())
            .push(format!("{parent_hash:?}").as_str())
            .push(format!("{parent_root:?}").as_str())
            .push(proposer_pubkey.as_hex_string().as_str());

        let timeout = Duration::from_millis(DEFAULT_GET_EXECUTION_PAYLOAD_BID_TIMEOUT_MILLIS);
        let headers = self.compute_get_execution_payload_bid_headers();
        let mut request = self.client.post(path).timeout(timeout).headers(headers);

        // The auth body is tiny; always send it as JSON. SSZ-encoding it buys nothing and avoids
        // having to probe the builder's SSZ request-ingest support.
        if let Some(auth) = signed_request_auth {
            request = request.json(auth);
        }

        let response = ok_or_error(request.send().await.map_err(Error::from)?).await?;

        if response.status() == StatusCode::NO_CONTENT {
            return Ok(None);
        }

        let response_headers = response.headers().clone();
        let response_bytes = response.bytes().await?;

        match content_type_from_header(&response_headers) {
            ContentType::Ssz => {
                let bid = SignedExecutionPayloadBid::<E>::from_ssz_bytes(&response_bytes)
                    .map_err(Error::InvalidSsz)?;
                Ok(Some(GloasBidResponse {
                    bid,
                    ssz_response: true,
                }))
            }
            ContentType::Json => {
                let versioned: ForkVersionedResponse<SignedExecutionPayloadBid<E>> =
                    serde_json::from_slice(&response_bytes).map_err(Error::InvalidJson)?;
                Ok(Some(GloasBidResponse {
                    bid: versioned.data,
                    ssz_response: false,
                }))
            }
        }
    }

    /// `POST /eth/v1/builder/builder_preferences/{validator_pubkey}`
    ///
    /// Submit a proposer's builder preferences to a builder ahead of the bid request (typically in
    /// the epoch before the proposal, so the builder has them before `getExecutionPayloadBid`
    /// arrives). Success is HTTP 202.
    ///
    /// The body is small and always sent as JSON, so the `Eth-Consensus-Version` header (only
    /// required for SSZ request bodies) is omitted.
    pub async fn submit_builder_preferences(
        &self,
        builder_url: &SensitiveUrl,
        validator_pubkey: &PublicKeyBytes,
        preferences: &BuilderPreferencesRequestV1,
    ) -> Result<(), Error> {
        let mut path = builder_url.expose_full().clone();

        path.path_segments_mut()
            .map_err(|()| Error::InvalidUrl(builder_url.clone()))?
            .push("eth")
            .push("v1")
            .push("builder")
            .push("builder_preferences")
            .push(validator_pubkey.as_hex_string().as_str());

        let timeout = Duration::from_millis(DEFAULT_SUBMIT_TIMEOUT_MILLIS);
        let request = self.client.post(path).timeout(timeout).json(preferences);

        let response = success_or_error(request.send().await.map_err(Error::from)?).await?;

        if response.status() == StatusCode::ACCEPTED {
            Ok(())
        } else {
            // ACCEPTED is the only valid status code response
            Err(Error::StatusCode(response.status()))
        }
    }

    /// `POST /eth/v1/builder/beacon_blocks`
    ///
    /// Submit the signed Gloas beacon block to the builder that won selection. On success (HTTP
    /// 202) the builder becomes responsible for publishing the execution payload envelope.
    ///
    /// `ssz_request` selects the request-body encoding; pass the
    /// [`GloasBidResponse::ssz_response`] recorded when the winning bid was fetched.
    pub async fn submit_signed_beacon_block<E: EthSpec>(
        &self,
        builder_url: &SensitiveUrl,
        block: &SignedBeaconBlock<E>,
        ssz_request: bool,
    ) -> Result<(), Error> {
        let mut path = builder_url.expose_full().clone();

        path.path_segments_mut()
            .map_err(|()| Error::InvalidUrl(builder_url.clone()))?
            .push("eth")
            .push("v1")
            .push("builder")
            .push("beacon_blocks");

        let mut headers = HeaderMap::new();
        headers.insert(
            CONSENSUS_VERSION_HEADER,
            HeaderValue::from_str(&block.fork_name_unchecked().to_string())
                .map_err(|e| Error::InvalidHeaders(format!("{}", e)))?,
        );

        let timeout = Duration::from_millis(DEFAULT_SUBMIT_TIMEOUT_MILLIS);
        let request = if ssz_request && !self.disable_ssz {
            headers.insert(
                CONTENT_TYPE_HEADER,
                HeaderValue::from_str(SSZ_CONTENT_TYPE_HEADER)
                    .map_err(|e| Error::InvalidHeaders(format!("{}", e)))?,
            );
            self.client
                .post(path)
                .timeout(timeout)
                .headers(headers)
                .body(block.as_ssz_bytes())
        } else {
            headers.insert(
                CONTENT_TYPE_HEADER,
                HeaderValue::from_str(JSON_CONTENT_TYPE_HEADER)
                    .map_err(|e| Error::InvalidHeaders(format!("{}", e)))?,
            );
            self.client
                .post(path)
                .timeout(timeout)
                .headers(headers)
                .json(block)
        };

        let response = success_or_error(request.send().await.map_err(Error::from)?).await?;

        if response.status() == StatusCode::ACCEPTED {
            Ok(())
        } else {
            // ACCEPTED is the only valid status code response
            Err(Error::StatusCode(response.status()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use eth2::types::beacon_response::EmptyMetadata;
    use eth2::types::{ForkName, MainnetEthSpec};
    use mockito::{Matcher, Server, ServerGuard};
    use std::str::FromStr;

    type E = MainnetEthSpec;

    fn client_for() -> BuilderHttpClient {
        BuilderHttpClient::new(None, false).unwrap()
    }

    fn builder_url(server: &ServerGuard) -> SensitiveUrl {
        SensitiveUrl::from_str(&server.url()).unwrap()
    }

    fn empty_bid_response() -> ForkVersionedResponse<SignedExecutionPayloadBid<E>> {
        ForkVersionedResponse {
            version: ForkName::Gloas,
            metadata: EmptyMetadata {},
            data: SignedExecutionPayloadBid::empty(),
        }
    }

    fn mock_bid(server: &mut ServerGuard, content_type: ContentType) {
        let body = empty_bid_response();
        let mut mock = server.mock(
            "POST",
            Matcher::Regex(r"^/eth/v1/builder/execution_payload_bid/.+$".to_string()),
        );
        mock = match content_type {
            ContentType::Json => mock
                .with_header(CONTENT_TYPE_HEADER, JSON_CONTENT_TYPE_HEADER)
                .with_header(CONSENSUS_VERSION_HEADER, "gloas")
                .with_body(serde_json::to_string(&body).unwrap()),
            ContentType::Ssz => mock
                .with_header(CONTENT_TYPE_HEADER, SSZ_CONTENT_TYPE_HEADER)
                .with_header(CONSENSUS_VERSION_HEADER, "gloas")
                .with_body(body.data.as_ssz_bytes()),
        };
        mock.with_status(200).create();
    }

    async fn request_bid(server: &ServerGuard) -> Option<GloasBidResponse<E>> {
        client_for()
            .get_execution_payload_bid::<E>(
                &builder_url(server),
                Slot::new(1),
                ExecutionBlockHash::repeat_byte(1),
                Hash256::repeat_byte(2),
                &PublicKeyBytes::empty(),
                None,
            )
            .await
            .expect("bid request should succeed")
    }

    #[tokio::test]
    async fn get_execution_payload_bid_json() {
        let mut server = Server::new_async().await;
        mock_bid(&mut server, ContentType::Json);
        let response = request_bid(&server).await.expect("should have a bid");
        assert!(!response.ssz_response);
        assert_eq!(response.bid, SignedExecutionPayloadBid::empty());
    }

    #[tokio::test]
    async fn get_execution_payload_bid_ssz() {
        let mut server = Server::new_async().await;
        mock_bid(&mut server, ContentType::Ssz);
        let response = request_bid(&server).await.expect("should have a bid");
        assert!(response.ssz_response);
        assert_eq!(response.bid, SignedExecutionPayloadBid::empty());
    }

    #[tokio::test]
    async fn submit_builder_preferences_accepted() {
        use arbitrary::Arbitrary;
        let mut server = Server::new_async().await;
        server
            .mock(
                "POST",
                Matcher::Regex(r"^/eth/v1/builder/builder_preferences/.+$".to_string()),
            )
            .with_status(202)
            .create();

        let mut u = types::test_utils::test_unstructured();
        let preferences = BuilderPreferencesRequestV1::arbitrary(&mut u).unwrap();

        client_for()
            .submit_builder_preferences(
                &builder_url(&server),
                &PublicKeyBytes::empty(),
                &preferences,
            )
            .await
            .expect("preferences should be accepted");
    }

    #[tokio::test]
    async fn get_execution_payload_bid_no_content() {
        let mut server = Server::new_async().await;
        server
            .mock(
                "POST",
                Matcher::Regex(r"^/eth/v1/builder/execution_payload_bid/.+$".to_string()),
            )
            .with_status(204)
            .create();
        assert!(request_bid(&server).await.is_none());
    }
}
