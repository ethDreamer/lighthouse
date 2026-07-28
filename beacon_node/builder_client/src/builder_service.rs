use crate::{BuilderHttpClient, DirectBid, DirectBidCache, GloasBidResponse};
use bls::PublicKeyBytes;
use eth2::types::{
    BuilderPreferencesRequestV1, EthSpec, ExecutionBlockHash, Hash256, SignedExecutionPayloadBid,
    Slot,
};
use futures::future::join_all;
use sensitive_url::SensitiveUrl;
use std::collections::HashSet;
use std::fmt::Display;
use std::future::Future;
use std::str::FromStr;
use std::sync::Arc;
use tracing::{debug, warn};

/// The per-proposal parameters used to address each `getExecutionPayloadBid` request.
///
/// Validation of returned bids is performed entirely by the caller's `validate` callback (which has
/// the beacon-chain state), so this only carries what's needed to build the request.
#[derive(Clone)]
pub struct DirectBidRequest {
    pub slot: Slot,
    pub parent_hash: ExecutionBlockHash,
    pub parent_root: Hash256,
    pub proposer_pubkey: PublicKeyBytes,
}

/// Orchestrates direct builder bid requests.
///
/// Fans `getExecutionPayloadBid` out to the builders a proposer configured and records the
/// resulting bids — highest by proposer value, with provenance — in the shared [`DirectBidCache`]
/// for the block producer to select from.
pub struct BuilderService<E: EthSpec> {
    client: Arc<BuilderHttpClient>,
    cache: Arc<DirectBidCache<E>>,
}

impl<E: EthSpec> BuilderService<E> {
    pub fn new(client: Arc<BuilderHttpClient>, cache: Arc<DirectBidCache<E>>) -> Self {
        Self { client, cache }
    }

    /// The shared cache this service populates.
    pub fn cache(&self) -> &Arc<DirectBidCache<E>> {
        &self.cache
    }

    /// Request bids from every configured builder concurrently, validate them, and record the valid
    /// ones in the cache.
    ///
    /// Each preference's builder URL is decoded from its request-auth `data` field. Empty-URL
    /// entries (the gossip default) and preferences with a malformed or non-http(s) URL are
    /// skipped, and duplicate URLs are requested only once.
    ///
    /// Each builder runs in its own pipeline — request, then the producer-supplied `validate`
    /// callback, which performs *all* bid validation against the block producer's advanced beacon
    /// state (consensus consistency, builder eligibility, collateral, and the BLS signature). These
    /// pipelines run **concurrently across builders**, so a slow builder or an expensive validation
    /// for one bid does not hold up the others. A failure, timeout, empty (204) response, or
    /// validation error for one builder is isolated: it is logged and that bid is skipped, never
    /// entering the cache.
    ///
    /// Returns the number of bids that passed validation and were observed into the cache. The
    /// block producer reads the winning bid later via [`DirectBidCache::get_highest_bid`].
    pub async fn request_and_cache_bids<F, Fut, Err>(
        &self,
        ctx: &DirectBidRequest,
        preferences: &[BuilderPreferencesRequestV1],
        validate: F,
    ) -> usize
    where
        F: Fn(Arc<SignedExecutionPayloadBid<E>>) -> Fut,
        Fut: Future<Output = Result<(), Err>>,
        Err: Display,
    {
        // Resolve each preference to a `(url, max_execution_payment, auth)` target, filtering out
        // invalid entries and de-duplicating by normalized URL.
        let mut seen = HashSet::new();
        let mut targets = Vec::new();
        for preference in preferences {
            let auth = preference.auth();
            let url_bytes = &auth.message.data[..];
            if url_bytes.is_empty() {
                // Empty-URL entry: applies to the gossip default, not a direct builder.
                continue;
            }
            let Some(url) = std::str::from_utf8(url_bytes)
                .ok()
                .and_then(|url| SensitiveUrl::from_str(url).ok())
            else {
                warn!("Skipping builder preference with a malformed URL");
                continue;
            };
            if !matches!(url.expose_full().scheme(), "http" | "https") {
                warn!(url = ?url, "Skipping builder preference with an unsupported URL scheme");
                continue;
            }
            if !seen.insert(url.expose_full().as_str().to_string()) {
                // Already requesting this builder.
                continue;
            }
            targets.push((url, preference.preferences().max_execution_payment, auth));
        }

        // Run one pipeline per builder — request, then the producer's `validate` callback — and let
        // them run concurrently across builders. Each request carries its own timeout, so a slow
        // builder cannot delay the others.
        let client = &self.client;
        let validate = &validate;
        let pipelines = targets
            .iter()
            .map(|(url, max_execution_payment, auth)| async move {
                let response = client
                    .get_execution_payload_bid::<E>(
                        url,
                        ctx.slot,
                        ctx.parent_hash,
                        ctx.parent_root,
                        &ctx.proposer_pubkey,
                        auth,
                    )
                    .await;

                match response {
                    Ok(Some(GloasBidResponse { bid, ssz_response })) => {
                        let bid = Arc::new(bid);
                        if let Err(error) = validate(bid.clone()).await {
                            warn!(url = ?url, %error, "Builder bid failed validation");
                            return None;
                        }
                        Some(DirectBid::new(
                            bid,
                            url.clone(),
                            ssz_response,
                            *max_execution_payment,
                        ))
                    }
                    Ok(None) => {
                        debug!(url = ?url, "Builder returned no bid");
                        None
                    }
                    Err(error) => {
                        warn!(url = ?url, error = %error, "Builder bid request failed");
                        None
                    }
                }
            });

        let validated_bids = join_all(pipelines).await;

        let mut received = 0;
        for bid in validated_bids.into_iter().flatten() {
            self.cache.observe_bid(bid);
            received += 1;
        }
        received
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bls::Signature;
    use eth2::types::beacon_response::EmptyMetadata;
    use eth2::types::{
        BuilderPreferencesV1, ExecutionPayloadBid, ForkName, ForkVersionedResponse, MainnetEthSpec,
        RequestAuthData, RequestAuthV1, SignedExecutionPayloadBid, SignedRequestAuthV1,
    };
    use eth2::{CONSENSUS_VERSION_HEADER, CONTENT_TYPE_HEADER, JSON_CONTENT_TYPE_HEADER};
    use mockito::{Matcher, Mock, Server, ServerGuard};

    type E = MainnetEthSpec;

    const BID_PATH: &str = r"^/eth/v1/builder/execution_payload_bid/.+$";

    fn preference(url: &str, max_execution_payment: u64) -> BuilderPreferencesRequestV1 {
        BuilderPreferencesRequestV1::new(
            BuilderPreferencesV1 {
                max_execution_payment,
            },
            SignedRequestAuthV1 {
                message: RequestAuthV1 {
                    data: RequestAuthData::new(url.as_bytes().to_vec()).unwrap(),
                    slot: Slot::new(1),
                },
                signature: Signature::empty(),
            },
        )
    }

    fn bid_body(value: u64) -> String {
        let body = ForkVersionedResponse {
            version: ForkName::Gloas,
            metadata: EmptyMetadata {},
            data: SignedExecutionPayloadBid::<E> {
                message: ExecutionPayloadBid {
                    slot: Slot::new(1),
                    parent_block_hash: ExecutionBlockHash::zero(),
                    parent_block_root: Hash256::ZERO,
                    value,
                    ..ExecutionPayloadBid::default()
                },
                signature: Signature::empty(),
            },
        };
        serde_json::to_string(&body).unwrap()
    }

    fn mock_bid(server: &mut ServerGuard, value: u64) -> Mock {
        server
            .mock("POST", Matcher::Regex(BID_PATH.to_string()))
            .with_header(CONTENT_TYPE_HEADER, JSON_CONTENT_TYPE_HEADER)
            .with_header(CONSENSUS_VERSION_HEADER, "gloas")
            .with_body(bid_body(value))
            .with_status(200)
            .create()
    }

    fn context() -> DirectBidRequest {
        DirectBidRequest {
            slot: Slot::new(1),
            parent_hash: ExecutionBlockHash::zero(),
            parent_root: Hash256::ZERO,
            proposer_pubkey: PublicKeyBytes::empty(),
        }
    }

    fn service() -> BuilderService<E> {
        BuilderService::new(
            Arc::new(BuilderHttpClient::new(None, false).unwrap()),
            Arc::new(DirectBidCache::new()),
        )
    }

    #[tokio::test]
    async fn fans_out_and_caches_highest_bid() {
        let mut server_a = Server::new_async().await;
        let mut server_b = Server::new_async().await;
        mock_bid(&mut server_a, 100);
        mock_bid(&mut server_b, 200);

        let service = service();
        let prefs = vec![
            preference(&server_a.url(), 1000),
            preference(&server_b.url(), 1000),
        ];

        let received = service
            .request_and_cache_bids(&context(), &prefs, |_bid| async { Ok::<(), String>(()) })
            .await;
        assert_eq!(received, 2);

        let highest = service
            .cache()
            .get_highest_bid(Slot::new(1), ExecutionBlockHash::zero(), Hash256::ZERO)
            .unwrap();
        assert_eq!(highest.proposer_value, 200);
    }

    #[tokio::test]
    async fn skips_empty_url_preference() {
        let service = service();
        let prefs = vec![preference("", 1000)];

        let received = service
            .request_and_cache_bids(&context(), &prefs, |_bid| async { Ok::<(), String>(()) })
            .await;
        assert_eq!(received, 0);
        assert!(
            service
                .cache()
                .get_highest_bid(Slot::new(1), ExecutionBlockHash::zero(), Hash256::ZERO)
                .is_none()
        );
    }

    #[tokio::test]
    async fn deduplicates_repeated_urls() {
        let mut server = Server::new_async().await;
        let mock = mock_bid(&mut server, 100).expect(1);

        let service = service();
        let prefs = vec![
            preference(&server.url(), 1000),
            preference(&server.url(), 1000),
        ];

        let received = service
            .request_and_cache_bids(&context(), &prefs, |_bid| async { Ok::<(), String>(()) })
            .await;
        assert_eq!(received, 1);
        mock.assert();
    }

    #[tokio::test]
    async fn rejects_bid_failing_producer_validation() {
        let mut server = Server::new_async().await;
        mock_bid(&mut server, 100);

        let service = service();
        let prefs = vec![preference(&server.url(), 1000)];
        // The producer callback rejects the bid (e.g. a failed signature or ineligible builder).
        let received = service
            .request_and_cache_bids(&context(), &prefs, |_bid| async {
                Err::<(), String>("rejected by producer".to_string())
            })
            .await;
        assert_eq!(received, 0);
        assert!(
            service
                .cache()
                .get_highest_bid(Slot::new(1), ExecutionBlockHash::zero(), Hash256::ZERO)
                .is_none()
        );
    }
}
