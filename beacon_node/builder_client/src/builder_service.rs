use crate::{
    BuilderHttpClient, DirectBid, DirectBidCache, Error as BuilderClientError, GloasBidResponse,
};
use bls::PublicKeyBytes;
use eth2::types::{
    BuilderEntryV1, BuilderPreferenceEntryV1, BuilderPreferencesRequestV1, BuilderPreferencesV1,
    EthSpec, ExecutionBlockHash, Hash256, SignedExecutionPayloadBid, Slot,
};
use futures::future::join_all;
use std::fmt::Display;
use std::future::Future;
use std::sync::Arc;
use tracing::{debug, warn};

/// The per-proposal parameters used to address each `getExecutionPayloadBid` request.
///
/// Validation of returned bids is performed entirely by the caller's `validate` callback (which has
/// the beacon-chain state), so this only carries what's needed to build the request.
#[derive(Clone)]
pub struct BidRequestContext {
    pub slot: Slot,
    pub parent_hash: ExecutionBlockHash,
    pub parent_root: Hash256,
    pub proposer_pubkey: PublicKeyBytes,
}

/// Orchestrates direct builder bid requests.
///
/// Fans `getExecutionPayloadBid` out to the builders a proposer configured and records the
/// resulting bids — highest by boosted value, with provenance — in the shared [`DirectBidCache`]
/// for the block producer to select from.
pub struct BuilderService<E: EthSpec> {
    client: Arc<BuilderHttpClient>,
    cache: Arc<DirectBidCache<E>>,
}

/// A single failed builder-preference submission, identified by its position in the submitted list.
pub struct SubmissionFailure {
    /// Index of the failing entry in the submitted list.
    pub index: usize,
    /// Why the submission failed.
    pub error: BuilderClientError,
}

impl<E: EthSpec> BuilderService<E> {
    pub fn new(client: Arc<BuilderHttpClient>, cache: Arc<DirectBidCache<E>>) -> Self {
        Self { client, cache }
    }

    /// The shared cache this service populates.
    pub fn cache(&self) -> &Arc<DirectBidCache<E>> {
        &self.cache
    }

    /// The Builder API HTTP client. It is stateless w.r.t. the target builder — each request takes
    /// the builder URL as a parameter — so a single client fans out to any builder.
    pub fn client(&self) -> &Arc<BuilderHttpClient> {
        &self.client
    }

    /// Request bids from every builder in `entries` concurrently, validate them, and record the
    /// valid ones in the cache.
    ///
    /// Every entry is a bid request to its `url`, which beacon-APIs #630 requires (a zero-length url
    /// is invalid); an entry whose `url` is empty, malformed, or not http(s) can't be requested and
    /// is skipped. One request is made **per entry** — several entries MAY share a `url` with
    /// different `auth`, so requests are not de-duplicated by URL (#630 forbids two entries sharing
    /// both a `url` and their `auth`'s `data`).
    ///
    /// Each builder runs in its own pipeline — request, then a `min_bid` check that drops any bid
    /// whose clamped (trusted) value is below the entry's `min_bid`, then the producer-supplied
    /// `validate` callback, which performs *all* bid validation against the block producer's
    /// advanced beacon state (consensus consistency, builder eligibility, collateral, and the BLS
    /// signature). The entry's expected `builder_pubkey` (`None` when the entry omits one) is passed
    /// to `validate`
    /// so it can enforce that the bid is signed by the expected builder — the state and signing
    /// domain that check needs live on the producer side, not here. These pipelines run
    /// **concurrently across builders**, so a slow builder or an expensive validation for one bid
    /// does not hold up the others. A failure, timeout, empty (204) response, or validation error
    /// for one builder is isolated: it is logged and that bid is skipped, never entering the cache.
    ///
    /// Returns the number of bids that passed validation and were observed into the cache. The
    /// block producer reads the winning bid later via [`DirectBidCache::get_highest_bid`].
    pub async fn request_and_cache_bids<F, Fut, Err>(
        &self,
        ctx: &BidRequestContext,
        entries: &[BuilderEntryV1],
        validate: F,
    ) -> usize
    where
        F: Fn(Arc<SignedExecutionPayloadBid<E>>, Option<PublicKeyBytes>) -> Fut,
        Fut: Future<Output = Result<(), Err>>,
        Err: Display,
    {
        // Resolve each entry to a `(resolved_url, entry)` target. Every entry must carry a valid url
        // (#630); one that's empty, malformed, or non-http(s) can't be requested and is skipped. One
        // request is made per entry (no URL de-duplication).
        let mut targets = Vec::new();
        for entry in entries {
            let url = match entry.url.to_sensitive_url() {
                Ok(url) => url,
                Err(e) => {
                    warn!(error = ?e, "Skipping builder entry with a malformed URL");
                    continue;
                }
            };
            if !matches!(url.expose_full().scheme(), "http" | "https") {
                warn!(url = ?url, "Skipping builder entry with an unsupported URL scheme");
                continue;
            }
            targets.push((url, entry));
        }

        // Run one pipeline per builder — request, then the producer's `validate` callback — and let
        // them run concurrently across builders. Each request carries its own timeout, so a slow
        // builder cannot delay the others.
        let client = &self.client;
        let validate = &validate;
        let pipelines = targets.iter().map(|(url, entry)| async move {
            let response = client
                .get_execution_payload_bid::<E>(
                    url,
                    ctx.slot,
                    ctx.parent_hash,
                    ctx.parent_root,
                    &ctx.proposer_pubkey,
                    &entry.auth,
                )
                .await;

            match response {
                Ok(Some(GloasBidResponse { bid, ssz_response })) => {
                    let direct_bid = DirectBid::new(
                        Arc::new(bid),
                        url.clone(),
                        ssz_response,
                        entry.max_execution_payment,
                        entry.builder_boost_factor,
                    );

                    // Reject bids whose clamped (trusted) value is below the entry's `min_bid`. The
                    // clamp keeps a builder from clearing the floor with untrusted `execution_payment`.
                    if direct_bid.clamped_value() < entry.min_bid {
                        debug!(
                            url = ?url,
                            clamped_value = direct_bid.clamped_value(),
                            min_bid = entry.min_bid,
                            "Skipping builder bid below min_bid"
                        );
                        return None;
                    }

                    if let Err(error) =
                        validate(direct_bid.signed_bid.clone(), entry.builder_pubkey()).await
                    {
                        warn!(url = ?url, %error, "Builder bid failed validation");
                        return None;
                    }
                    Some(direct_bid)
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

    /// Submit a proposer's builder preferences to each entry's builder, concurrently and
    /// best-effort.
    ///
    /// One submission is made per entry — entries are **not** de-duplicated by URL, since
    /// beacon-APIs #630 allows several entries to share a `url`. Each submission is isolated: a
    /// malformed URL or a failed request is recorded against that entry's index and never aborts the
    /// others. The submissions run **concurrently**, so a slow builder cannot delay the rest.
    ///
    /// Returns `Ok(())` when every entry was submitted, or the per-entry [`SubmissionFailure`]s by
    /// index.
    pub async fn submit_preferences(
        &self,
        proposer_pubkey: &PublicKeyBytes,
        entries: Vec<BuilderPreferenceEntryV1>,
    ) -> Result<(), Vec<SubmissionFailure>> {
        let client = &self.client;
        let submissions = entries
            .into_iter()
            .enumerate()
            .map(|(index, entry)| async move {
                let url = entry
                    .url
                    .to_sensitive_url()
                    .map_err(|e| SubmissionFailure {
                        index,
                        error: e.into(),
                    })?;
                let request = BuilderPreferencesRequestV1::new(
                    BuilderPreferencesV1 {
                        max_execution_payment: entry.max_execution_payment,
                    },
                    entry.auth,
                );
                client
                    .submit_builder_preferences(&url, proposer_pubkey, &request)
                    .await
                    .map_err(|error| SubmissionFailure { index, error })
            });

        let failures: Vec<SubmissionFailure> = join_all(submissions)
            .await
            .into_iter()
            .filter_map(Result::err)
            .collect();

        if failures.is_empty() {
            Ok(())
        } else {
            Err(failures)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bls::Signature;
    use eth2::types::beacon_response::EmptyMetadata;
    use eth2::types::{
        ExecutionPayloadBid, ForkName, ForkVersionedResponse, MainnetEthSpec, RequestAuthData,
        RequestAuthV1, SignedExecutionPayloadBid, SignedRequestAuthV1,
    };
    use eth2::{CONSENSUS_VERSION_HEADER, CONTENT_TYPE_HEADER, JSON_CONTENT_TYPE_HEADER};
    use mockito::{Matcher, Mock, Server, ServerGuard};

    type E = MainnetEthSpec;

    const BID_PATH: &str = r"^/eth/v1/builder/execution_payload_bid/.+$";

    fn entry(url: &str, max_execution_payment: u64) -> BuilderEntryV1 {
        BuilderEntryV1 {
            url: url.parse().unwrap(),
            auth: SignedRequestAuthV1 {
                message: RequestAuthV1 {
                    data: RequestAuthData::default(),
                    slot: Slot::new(1),
                },
                signature: Signature::empty(),
            },
            builder_pubkey: PublicKeyBytes::empty(),
            max_execution_payment,
            min_bid: 0,
            builder_boost_factor: 100,
        }
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

    fn context() -> BidRequestContext {
        BidRequestContext {
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
        let entries = vec![entry(&server_a.url(), 1000), entry(&server_b.url(), 1000)];

        let received = service
            .request_and_cache_bids(&context(), &entries, |_bid, _expected| async {
                Ok::<(), String>(())
            })
            .await;
        assert_eq!(received, 2);

        let highest = service
            .cache()
            .get_highest_bid(Slot::new(1), ExecutionBlockHash::zero(), Hash256::ZERO)
            .unwrap();
        assert_eq!(highest.proposer_value(), 200);
    }

    #[tokio::test]
    async fn skips_invalid_url_entry() {
        let service = service();
        // #630 requires a url; an empty one is invalid and can't be requested, so it is skipped.
        let entries = vec![entry("", 1000)];

        let received = service
            .request_and_cache_bids(&context(), &entries, |_bid, _expected| async {
                Ok::<(), String>(())
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

    #[tokio::test]
    async fn requests_each_entry_even_when_url_is_shared() {
        let mut server = Server::new_async().await;
        // Two entries share a URL but carry different `auth`, so both are requested (one per entry).
        let mock = mock_bid(&mut server, 100).expect(2);

        let service = service();
        let entry_a = entry(&server.url(), 1000);
        let mut entry_b = entry(&server.url(), 1000);
        entry_b.auth.message.slot = Slot::new(2);
        let entries = vec![entry_a, entry_b];

        let received = service
            .request_and_cache_bids(&context(), &entries, |_bid, _expected| async {
                Ok::<(), String>(())
            })
            .await;
        assert_eq!(received, 2);
        mock.assert();
    }

    #[tokio::test]
    async fn skips_bid_below_min_bid() {
        let mut server = Server::new_async().await;
        // Bid value 100, no execution payment, so the clamped value is 100.
        mock_bid(&mut server, 100);

        let service = service();
        let mut entry = entry(&server.url(), 1000);
        entry.min_bid = 500;
        let entries = vec![entry];

        let received = service
            .request_and_cache_bids(&context(), &entries, |_bid, _expected| async {
                Ok::<(), String>(())
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

    #[tokio::test]
    async fn rejects_bid_failing_producer_validation() {
        let mut server = Server::new_async().await;
        mock_bid(&mut server, 100);

        let service = service();
        let entries = vec![entry(&server.url(), 1000)];
        // The producer callback rejects the bid (e.g. a failed signature or ineligible builder).
        let received = service
            .request_and_cache_bids(&context(), &entries, |_bid, _expected| async {
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
