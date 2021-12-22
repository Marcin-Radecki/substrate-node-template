//! `CasinoGossip`: a broadcasting engine for messages when blocks are imported by Substrate
//!
//! Inspired by [GRANDPA finality gadget code](https://github.com/paritytech/substrate/blob/master/client/finality-grandpa/src/lib.rs)

use std::collections::HashMap;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use futures::{FutureExt, StreamExt};

use sp_runtime::traits::Header;

// this seems to have needs specific syntax name/<number>, as described in https://github.com/paritytech/substrate/blob/master/client/network/README.md
pub const CASINO_PROTOCOL_NAME: &'static str = "casino/1";

pub const CASINO_TOPIC: &'static str = "casino-topic";

/// Block importer specific for gossiping timestamp
pub struct CasinoBlockImporter<Block: sp_runtime::traits::Block, Client, InnerBlockImport> {
    block_import: InnerBlockImport,
    gossip_block_number_threshold: u64,
    gossip_next_block_number: u64,
    first_timestamp: Option<sp_timestamp::Timestamp>,
    timestamp_sender: sc_utils::mpsc::TracingUnboundedSender<sp_timestamp::Timestamp>,
    _phantom: PhantomData<(Block, Client)>,
}

impl<Block: sp_runtime::traits::Block, Client, InnerBlockImport> CasinoBlockImporter<Block, Client, InnerBlockImport> {
    pub fn new(block_import: InnerBlockImport,
               gossip_block_number_threshold: u64,
               gossip_next_block_number: u64,
               timestamp_sender: sc_utils::mpsc::TracingUnboundedSender<sp_timestamp::Timestamp>) -> Self {
        Self {
            block_import,
            gossip_block_number_threshold,
            gossip_next_block_number,
            first_timestamp: None,
            timestamp_sender,
            _phantom: Default::default(),
        }
    }
}

impl<Block: sp_runtime::traits::Block, Client, InnerBlockImport: Clone> Clone for CasinoBlockImporter<Block, Client, InnerBlockImport>
{
    fn clone(&self) -> Self {
        Self {
            block_import: self.block_import.clone(),
            gossip_block_number_threshold: self.gossip_block_number_threshold,
            gossip_next_block_number: self.gossip_next_block_number,
            first_timestamp: self.first_timestamp.clone(),
            timestamp_sender: self.timestamp_sender.clone(),
            _phantom: Default::default(),
        }
    }
}

#[async_trait::async_trait]
impl<Block, Client, InnerBlockImport> sc_consensus::BlockImport<Block> for CasinoBlockImporter<Block, Client, InnerBlockImport>
    where
        Block: sp_runtime::traits::Block,
        Client: sp_api::ProvideRuntimeApi<Block> + Send,
        InnerBlockImport: sc_consensus::BlockImport<Block, Error=sp_consensus::Error, Transaction=sp_api::TransactionFor<Client, Block>> + Send + Sync,
        sp_api::TransactionFor<Client, Block>: 'static,
{
    type Error = sp_consensus::Error;
    type Transaction = sp_api::TransactionFor<Client, Block>;

    async fn check_block(&mut self, block: sc_consensus::BlockCheckParams<Block>) -> Result<sc_consensus::ImportResult, Self::Error> {
        self.block_import.check_block(block).await
    }

    async fn import_block(
        &mut self,
        block: sc_consensus::BlockImportParams<Block, Self::Transaction>,
        new_cache: HashMap<sp_blockchain::well_known_cache_keys::Id, Vec<u8>>,
    ) -> Result<sc_consensus::ImportResult, Self::Error> {
        let number = *block.header.number();
        let timestamp = sp_timestamp::InherentDataProvider::from_system_time().timestamp();
        log::debug!("🎰 Importing block {}, block hash {}, timestamp: {}", number, block.header.hash(), u64_timestamp_to_string(*timestamp));

        let number_parsed = sp_runtime::traits::NumberFor::<Block>::from(self.gossip_block_number_threshold as u32);
        if number.eq(&number_parsed) {
            self.first_timestamp = Some(timestamp);
        } else if self.first_timestamp.is_some() {
            let current_timestamp = *self.first_timestamp.unwrap();
            let diff = *timestamp - current_timestamp;
            let predicted_timestamp = current_timestamp + self.gossip_next_block_number * diff;
            log::info!("🎰 Sending timestamp {} from block importer", u64_timestamp_to_string(predicted_timestamp));
            self.timestamp_sender.unbounded_send(sp_timestamp::Timestamp::new(predicted_timestamp)).expect("Failed to send a timestamp!");
            self.first_timestamp = None;
        }
        self.block_import.import_block(block, new_cache).await.map_err(Into::into)
    }
}

fn casino_topic<B: sp_runtime::traits::Block>() -> B::Hash {
    <<B::Header as sp_runtime::traits::Header>::Hashing as sp_runtime::traits::Hash>::hash(CASINO_TOPIC.as_bytes())
}

/// Validator which just accepts all incoming messages
#[derive(Clone)]
pub struct ForwardAllValidator;

impl<Block: sp_runtime::traits::Block> sc_network_gossip::Validator<Block> for ForwardAllValidator {
    fn validate(
        &self,
        _: &mut dyn sc_network_gossip::ValidatorContext<Block>,
        _: &sc_network::PeerId,
        _: &[u8],
    ) -> sc_network_gossip::ValidationResult<Block::Hash> {
        sc_network_gossip::ValidationResult::ProcessAndDiscard(casino_topic::<Block>())
    }

    // This is not ideal that in order to get rid of sent message in gossip engine we need to
    // explicitly expire message immediately; the ProcessAndDiscard in validate above
    // should do the job. so it's either my substrate misunderstand or substrate bug
    fn message_expired<'a>(&'a self) -> Box<dyn FnMut(Block::Hash, &[u8]) -> bool + 'a> {
        Box::new(move |_topic, _data| true)
    }
}

pub struct CasinoGossipEngine<Block: sp_runtime::traits::Block, Network: sc_network_gossip::Network<Block>> {
    gossip_engine: Arc<Mutex<sc_network_gossip::GossipEngine<Block>>>,
    timestamp_receiver: sc_utils::mpsc::TracingUnboundedReceiver<sp_timestamp::Timestamp>,
    _phantom: PhantomData<Network>,
}

impl<Block, Network> CasinoGossipEngine<Block, Network>
    where
        Block: sp_runtime::traits::Block,
        Network: sc_network_gossip::Network<Block> + Clone + Send + 'static,
{
    pub fn new(network_service: Network,
               timestamp_receiver: sc_utils::mpsc::TracingUnboundedReceiver<sp_timestamp::Timestamp>) -> Self {
        let validator = Arc::new(ForwardAllValidator {});
        let gossip_engine = Arc::new(Mutex::new(sc_network_gossip::GossipEngine::new(
            network_service.clone(),
            CASINO_PROTOCOL_NAME,
            validator.clone(),
            None,
        )));

        Self {
            gossip_engine,
            timestamp_receiver,
            _phantom: Default::default(),
        }
    }
}

pub struct CasinoError(String);

fn u64_timestamp_to_string(timestamp: u64) -> String {
    let seconds = timestamp / 1000;
    let nanoseconds: u32 = (seconds % 1000) as u32 * 1_000_000;
    chrono::NaiveDateTime::from_timestamp(seconds as i64, nanoseconds).to_string()
}

impl<B: sp_runtime::traits::Block, N: sc_network_gossip::Network<B>> Unpin for CasinoGossipEngine<B, N> {}

impl<Block, Network> std::future::Future for CasinoGossipEngine<Block, Network>
    where
        Block: sp_runtime::traits::Block,
        Network: sc_network_gossip::Network<Block> + Clone + Send + 'static,
{
    type Output = Result<(), CasinoError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        log::debug!("🎰 Consuming gossip engine future to get messages propagated.");
        match self.gossip_engine.lock().unwrap().poll_unpin(cx) {
            Poll::Ready(()) => {
                return Poll::Ready(Err(CasinoError(String::from("Gossip engine future finished."))));
            }
            Poll::Pending => {}
        }

        loop {
            log::debug!("🎰 Polling CasinoGossipEngine future.");
            match self.timestamp_receiver.poll_next_unpin(cx) {
                Poll::Ready(Some(timestamp)) => {
                    log::info!("🎰 Received timestamp {} from block importer. Sending to gossip engine.", u64_timestamp_to_string(*timestamp));
                    self.gossip_engine.lock().unwrap().gossip_message(
                        casino_topic::<Block>(),
                        Vec::from(timestamp.to_be_bytes()),
                        false);
                }
                Poll::Ready(None) => {
                    return Poll::Ready(Err(CasinoError(String::from("Gossip validator report stream closed."))));
                }
                Poll::Pending => break,
            }
        }

        // messages_for(topic) should be really called subscribe_messages_for(topic); this is because
        // it returns a Receiver for all _current_ or _past_ messages in gossip engine
        // it means we cannot call this function before this future poll() is called
        let mut messages_for_topic = self.gossip_engine.lock().unwrap().messages_for(casino_topic::<Block>());
        loop {
            log::debug!("🎰 Waiting for gossip from CasinoGossipEngine future.");
            match messages_for_topic.poll_next_unpin(cx) {
                Poll::Ready(Some(notification)) => {
                    let a: [u8; 8] = notification.message.clone().try_into().unwrap();
                    let timestamp = u64::from_be_bytes(a);
                    log::info!("🎰 Received timestamp via gossip engine {}", u64_timestamp_to_string(timestamp));
                }
                Poll::Ready(None) => {
                    return Poll::Ready(Err(CasinoError(String::from("Gossip engine stream closed."))));
                }
                Poll::Pending => break,
            }
        }

        Poll::Pending
    }
}

pub fn run_casino_gossiper<Block, Network>(rx: sc_utils::mpsc::TracingUnboundedReceiver<sp_timestamp::Timestamp>,
                                           network_service: Network)
                                           -> sp_blockchain::Result<impl std::future::Future<Output=()> + Send>
    where
        Block: sp_runtime::traits::Block,
        Network: sc_network_gossip::Network<Block> + Clone + Send + 'static,
{
    let engine = CasinoGossipEngine::new(network_service, rx);
    let engine = engine.map(|res| match res {
        Ok(()) => log::error!("Casino gossiper future has concluded naturally, this should be unreachable."),
        Err(e) => log::error!("Casino gossiper error: {:?}", e.0),
    });
    Ok(engine)
}
