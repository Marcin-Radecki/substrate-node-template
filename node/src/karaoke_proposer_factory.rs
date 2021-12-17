use std::pin::Pin;
use std::time;
use sc_basic_authorship::{Proposer, ProposerFactory};
use sc_client_api::backend;
use sc_transaction_pool_api::TransactionPool;
use sp_api::{ApiExt, ProvideRuntimeApi};
use sp_blockchain::HeaderBackend;
use sp_consensus::{ProofRecording, Proposal};
use sp_runtime::{
    traits::{Block as BlockT},
};
use futures::{
    future,
    future::{Future},
};
use sc_block_builder::{BlockBuilderProvider, BlockBuilderApi};
use sp_inherents::{InherentData, InherentIdentifier};
use sp_runtime::traits::{DigestFor, Header};
use log::info;

pub const INHERENT_IDENTIFIER: InherentIdentifier = *b"kara0ke!";

pub struct KaraokeProposerFactory<A, B, C, PR>(pub ProposerFactory<A, B, C, PR>);

impl<A, B, Block, C, PR> sp_consensus::Environment<Block> for KaraokeProposerFactory<A, B, C, PR>
    where
        A: TransactionPool<Block = Block> + 'static,
        B: backend::Backend<Block> + Send + Sync + 'static,
        Block: BlockT,
        C: BlockBuilderProvider<B, Block, C>
        + HeaderBackend<Block>
        + ProvideRuntimeApi<Block>
        + Send
        + Sync
        + 'static,
        C::Api:
        ApiExt<Block, StateBackend = backend::StateBackendFor<B, Block>> + BlockBuilderApi<Block>,
        PR: ProofRecording,
{
    type Proposer = KaraokeProposer<B, Block, C, A, PR>;
    type CreateProposer = future::Ready<Result<Self::Proposer, Self::Error>>;
    type Error = sp_blockchain::Error;

    fn init(&mut self, parent_header: &<Block as BlockT>::Header) -> Self::CreateProposer {
        info!("🐈 Creating KaraokeProposer on top of parent {}", parent_header.hash());
        let proposer = self.0.init(parent_header).into_inner();
        future::ready(Ok(KaraokeProposer(proposer.unwrap())))
    }
}

pub struct KaraokeProposer<B, Block: BlockT, C, A: TransactionPool, PR>(Proposer<B, Block, C, A, PR>);

impl<A, B, Block, C, PR> sp_consensus::Proposer<Block> for KaraokeProposer<B, Block, C, A, PR>
    where
        A: TransactionPool<Block = Block> + 'static,
        B: backend::Backend<Block> + Send + Sync + 'static,
        Block: BlockT,
        C: BlockBuilderProvider<B, Block, C>
        + HeaderBackend<Block>
        + ProvideRuntimeApi<Block>
        + Send
        + Sync
        + 'static,
        C::Api:
        ApiExt<Block, StateBackend = backend::StateBackendFor<B, Block>> + BlockBuilderApi<Block>,
        PR: ProofRecording,
{
    type Error = sp_blockchain::Error;
    type Transaction = backend::TransactionFor<B, Block>;
    type Proposal = Pin<
        Box<
            dyn Future<Output=Result<Proposal<Block, Self::Transaction, PR::Proof>, Self::Error>>
            + Send,
        >,
    >;
    type ProofRecording = PR;
    type Proof = PR::Proof;

    fn propose(
        self,
        inherent_data: InherentData,
        inherent_digests: DigestFor<Block>,
        max_duration: time::Duration,
        block_size_limit: Option<usize>,
    ) -> Self::Proposal {
        let fade_to_black_lyrics = "Life, it seems, will fade away".as_bytes().to_vec();
        let mut inherent_data = inherent_data;
        inherent_data
            .put_data(INHERENT_IDENTIFIER, &fade_to_black_lyrics)
            .expect("Failed to put a song lyric to a block!");

        info!("🐩 Proposing song lyric to a block.");
        self.0.propose(inherent_data, inherent_digests, max_duration, block_size_limit)
    }
}
