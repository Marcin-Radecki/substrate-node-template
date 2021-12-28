use std::sync::atomic::{AtomicU64, Ordering};
use frame_benchmarking::frame_support::traits::Len;
use sp_core::Decode;

pub const INHERENT_IDENTIFIER: sp_inherents::InherentIdentifier = *b"kara0ke!";

static mut COUNTER: AtomicU64 = AtomicU64::new(0);

pub struct KaraokeInherentDataProvider;

#[async_trait::async_trait]
impl sp_inherents::InherentDataProvider for KaraokeInherentDataProvider {
    fn provide_inherent_data(&self, inherent_data: &mut sp_inherents::InherentData) -> Result<(), sp_inherents::Error> {
        let fade_to_black_lyrics  =
            ["Life, it seems, will fade away",
        "Drifting further, every day",
        "Getting lost within myself",
        "Nothing matters, no one else",
        "I have lost the will to live",
        "Simply nothing more to give",
        "There is nothing more for me",
        "Need the end to set me free"];

        let new_verse_index = unsafe {
            COUNTER.fetch_add(1, Ordering::SeqCst) as usize % fade_to_black_lyrics.len()
        };
        let verse = fade_to_black_lyrics[new_verse_index];
        let msg = verse.as_bytes().to_vec();
        log::info!("🎤 Proposing song lyric to a block.");
        inherent_data
            .put_data(INHERENT_IDENTIFIER, &msg)
    }

    async fn try_handle_error(&self, identifier: &sp_inherents::InherentIdentifier, mut error: &[u8])
                              -> Option<Result<(), sp_inherents::Error>> {
        if *identifier != INHERENT_IDENTIFIER {
            return None
        }

        Some(Err(
            sp_inherents::Error::Application(Box::from(String::decode(&mut error).ok()?))
        ))
    }
}
