use assert_matches::assert_matches;
use matrix_sdk_base::{
    RoomState,
    deserialized_responses::TimelineEvent,
    linked_chunk::{ChunkIdentifier, LinkedChunkId, Position, Update},
    store::SerializableEventContent,
};
use matrix_sdk_test::{JoinedRoomBuilder, async_test, event_factory::EventFactory};
use ruma::{
    EventId, MilliSecondsSinceUnixEpoch, OwnedTransactionId, event_id, owned_room_id,
    events::{AnyMessageLikeEventContent, room::message::RoomMessageEventContent},
    user_id,
};
use stream_assert::assert_next_matches;

use super::{LatestEvent, LatestEventValue};
use crate::{
    Client,
    client::WeakClient,
    event_cache::RoomEventCache,
    room::WeakRoom,
    send_queue::{LocalEcho, LocalEchoContent, RoomSendQueueUpdate, SendHandle},
    test_utils::mocks::MatrixMockServer,
};

struct Fixture {
    server: MatrixMockServer,
    client: Client,
    cache: RoomEventCache,
    latest: LatestEvent,
}

fn factory() -> EventFactory {
    EventFactory::new().room(&owned_room_id!("!preview")).sender(user_id!("@alice:server.org"))
}

impl Fixture {
    async fn new(events: Vec<TimelineEvent>) -> Self {
        let server = MatrixMockServer::new().await;
        let client = server.client_builder().build().await;
        let room_id = owned_room_id!("!preview");
        client.base_client().get_or_create_room(&room_id, RoomState::Joined);
        let event_cache = client.event_cache();
        event_cache.subscribe().unwrap();
        client.event_cache_store().lock().await.unwrap().as_clean().unwrap()
            .handle_linked_chunk_updates(LinkedChunkId::Room(&room_id), vec![
                Update::NewItemsChunk { previous: None, new: ChunkIdentifier::new(0), next: None },
                Update::PushItems { at: Position::new(ChunkIdentifier::new(0), 0), items: events },
            ]).await.unwrap();
        let (cache, _) = event_cache.room(&room_id).await.unwrap();
        let weak = WeakRoom::new(WeakClient::from_client(&client), room_id);
        let latest = super::With::inner(LatestEvent::new(&weak, None));
        Self { server, client, cache, latest }
    }

    async fn gate(&mut self, boundary: &EventId) {
        self.latest.install_local_clear_boundary(boundary).await;
        self.recompute().await;
    }

    async fn recompute(&mut self) {
        self.latest.update_with_event_cache(&self.cache, user_id!("@alice:server.org"), None).await;
    }

    async fn assert_id(&self, expected: Option<&EventId>) {
        let value = self.latest.get().await;
        assert!(!value.is_local());
        assert_eq!(value.event_id().as_deref(), expected);
        if expected.is_none() {
            assert_matches!(value, LatestEventValue::None);
        }
    }

    fn local_update(&self) -> RoomSendQueueUpdate {
        let queue = self.client.send_queue().for_room(self.client.get_room(&owned_room_id!("!preview")).unwrap());
        let transaction_id: OwnedTransactionId = "pending".into();
        RoomSendQueueUpdate::NewLocalEvent(LocalEcho {
            transaction_id: transaction_id.clone(),
            content: LocalEchoContent::Event {
                serialized_event: SerializableEventContent::new(&AnyMessageLikeEventContent::RoomMessage(
                    RoomMessageEventContent::text_plain("pending"),
                )).unwrap(),
                send_handle: SendHandle::new(queue, transaction_id, MilliSecondsSinceUnixEpoch::now()),
                send_error: None,
            },
        })
    }

    async fn local(&mut self) {
        self.latest.update_with_send_queue(
            &self.local_update(), &self.cache, user_id!("@alice:server.org"), None,
        ).await;
    }
}

#[async_test]
async fn test_local_clear_preview_reaction_boundary() {
    let f = factory();
    let mut t = Fixture::new(vec![
        f.text_msg("old").event_id(event_id!("$old")).into(),
        f.reaction(event_id!("$old"), "+1").event_id(event_id!("$boundary")).into(),
    ]).await;
    t.gate(event_id!("$boundary")).await;
    t.assert_id(None).await;
}

#[async_test]
async fn test_local_clear_preview_state_boundary() {
    let f = factory();
    let mut t = Fixture::new(vec![
        f.text_msg("old").event_id(event_id!("$old")).into(),
        f.room_topic("topic").event_id(event_id!("$boundary")).into(),
    ]).await;
    t.gate(event_id!("$boundary")).await;
    t.assert_id(None).await;
}

#[async_test]
async fn test_local_clear_preview_new_message() {
    let f = factory();
    let mut t = Fixture::new(vec![
        f.text_msg("boundary").event_id(event_id!("$boundary")).into(),
        f.text_msg("new").event_id(event_id!("$new")).into(),
    ]).await;
    t.gate(event_id!("$boundary")).await;
    t.assert_id(Some(event_id!("$new"))).await;
}

#[async_test]
async fn test_local_clear_preview_old_boundary_new() {
    let f = factory();
    let mut t = Fixture::new(vec![
        f.text_msg("old").event_id(event_id!("$old")).into(),
        f.room_topic("topic").event_id(event_id!("$boundary")).into(),
        f.text_msg("new").event_id(event_id!("$new")).into(),
    ]).await;
    t.gate(event_id!("$boundary")).await;
    t.assert_id(Some(event_id!("$new"))).await;
}

#[async_test]
async fn test_local_clear_preview_redaction_does_not_fall_back() {
    let f = factory();
    let mut t = Fixture::new(vec![
        f.text_msg("old").event_id(event_id!("$old")).into(),
        f.room_topic("topic").event_id(event_id!("$boundary")).into(),
        f.text_msg("new").event_id(event_id!("$new")).into(),
    ]).await;
    t.gate(event_id!("$boundary")).await;
    t.server.sync_room(&t.client, JoinedRoomBuilder::new(&owned_room_id!("!preview"))
        .add_timeline_event(f.redaction(event_id!("$new")))).await;
    t.recompute().await;
    t.assert_id(None).await;
}

#[async_test]
async fn test_local_clear_preview_unknown_boundary() {
    let mut t = Fixture::new(vec![factory().text_msg("unproven").event_id(event_id!("$new")).into()]).await;
    t.gate(event_id!("$missing")).await;
    t.assert_id(None).await;
}

#[async_test]
async fn test_local_clear_preview_later_boundary_recomputes_observer() {
    let f = factory();
    let t = Fixture::new(vec![f.text_msg("old").event_id(event_id!("$old")).into()]).await;
    let latest = t.client.latest_events().await;
    let value = latest.latest_event_after_boundary(&owned_room_id!("!preview"), event_id!("$boundary")).await.unwrap();
    assert_matches!(value, LatestEventValue::None);
    let mut updates = latest.listen_and_subscribe_to_room(&owned_room_id!("!preview")).await.unwrap().unwrap();
    assert_matches!(updates.next_now().await, LatestEventValue::None);
    t.server.sync_room(&t.client, JoinedRoomBuilder::new(&owned_room_id!("!preview"))
        .add_timeline_event(f.room_topic("boundary").event_id(event_id!("$boundary")))
        .add_timeline_event(f.text_msg("new").event_id(event_id!("$new")))).await;
    assert_next_matches!(updates, LatestEventValue::Remote(event) => {
        assert_eq!(event.event_id(), Some(event_id!("$new")));
    });
}

#[async_test]
async fn test_local_clear_preview_no_boundary_preserves_local_behavior() {
    let mut t = Fixture::new(vec![]).await;
    t.local().await;
    assert_matches!(t.latest.get().await, LatestEventValue::LocalIsSending(_));
}

#[async_test]
async fn test_local_clear_preview_pre_clear_pending_is_hidden() {
    let mut t = Fixture::new(vec![factory().text_msg("boundary").event_id(event_id!("$boundary")).into()]).await;
    t.local().await;
    assert_matches!(t.latest.get().await, LatestEventValue::LocalIsSending(_));
    t.gate(event_id!("$boundary")).await;
    t.assert_id(None).await;
}

#[async_test]
async fn test_local_clear_preview_post_clear_pending_is_hidden() {
    let mut t = Fixture::new(vec![factory().text_msg("boundary").event_id(event_id!("$boundary")).into()]).await;
    t.gate(event_id!("$boundary")).await;
    t.local().await;
    t.assert_id(None).await;
}

#[async_test]
async fn test_local_clear_preview_ack_waits_for_remote_cache() {
    let f = factory();
    let mut t = Fixture::new(vec![f.text_msg("boundary").event_id(event_id!("$boundary")).into()]).await;
    t.gate(event_id!("$boundary")).await;
    t.local().await;
    t.latest.update_with_send_queue(&RoomSendQueueUpdate::SentEvent {
        transaction_id: "pending".into(), event_id: event_id!("$new").to_owned(),
    }, &t.cache, user_id!("@alice:server.org"), None).await;
    t.assert_id(None).await;
    t.server.sync_room(&t.client, JoinedRoomBuilder::new(&owned_room_id!("!preview"))
        .add_timeline_event(f.text_msg("new").event_id(event_id!("$new")))).await;
    t.recompute().await;
    t.assert_id(Some(event_id!("$new"))).await;
}

#[async_test]
async fn test_local_clear_preview_restored_room_info_is_gated() {
    let mut t = Fixture::new(vec![factory().text_msg("old").event_id(event_id!("$old")).into()]).await;
    t.recompute().await;
    let weak = WeakRoom::new(WeakClient::from_client(&t.client), owned_room_id!("!preview"));
    t.latest = super::With::inner(LatestEvent::new(&weak, None));
    t.assert_id(Some(event_id!("$old"))).await;
    t.gate(event_id!("$missing")).await;
    t.assert_id(None).await;
}
