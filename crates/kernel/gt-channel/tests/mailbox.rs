//! Mailbox round-trips: drain existing files at subscribe time, then deliver live writes,
//! then re-deliver if not acked. Each test owns a unique directory.

use std::time::Duration;

use gt_channel::Channel;

fn fresh_root(label: &str) -> std::path::PathBuf {
    let p = std::env::temp_dir().join(format!(
        "gt-channel-{}-{}-{}",
        label,
        std::process::id(),
        ulid::Ulid::new()
    ));
    let _ = std::fs::remove_dir_all(&p);
    p
}

async fn recv_one(rx: &mut tokio::sync::mpsc::Receiver<gt_channel::ChannelMessage>) -> gt_channel::ChannelMessage {
    tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("timed out waiting for channel message")
        .expect("channel closed")
}

#[tokio::test]
async fn live_emit_delivers_to_subscriber() {
    let root = fresh_root("live");
    let ch = Channel::open(&root, "merge").unwrap();
    let mut rx = ch.subscribe(8).unwrap();

    ch.emit(b"hello").unwrap();
    let msg = recv_one(&mut rx).await;
    assert_eq!(msg.payload, b"hello");
    ch.ack(&msg).unwrap();

    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn existing_files_are_drained_on_subscribe() {
    let root = fresh_root("drain");
    let ch = Channel::open(&root, "merge").unwrap();
    ch.emit(b"first").unwrap();
    ch.emit(b"second").unwrap();

    let mut rx = ch.subscribe(8).unwrap();
    let m1 = recv_one(&mut rx).await;
    let m2 = recv_one(&mut rx).await;
    // The channel guarantees both preexisting files are drained, but delivery is unordered:
    // when both emits land in the same millisecond their ULID time bits tie and the random
    // suffix decides drain order (see Channel::subscribe docs). Assert set-equality, not
    // sequence — the refinery treats each message as an independent slot, so order is not
    // part of the contract.
    let mut payloads: Vec<&[u8]> = vec![&m1.payload, &m2.payload];
    payloads.sort();
    assert_eq!(payloads, vec![&b"first"[..], &b"second"[..]]);
    ch.ack(&m1).unwrap();
    ch.ack(&m2).unwrap();

    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn unacked_messages_redelivered_on_resubscribe() {
    let root = fresh_root("redeliver");
    let ch = Channel::open(&root, "merge").unwrap();
    ch.emit(b"keep").unwrap();

    let mut rx1 = ch.subscribe(8).unwrap();
    let msg1 = recv_one(&mut rx1).await;
    assert_eq!(msg1.payload, b"keep");
    drop(rx1); // no ack — file stays

    let mut rx2 = ch.subscribe(8).unwrap();
    let msg2 = recv_one(&mut rx2).await;
    assert_eq!(msg2.payload, b"keep");
    assert_eq!(msg2.id, msg1.id, "same backing file must redeliver");
    ch.ack(&msg2).unwrap();

    let _ = std::fs::remove_dir_all(&root);
}
