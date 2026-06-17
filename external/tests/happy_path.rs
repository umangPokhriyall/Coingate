//! Happy-path end-to-end on the mocks (no infra). Proves the trait wiring and, crucially,
//! that one logical withdrawal results in exactly one `send` per key (Invariant #2's
//! instrument): `send_count(key) == 1`.

use external::{Chain, CountingMockSigner, DepositEvent, MockChain, SendRequest, Signer, TxKind};
use uuid::Uuid;

fn sample_deposit(memo: &str) -> DepositEvent {
    DepositEvent {
        signature: "sig-1111".to_string(),
        slot: 42,
        memo_id: Some(memo.to_string()),
        kind: TxKind::Sol,
        from: Some("alice".to_string()),
        to: Some("fat-wallet".to_string()),
        amount: Some(1_000_000),
        token_mint: None,
        token_decimals: None,
    }
}

#[tokio::test]
async fn mock_chain_emits_matching_deposit() {
    // create order → MockChain emits a matching deposit (the credit side runs on this event).
    let memo = "order-memo-abc";
    let chain = MockChain::new(vec![sample_deposit(memo)]);

    let (events, cursor) = chain.deposits_since(None).await.unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].memo_id.as_deref(), Some(memo));
    assert_eq!(events[0].amount, Some(1_000_000));
    assert!(cursor.is_some(), "a delivered batch advances the cursor");

    // Draining the script yields nothing more (no duplicate credit on the happy path).
    let (events2, cursor2) = chain.deposits_since(cursor).await.unwrap();
    assert!(events2.is_empty());
    assert!(cursor2.is_none());
}

#[tokio::test]
async fn withdrawal_sends_exactly_once_via_counting_mock() {
    // create withdrawal → worker sends via CountingMockSigner.
    let signer = CountingMockSigner::new();
    let withdrawal_id = Uuid::new_v4();

    let sig = signer
        .send(SendRequest {
            key: withdrawal_id,
            to: "dest-address",
            amount: 1_000_000,
            mint: None,
        })
        .await
        .expect("happy-path send succeeds");

    // The invariant the whole project exists to prove, in miniature.
    assert_eq!(signer.send_count(withdrawal_id), 1);

    // Reconciliation returns the prior signature and does NOT count as a send.
    let looked_up = signer.lookup(withdrawal_id).await.unwrap();
    assert_eq!(looked_up, Some(sig));
    assert_eq!(
        signer.send_count(withdrawal_id),
        1,
        "lookup must not increment the send count"
    );

    // A naive re-send on redelivery (the bug Phase 1.4 prevents) would drive the count to 2.
    signer
        .send(SendRequest {
            key: withdrawal_id,
            to: "dest-address",
            amount: 1_000_000,
            mint: None,
        })
        .await
        .unwrap();
    assert_eq!(
        signer.send_count(withdrawal_id),
        2,
        "re-sending the same key is detectable as a second send"
    );
}
