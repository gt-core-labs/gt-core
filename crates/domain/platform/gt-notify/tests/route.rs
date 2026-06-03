//! Gate tests for the Notifier port: routing policy + fake capture.

use gt_notify::{route, Channel, FakeNotifier, Notification, Notifier, Severity, Signal};

#[test]
fn all_three_operator_signals_route_to_mail() {
    assert_eq!(
        route(&Signal::WorkerStuck {
            worker: "w1".into(),
            age_secs: 700
        }),
        Channel::Mail
    );
    assert_eq!(
        route(&Signal::MergeStuck {
            bead: "b1".into(),
            reason: "conflict".into()
        }),
        Channel::Mail
    );
    assert_eq!(
        route(&Signal::QuotaBlock {
            account: "acc-1".into()
        }),
        Channel::Mail
    );
}

#[test]
fn for_signal_renders_severity_and_subject() {
    let n = Notification::for_signal(Signal::WorkerStuck {
        worker: "toast".into(),
        age_secs: 900,
    });
    assert_eq!(n.severity, Severity::Urgent);
    assert!(n.subject.contains("toast"));
    assert!(n.body.contains("900"));
}

#[test]
fn fake_notifier_captures_in_order() {
    let fake = FakeNotifier::new();
    assert!(fake.is_empty());

    fake.notify(&Notification::for_signal(Signal::QuotaBlock {
        account: "acc-1".into(),
    }));
    fake.notify(&Notification::for_signal(Signal::MergeStuck {
        bead: "b1".into(),
        reason: "hook failed".into(),
    }));

    let got = fake.captured();
    assert_eq!(got.len(), 2);
    assert_eq!(got[0].signal.tag(), "quota_block");
    assert_eq!(got[1].signal.tag(), "merge_stuck");
}
