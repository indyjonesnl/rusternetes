//! Unit tests moved from `src/event_correlator.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use chrono::{DateTime, Utc};
    pub use std::cell::Cell;
    pub use std::collections::{HashMap, HashSet};
    pub use std::rc::Rc;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::event_correlator::*;

    fn make_object_reference(kind: &str, name: &str, namespace: &str) -> CorrelatorObjectReference {
        CorrelatorObjectReference {
            kind: kind.to_string(),
            name: name.to_string(),
            namespace: namespace.to_string(),
            uid: "C934D34AFB20242".to_string(),
            api_version: "version".to_string(),
            field_path: "spec.containers{mycontainer}".to_string(),
        }
    }

    fn make_event(
        reason: &str,
        message: &str,
        involved_object: CorrelatorObjectReference,
    ) -> CorrelatorEvent {
        let now = Utc::now();
        CorrelatorEvent {
            name: String::new(),
            namespace: String::new(),
            resource_version: String::new(),
            reason: reason.to_string(),
            message: message.to_string(),
            involved_object,
            source: CorrelatorEventSource {
                component: "kubelet".to_string(),
                host: "kublet.node1".to_string(),
            },
            event_type: CorrelatorEventType::Normal,
            reporting_controller: String::new(),
            reporting_instance: String::new(),
            count: 1,
            first_timestamp: now,
            last_timestamp: now,
        }
    }

    fn make_events(num: usize, template: &CorrelatorEvent) -> Vec<CorrelatorEvent> {
        (0..num).map(|_| template.clone()).collect()
    }

    fn make_unique_events(num: usize) -> Vec<CorrelatorEvent> {
        let kind = "Pod";
        (0..num)
            .map(|i| {
                let reason = format!("reason-{i}");
                let message = format!("message-{i}");
                let name = format!("pod-{i}");
                let namespace = format!("ns-{i}");
                make_event(
                    &reason,
                    &message,
                    make_object_reference(kind, &name, &namespace),
                )
            })
            .collect()
    }

    fn make_similar_events(
        num: usize,
        template: &CorrelatorEvent,
        message_prefix: &str,
    ) -> Vec<CorrelatorEvent> {
        let mut events = make_events(num, template);
        for (i, ev) in events.iter_mut().enumerate() {
            ev.message = format!("{}-{}-{}", message_prefix, i, ev.message);
        }
        events
    }

    fn set_count(mut event: CorrelatorEvent, count: i32) -> CorrelatorEvent {
        event.count = count;
        event
    }

    /// Faithful port of upstream `validateEvent`: checks timestamps reflect
    /// compression, name prefix, and field-by-field equality (ignoring the
    /// timestamps that we don't control exactly).
    fn validate_event(
        message_prefix: &str,
        actual_event: &CorrelatorEvent,
        expected_event: &CorrelatorEvent,
    ) {
        let mut recv_event = actual_event.clone();
        let expect_compression = expected_event.count > 1;

        let actual_first = recv_event.first_timestamp;
        let actual_last = recv_event.last_timestamp;
        if actual_first == actual_last {
            if expect_compression {
                panic!(
                    "{message_prefix} - FirstTimestamp ({actual_first}) and LastTimestamp \
                     ({actual_last}) must differ to indicate compression, but were the same"
                );
            }
        } else if expected_event.count == 1 {
            panic!(
                "{message_prefix} - FirstTimestamp ({actual_first}) and LastTimestamp \
                 ({actual_last}) must be equal for a single occurrence, but differed"
            );
        }

        // Temp clear timestamps for comparison (actual values don't matter).
        recv_event.first_timestamp = expected_event.first_timestamp;
        recv_event.last_timestamp = expected_event.last_timestamp;

        // reportingController is copied from expected (upstream does the same).
        recv_event.reporting_controller = expected_event.reporting_controller.clone();

        // Name must carry the expected prefix.
        assert!(
            recv_event.name.starts_with(&expected_event.name),
            "{message_prefix} - Name '{}' does not contain prefix '{}'",
            recv_event.name,
            expected_event.name
        );
        recv_event.name = expected_event.name.clone();

        // resource_version is server-assigned; not compared (always empty in
        // these pure-logic scenarios on both sides).
        assert_eq!(
            expected_event, &recv_event,
            "{message_prefix} - events differ"
        );
    }

    // Port of TestEventAggregatorByReasonFunc.
    #[test]
    fn test_event_aggregator_by_reason_func() {
        let event1 = make_event(
            "end-of-world",
            "it was fun",
            make_object_reference("Pod", "pod1", "other"),
        );
        let event2 = make_event(
            "end-of-world",
            "it was awful",
            make_object_reference("Pod", "pod1", "other"),
        );
        let event3 = make_event(
            "nevermind",
            "it was a bug",
            make_object_reference("Pod", "pod1", "other"),
        );

        let (agg_key1, local_key1) = event_aggregator_by_reason_func(&event1);
        let (agg_key2, local_key2) = event_aggregator_by_reason_func(&event2);
        let (agg_key3, _) = event_aggregator_by_reason_func(&event3);

        assert_eq!(agg_key1, agg_key2, "Expected {agg_key1} equal {agg_key2}");
        assert_ne!(
            local_key1, local_key2,
            "Expected local keys to differ for different messages"
        );
        assert_ne!(
            agg_key1, agg_key3,
            "Expected aggregate keys to differ for different reasons"
        );
    }

    // Port of TestEventAggregatorByReasonMessageFunc.
    #[test]
    fn test_event_aggregator_by_reason_message_func() {
        let expected_prefix = "(combined from similar events): ";
        let event1 = make_event(
            "end-of-world",
            "it was fun",
            make_object_reference("Pod", "pod1", "other"),
        );
        let actual = event_aggregator_by_reason_message_func(&event1);
        assert!(
            actual.starts_with(expected_prefix),
            "Expected {actual} to begin with prefix {expected_prefix}"
        );
    }

    struct CorrelatorScenario {
        previous_events: Vec<CorrelatorEvent>,
        new_event: CorrelatorEvent,
        expected_event: Option<CorrelatorEvent>,
        interval_seconds: i64,
        expected_skip: bool,
    }

    // Port of TestEventCorrelator (all 8 scenarios).
    #[test]
    fn test_event_correlator() {
        let first_event = make_event(
            "first",
            "i am first",
            make_object_reference("Pod", "my-pod", "my-ns"),
        );
        let duplicate_event = make_event(
            "duplicate",
            "me again",
            make_object_reference("Pod", "my-pod", "my-ns"),
        );
        let unique_event = make_event(
            "unique",
            "snowflake",
            make_object_reference("Pod", "my-pod", "my-ns"),
        );
        let mut similar_event = make_event(
            "similar",
            "similar message",
            make_object_reference("Pod", "my-pod", "my-ns"),
        );
        similar_event.involved_object.field_path = "spec.containers{container1}".to_string();
        let aggregate_event = make_event(
            &similar_event.reason,
            &event_aggregator_by_reason_message_func(&similar_event),
            similar_event.involved_object.clone(),
        );
        let mut similar_but_different_container_event = similar_event.clone();
        similar_but_different_container_event
            .involved_object
            .field_path = "spec.containers{container2}".to_string();

        let scenarios: Vec<(&str, CorrelatorScenario)> = vec![
            (
                "create-a-single-event",
                CorrelatorScenario {
                    previous_events: vec![],
                    new_event: first_event.clone(),
                    expected_event: Some(set_count(first_event.clone(), 1)),
                    interval_seconds: 5,
                    expected_skip: false,
                },
            ),
            (
                "the-same-event-should-just-count",
                CorrelatorScenario {
                    previous_events: make_events(1, &duplicate_event),
                    new_event: duplicate_event.clone(),
                    expected_event: Some(set_count(duplicate_event.clone(), 2)),
                    interval_seconds: 5,
                    expected_skip: false,
                },
            ),
            (
                "the-same-event-should-just-count-even-if-more-than-aggregate",
                CorrelatorScenario {
                    previous_events: make_events(DEFAULT_AGGREGATE_MAX_EVENTS, &duplicate_event),
                    new_event: duplicate_event.clone(),
                    expected_event: Some(set_count(
                        duplicate_event.clone(),
                        DEFAULT_AGGREGATE_MAX_EVENTS as i32 + 1,
                    )),
                    interval_seconds: 30, // larger interval induces aggregation but not spam.
                    expected_skip: false,
                },
            ),
            (
                "the-same-event-is-spam-if-happens-too-frequently",
                CorrelatorScenario {
                    previous_events: make_events(DEFAULT_SPAM_BURST + 1, &duplicate_event),
                    new_event: duplicate_event.clone(),
                    expected_event: None,
                    interval_seconds: 1,
                    expected_skip: true,
                },
            ),
            (
                "create-many-unique-events",
                CorrelatorScenario {
                    previous_events: make_unique_events(30),
                    new_event: unique_event.clone(),
                    expected_event: Some(set_count(unique_event.clone(), 1)),
                    interval_seconds: 5,
                    expected_skip: false,
                },
            ),
            (
                "similar-events-should-aggregate-event",
                CorrelatorScenario {
                    previous_events: make_similar_events(
                        DEFAULT_AGGREGATE_MAX_EVENTS - 1,
                        &similar_event,
                        &similar_event.message,
                    ),
                    new_event: similar_event.clone(),
                    expected_event: Some(set_count(aggregate_event.clone(), 1)),
                    interval_seconds: 5,
                    expected_skip: false,
                },
            ),
            (
                "similar-events-many-times-should-count-the-aggregate",
                CorrelatorScenario {
                    previous_events: make_similar_events(
                        DEFAULT_AGGREGATE_MAX_EVENTS,
                        &similar_event,
                        &similar_event.message,
                    ),
                    new_event: similar_event.clone(),
                    expected_event: Some(set_count(aggregate_event.clone(), 2)),
                    interval_seconds: 5,
                    expected_skip: false,
                },
            ),
            (
                "events-from-different-containers-do-not-aggregate",
                CorrelatorScenario {
                    previous_events: make_events(1, &similar_but_different_container_event),
                    new_event: similar_event.clone(),
                    expected_event: Some(set_count(similar_event.clone(), 1)),
                    interval_seconds: 5,
                    expected_skip: false,
                },
            ),
            (
                "similar-events-whose-interval-is-greater-than-aggregate-interval-do-not-aggregate",
                CorrelatorScenario {
                    previous_events: make_similar_events(
                        DEFAULT_AGGREGATE_MAX_EVENTS - 1,
                        &similar_event,
                        &similar_event.message,
                    ),
                    new_event: similar_event.clone(),
                    expected_event: Some(set_count(similar_event.clone(), 1)),
                    interval_seconds: DEFAULT_AGGREGATE_INTERVAL_IN_SECONDS as i64,
                    expected_skip: false,
                },
            ),
        ];

        for (name, input) in scenarios {
            let event_interval = chrono::Duration::seconds(input.interval_seconds);
            // Shared clock handle: the correlator clones it internally, and the
            // test reads it directly to stamp events — mirroring upstream's
            // `&clock` pointer shared across all components.
            let clock = SimpleIntervalClock::new(Utc::now(), event_interval);
            let mut correlator = EventCorrelator::new(clock.clone());

            for prev in &input.previous_events {
                let mut event = prev.clone();
                // Snapshot clock for first/last timestamps (advances on read).
                let now = clock.now();
                event.first_timestamp = now;
                event.last_timestamp = now;
                let result = correlator.event_correlate(&event);
                if !result.skip {
                    correlator.update_state(result.event.as_ref().unwrap());
                }
            }

            let now = clock.now();
            let mut new_event = input.new_event.clone();
            new_event.first_timestamp = now;
            new_event.last_timestamp = now;
            let result = correlator.event_correlate(&new_event);

            assert_eq!(
                result.skip, input.expected_skip,
                "scenario {name}: expected skip {}, got {}",
                input.expected_skip, result.skip
            );

            if input.expected_skip {
                continue;
            }

            validate_event(
                name,
                result.event.as_ref().unwrap(),
                input.expected_event.as_ref().unwrap(),
            );
        }
    }

    // Port of TestEventSpamFilter.
    #[test]
    fn test_event_spam_filter() {
        fn spam_key_func_based_on_objects_and_reason(e: &CorrelatorEvent) -> String {
            [
                e.source.component.as_str(),
                e.source.host.as_str(),
                e.involved_object.kind.as_str(),
                e.involved_object.namespace.as_str(),
                e.involved_object.name.as_str(),
                e.involved_object.uid.as_str(),
                e.involved_object.api_version.as_str(),
                e.reason.as_str(),
            ]
            .concat()
        }

        let burst_size = 1;
        let event_interval = chrono::Duration::seconds(1);
        let original_event = make_event(
            "original",
            "i am first",
            make_object_reference("Pod", "my-pod", "my-ns"),
        );
        let different_reason_event = make_event(
            "duplicate",
            "me again",
            make_object_reference("Pod", "my-pod", "my-ns"),
        );
        let spam_event = make_event(
            "original",
            "me again",
            make_object_reference("Pod", "my-pod", "my-ns"),
        );

        struct SpamCase {
            new_event: CorrelatorEvent,
            expected_event: Option<CorrelatorEvent>,
            expected_skip: bool,
            spam_key_func: Option<EventSpamKeyFunc>,
        }

        let cases: Vec<(&str, SpamCase)> = vec![
            (
                "spam if object reference is the same for default spam filter",
                SpamCase {
                    new_event: different_reason_event.clone(),
                    expected_event: None,
                    expected_skip: true,
                    spam_key_func: None,
                },
            ),
            (
                "not spam if object same but reason differs for custom spam filter",
                SpamCase {
                    new_event: different_reason_event.clone(),
                    expected_event: Some(different_reason_event.clone()),
                    expected_skip: false,
                    spam_key_func: Some(spam_key_func_based_on_objects_and_reason),
                },
            ),
            (
                "spam if object+reason same but message differs for custom spam filter",
                SpamCase {
                    new_event: spam_event.clone(),
                    expected_event: None,
                    expected_skip: true,
                    spam_key_func: Some(spam_key_func_based_on_objects_and_reason),
                },
            ),
        ];

        for (desc, input) in cases {
            let clock = SimpleIntervalClock::new(Utc::now(), event_interval);
            let mut correlator = EventCorrelator::with_options(CorrelatorOptions {
                clock,
                burst_size: Some(burst_size),
                qps: None,
                key_func: None,
                message_func: None,
                max_events: None,
                max_interval_in_seconds: None,
                spam_key_func: input.spam_key_func,
            });

            // Emit the original event.
            let result = correlator.event_correlate(&original_event);
            if !result.skip {
                correlator.update_state(result.event.as_ref().unwrap());
            }

            let result = correlator.event_correlate(&input.new_event);

            assert_eq!(
                result.skip, input.expected_skip,
                "scenario {desc}: expected skip {}, got {}",
                input.expected_skip, result.skip
            );

            if input.expected_skip {
                continue;
            }

            validate_event(
                desc,
                result.event.as_ref().unwrap(),
                input.expected_event.as_ref().unwrap(),
            );
        }
    }

    /// The de-duplication logger produces a count-incremented event and a patch
    /// on the second observation of the exact same event.
    #[test]
    fn test_event_observe_produces_count_bump_and_patch() {
        let clock = FixedClock::new(Utc::now());
        let mut logger = EventLogger::new(clock);
        let event = make_event(
            "Started",
            "started pod",
            make_object_reference("Pod", "my-pod", "my-ns"),
        );
        let key = event.event_key();

        let (first, patch1) = logger.event_observe(&event, &key);
        assert_eq!(first.count, 1, "first observation count should be 1");
        assert!(patch1.is_none(), "no patch on first observation");

        let (second, patch2) = logger.event_observe(&event, &key);
        assert_eq!(second.count, 2, "second observation count should be 2");
        let patch = patch2.expect("patch expected on repeat observation");
        let patch_str = String::from_utf8(patch).unwrap();
        assert!(
            patch_str.contains("\"count\":2"),
            "patch should carry count=2, got {patch_str}"
        );
    }
}
