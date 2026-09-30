//! Ports of ipallocator_test.go and cidrallocator_test.go.

use super::cidr::MetaAllocator;
use super::*;
use rusternetes_common::resources::ServiceCIDR;
use rusternetes_storage::MemoryStorage;
use std::collections::HashSet;

fn allocator(cidr: &str) -> Allocator<MemoryStorage> {
    Allocator::new(cidr.parse().unwrap(), Arc::new(MemoryStorage::new())).unwrap()
}

fn ip(s: &str) -> IpAddr {
    s.parse().unwrap()
}

/// `TestAllocateIPAllocator` (ipallocator_test.go:77). The IPv6 case uses a
/// /120 rather than upstream's /116: every allocation here re-lists the
/// IPAddresses from storage, where upstream reads an informer cache, and 4095
/// of them make the test quadratic.
#[tokio::test]
async fn allocate_ip_allocator() {
    struct Case {
        name: &'static str,
        cidr: &'static str,
        free: u64,
        released: &'static str,
        out_of_range: &'static [&'static str],
        already_allocated: &'static str,
    }
    let cases = [
        Case {
            name: "IPv4",
            cidr: "192.168.1.0/24",
            free: 254,
            released: "192.168.1.5",
            out_of_range: &["192.168.0.1", "192.168.1.0", "192.168.1.255", "192.168.2.2"],
            already_allocated: "192.168.1.1",
        },
        Case {
            name: "IPv6",
            cidr: "2001:db8:1::/120",
            free: 255,
            released: "2001:db8:1::5",
            out_of_range: &["2001:db8::1", "2001:db8:1::", "2001:db8:2::2"],
            already_allocated: "2001:db8:1::1",
        },
    ];
    for tc in cases {
        let r = allocator(tc.cidr);
        assert_eq!(r.free().await, tc.free, "{}", tc.name);
        assert_eq!(r.used().await, 0, "{}", tc.name);
        let mut found = HashSet::new();
        while r.free().await > 0 {
            let ip = r.allocate_next_service(None, false).await.unwrap();
            assert!(found.insert(ip), "{}: allocated {ip} twice", tc.name);
        }
        assert!(r.allocate_next_service(None, false).await.is_err());
        assert!(found.contains(&ip(tc.released)), "{}", tc.name);

        r.release(ip(tc.released), false).await.unwrap();
        assert_eq!(r.free().await, 1, "{}", tc.name);
        assert_eq!(r.used().await as u64, tc.free - 1, "{}", tc.name);
        let again = r.allocate_next_service(None, false).await.unwrap();
        assert_eq!(again, ip(tc.released), "{}", tc.name);

        r.release(ip(tc.released), false).await.unwrap();
        for out in tc.out_of_range {
            assert!(
                r.allocate_service(None, ip(out), false).await.is_err(),
                "{}: allocated {out}",
                tc.name
            );
        }
        assert!(matches!(
            r.allocate_service(None, ip(tc.already_allocated), false)
                .await,
            Err(IpError::Allocated)
        ));
        assert_eq!(r.free().await, 1, "{}", tc.name);
        r.allocate_service(None, ip(tc.released), false)
            .await
            .unwrap();
        assert_eq!(r.free().await, 0, "{}", tc.name);
        assert_eq!(r.used().await as u64, tc.free, "{}", tc.name);
    }
}

/// `TestAllocateTinyIPAllocator`.
#[tokio::test]
async fn allocate_tiny_ip_allocator() {
    let r = allocator("192.168.1.0/32");
    assert_eq!(r.free().await, 0);
    assert!(r.allocate_next_service(None, false).await.is_err());
}

/// `TestAllocateReservedIPAllocator`: dynamic allocation fills the upper
/// band before touching the static one.
#[tokio::test]
async fn allocate_reserved_ip_allocator() {
    let r = allocator("192.168.1.0/25");
    let offset = calculate_range_offset(&"192.168.1.0/25".parse().unwrap());
    assert_eq!(offset, 16);
    let dynamic = r.size() - offset;
    for _ in 0..dynamic {
        r.allocate_next_service(None, false).await.unwrap();
    }
    for i in offset..r.size() {
        let addr = ip(&format!("192.168.1.{}", i + 1));
        assert!(r.has(addr).await, "{addr} expected to be allocated");
    }
    assert_eq!(r.free().await, offset);
    for i in 0..offset {
        r.allocate_service(None, ip(&format!("192.168.1.{}", i + 1)), false)
            .await
            .unwrap();
    }
    assert_eq!(r.free().await, 0);
    r.release(ip("192.168.1.10"), false).await.unwrap();
    r.allocate_next_service(None, false).await.unwrap();
    assert_eq!(r.free().await, 0);
}

/// `TestAllocateSmallIPAllocator`.
#[tokio::test]
async fn allocate_small_ip_allocator() {
    let r = allocator("192.168.1.240/30");
    assert_eq!(r.free().await, 2);
    let mut found = HashSet::new();
    for _ in 0..2 {
        let ip = r.allocate_next_service(None, false).await.unwrap();
        assert!(found.insert(ip));
    }
    for s in &found {
        assert!(r.has(*s).await);
        assert!(r.allocate_service(None, *s, false).await.is_err());
    }
    assert_eq!(r.free().await, 0);
    for _ in 0..100 {
        assert!(r.allocate_next_service(None, false).await.is_err());
    }
}

/// `Test_hostsPerNetwork` (ipallocator_test.go:732).
#[test]
fn hosts_per_network_table() {
    let cases: &[(&str, u64)] = &[
        ("192.168.1.0/24", 254),
        ("192.168.1.0/32", 0),
        ("192.168.1.0/31", 0),
        ("0.0.0.0/1", i32::MAX as u64 - 1),
        ("0.0.0.0/0", u32::MAX as u64 - 1),
        ("2001:db2::/112", 65535),
        ("2001:db8::/128", 0),
        ("2001:db8::/127", 1),
        ("2001:db8::/65", i64::MAX as u64),
        ("2001:db8::/64", u64::MAX),
        ("2001:db8::/1", u64::MAX),
    ];
    for (cidr, want) in cases {
        let prefix: IpNet = cidr.parse().unwrap();
        assert_eq!(hosts_per_network(&prefix.trunc()), *want, "{cidr}");
    }
}

/// `TestCalculateRangeOffset` (ipallocator/bitmap_test.go).
#[test]
fn range_offset_table() {
    let cases: &[(&str, u64)] = &[
        ("192.168.1.0/28", 0),
        ("192.168.1.0/27", 16),
        ("192.168.1.0/24", 16),
        ("192.168.1.0/20", 256),
        ("10.96.0.0/12", 256),
        ("fd00::/120", 16),
        ("fd00::/64", 256),
    ];
    for (cidr, want) in cases {
        assert_eq!(
            calculate_range_offset(&cidr.parse().unwrap()),
            *want,
            "{cidr}"
        );
    }
}

/// A new allocator refuses an IPv6 prefix shorter than /64
/// (ipallocator.go:86-88).
#[test]
fn short_ipv6_prefixes_are_rejected() {
    let err = Allocator::new(
        "2001:db8::/48".parse().unwrap(),
        Arc::new(MemoryStorage::new()),
    )
    .err()
    .unwrap();
    assert_eq!(
        err,
        "shortest allowed prefix length for service CIDR is 64, got 48"
    );
}

#[tokio::test]
async fn a_dry_run_creates_nothing() {
    let r = allocator("192.168.1.0/24");
    r.allocate_service(None, ip("192.168.1.7"), true)
        .await
        .unwrap();
    assert_eq!(
        r.allocate_next_service(None, true).await.unwrap(),
        ip("192.168.1.0")
    );
    assert_eq!(r.used().await, 0);
}

#[tokio::test]
async fn the_ip_address_references_the_service() {
    let r = allocator("192.168.1.0/24");
    let svc: Service = serde_json::from_value(serde_json::json!({
        "apiVersion": "v1", "kind": "Service",
        "metadata": {"name": "web", "namespace": "prod"}, "spec": {}
    }))
    .unwrap();
    let addr = r.allocate_next_service(Some(&svc), false).await.unwrap();
    let stored: IPAddress = r
        .storage
        .get(&ip_address_key(&addr.to_string()))
        .await
        .unwrap();
    let parent = stored.spec.unwrap().parent_ref.unwrap();
    assert_eq!(parent.resource, "services");
    assert_eq!(parent.namespace.as_deref(), Some("prod"));
    assert_eq!(parent.name, "web");
    let labels = stored.metadata.labels.unwrap();
    assert_eq!(labels[LABEL_IP_ADDRESS_FAMILY], "IPv4");
    assert_eq!(labels[LABEL_MANAGED_BY], CONTROLLER_NAME);
}

// ---------------------------------------------------------------------------
// MetaAllocator
// ---------------------------------------------------------------------------

async fn add_cidr(storage: &MemoryStorage, name: &str, cidr: &str) {
    let sc = ServiceCIDR::new(name, vec![cidr.to_string()]);
    storage
        .create(&build_key("servicecidrs", None, name), &sc)
        .await
        .unwrap();
}

async fn fill(r: &MetaAllocator<MemoryStorage>, found: &mut HashSet<IpAddr>) -> usize {
    let mut count = 0;
    while r.free().await > 0 {
        let ip = r.allocate_next_service(None, false).await.unwrap();
        assert!(found.insert(ip), "allocated {ip} twice");
        count += 1;
    }
    count
}

/// `TestCIDRAllocateMultiple` (cidrallocator_test.go:103).
#[tokio::test]
async fn cidr_allocate_multiple() {
    let storage = Arc::new(MemoryStorage::new());
    let r = MetaAllocator::new(storage.clone(), false);
    assert_eq!(r.free().await, 0);
    assert!(r.allocate_next_service(None, false).await.is_err());

    add_cidr(&storage, "test", "192.168.0.0/28").await;
    let mut found = HashSet::new();
    let mut count = fill(&r, &mut found).await;
    assert_eq!(count, 14);
    assert!(r.allocate_next_service(None, false).await.is_err());

    add_cidr(&storage, "test2", "10.0.0.0/28").await;
    r.allocate_service(None, ip("10.0.0.11"), false)
        .await
        .unwrap();
    count += 1;
    count += fill(&r, &mut found).await;
    assert_eq!(count, 28);
    assert!(r.allocate_next_service(None, false).await.is_err());
}

/// `TestCIDRAllocateShadow`: a prefix's network address is allocatable
/// from a larger prefix that contains it.
#[tokio::test]
async fn cidr_allocate_shadow() {
    let storage = Arc::new(MemoryStorage::new());
    let r = MetaAllocator::new(storage.clone(), false);
    add_cidr(&storage, "test", "192.168.1.0/24").await;
    assert!(r
        .allocate_service(None, ip("192.168.1.0"), false)
        .await
        .is_err());
    assert_eq!(r.used().await, 0);

    add_cidr(&storage, "test2", "192.168.0.0/16").await;
    r.allocate_service(None, ip("192.168.1.0"), false)
        .await
        .unwrap();
    assert_eq!(r.used().await, 1);
}

/// `TestCIDRAllocateGrow`.
#[tokio::test]
async fn cidr_allocate_grow() {
    let storage = Arc::new(MemoryStorage::new());
    let r = MetaAllocator::new(storage.clone(), false);
    add_cidr(&storage, "test", "192.168.0.0/28").await;
    let mut found = HashSet::new();
    let mut count = fill(&r, &mut found).await;
    assert_eq!(count, 14);

    add_cidr(&storage, "test2", "192.168.0.0/24").await;
    count += fill(&r, &mut found).await;
    assert_eq!(count, 254);
    assert!(r.allocate_next_service(None, false).await.is_err());
}

/// `TestCIDRAllocateShrink`.
#[tokio::test]
async fn cidr_allocate_shrink() {
    let storage = Arc::new(MemoryStorage::new());
    let r = MetaAllocator::new(storage.clone(), false);
    add_cidr(&storage, "test", "192.168.0.0/24").await;
    let mut found = HashSet::new();
    assert_eq!(fill(&r, &mut found).await, 254);
    for ip in &found {
        r.release(*ip, false).await.unwrap();
    }
    assert_eq!(r.used().await, 0);

    add_cidr(&storage, "cidr2", "192.168.0.0/28").await;
    storage
        .delete(&build_key("servicecidrs", None, "test"))
        .await
        .unwrap();
    assert!(!r.has(ip("192.168.0.253")).await);
    assert!(matches!(
        r.allocate_service(None, ip("192.168.0.253"), false).await,
        Err(IpError::MismatchedNetwork)
    ));
    let mut found = HashSet::new();
    assert_eq!(fill(&r, &mut found).await, 14);
    assert!(r.allocate_next_service(None, false).await.is_err());
}

/// `Test_isNotContained`'s rule, as `Free` applies it: a prefix inside
/// another one does not add to the size.
#[tokio::test]
async fn nested_prefixes_count_once() {
    let storage = Arc::new(MemoryStorage::new());
    let r = MetaAllocator::new(storage.clone(), false);
    add_cidr(&storage, "a", "192.168.0.0/24").await;
    add_cidr(&storage, "b", "192.168.0.0/28").await;
    add_cidr(&storage, "c", "10.0.0.0/28").await;
    assert_eq!(r.free().await, 254 + 14);
}

/// An unready ServiceCIDR allocates nothing new but still releases
/// (cidrallocator.go:299-322, 390-404).
#[tokio::test]
async fn an_unready_service_cidr_only_releases() {
    let storage = Arc::new(MemoryStorage::new());
    let r = MetaAllocator::new(storage.clone(), false);
    add_cidr(&storage, "test", "192.168.0.0/28").await;
    r.allocate_service(None, ip("192.168.0.5"), false)
        .await
        .unwrap();

    let key = build_key("servicecidrs", None, "test");
    let mut sc: ServiceCIDR = storage.get(&key).await.unwrap();
    sc.status = Some(rusternetes_common::resources::ServiceCIDRStatus {
        conditions: Some(vec![rusternetes_common::resources::ServiceCIDRCondition {
            condition_type: "Ready".to_string(),
            status: "False".to_string(),
            observed_generation: None,
            last_transition_time: None,
            reason: "Terminating".to_string(),
            message: String::new(),
        }]),
    });
    storage.update(&key, &sc).await.unwrap();

    assert!(matches!(
        r.allocate_next_service(None, false).await,
        Err(IpError::Full)
    ));
    assert!(matches!(
        r.allocate_service(None, ip("192.168.0.6"), false).await,
        Err(IpError::MismatchedNetwork)
    ));
    r.release(ip("192.168.0.5"), false).await.unwrap();
    assert_eq!(r.used().await, 0);
}
