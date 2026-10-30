//! Unit tests moved from `src/serde_helpers.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use chrono::{DateTime, Utc};
    pub use rusternetes_common::serde_helpers::empty_as_none as empty_string_as_none;
    pub use serde::{Deserialize, Deserializer};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::serde_helpers::*;
    use serde::Serialize;

    #[derive(Debug, Deserialize, Serialize, PartialEq)]
    enum Policy {
        Cluster,
        Local,
    }

    #[derive(Debug, Deserialize)]
    struct Holder {
        #[serde(default, deserialize_with = "empty_as_none")]
        policy: Option<Policy>,
    }

    #[test]
    fn empty_as_none_cases() {
        let h: Holder = serde_json::from_str(r#"{"policy": ""}"#).unwrap();
        assert_eq!(h.policy, None);
        let h: Holder = serde_json::from_str(r#"{}"#).unwrap();
        assert_eq!(h.policy, None);
        let h: Holder = serde_json::from_str(r#"{"policy": null}"#).unwrap();
        assert_eq!(h.policy, None);
        let h: Holder = serde_json::from_str(r#"{"policy": "Local"}"#).unwrap();
        assert_eq!(h.policy, Some(Policy::Local));
        assert!(serde_json::from_str::<Holder>(r#"{"policy": "Galactic"}"#).is_err());
    }

    #[test]
    fn empty_as_none_string() {
        #[derive(Deserialize)]
        struct W {
            #[serde(default, deserialize_with = "empty_as_none")]
            rv: Option<String>,
        }
        let w: W = serde_json::from_str(r#"{"rv": ""}"#).unwrap();
        assert_eq!(w.rv, None);
        let w: W = serde_json::from_str(r#"{"rv": "7"}"#).unwrap();
        assert_eq!(w.rv.as_deref(), Some("7"));
    }

    #[test]
    fn null_default_cases() {
        #[derive(Deserialize)]
        struct W {
            #[serde(default, deserialize_with = "null_default")]
            v: Vec<u8>,
            #[serde(default, deserialize_with = "null_default")]
            n: i32,
            #[serde(default, deserialize_with = "deserialize_null_string")]
            s: String,
        }
        let w: W = serde_json::from_str(r#"{"v":null,"n":null,"s":null}"#).unwrap();
        assert!(w.v.is_empty() && w.n == 0 && w.s.is_empty());
        let w: W = serde_json::from_str(r#"{}"#).unwrap();
        assert!(w.v.is_empty() && w.n == 0 && w.s.is_empty());
        let w: W = serde_json::from_str(r#"{"v":[1,2],"n":3,"s":"x"}"#).unwrap();
        assert_eq!((w.v, w.n, w.s.as_str()), (vec![1, 2], 3, "x"));
    }

    #[test]
    fn zero_as_none_and_is_none_or_zero() {
        #[derive(Serialize, Deserialize, Debug, PartialEq)]
        struct W {
            #[serde(
                default,
                deserialize_with = "zero_as_none",
                skip_serializing_if = "is_none_or_zero"
            )]
            n: Option<i64>,
            #[serde(
                default,
                deserialize_with = "zero_as_none",
                skip_serializing_if = "is_none_or_zero"
            )]
            b: Option<bool>,
        }
        let w: W = serde_json::from_str(r#"{"n":0,"b":false}"#).unwrap();
        assert_eq!(w, W { n: None, b: None });
        let w: W = serde_json::from_str(r#"{"n":null}"#).unwrap();
        assert_eq!(w, W { n: None, b: None });
        let w: W = serde_json::from_str(r#"{"n":3,"b":true}"#).unwrap();
        assert_eq!(
            w,
            W {
                n: Some(3),
                b: Some(true)
            }
        );
        // Some(zero) built in code is still omitted, as Go omitempty would.
        let z = W {
            n: Some(0),
            b: Some(false),
        };
        assert_eq!(serde_json::to_string(&z).unwrap(), "{}");
        assert_eq!(serde_json::to_string(&w).unwrap(), r#"{"n":3,"b":true}"#);
    }

    #[test]
    fn bytes_b64_round_trip_and_null() {
        #[derive(Serialize, Deserialize)]
        struct W {
            #[serde(default, with = "bytes_b64")]
            d: Vec<u8>,
        }
        let w = W { d: b"hi".to_vec() };
        let j = serde_json::to_string(&w).unwrap();
        assert_eq!(j, r#"{"d":"aGk="}"#);
        let back: W = serde_json::from_str(&j).unwrap();
        assert_eq!(back.d, b"hi");
        let n: W = serde_json::from_str(r#"{"d":null}"#).unwrap();
        assert!(n.d.is_empty());
        assert!(serde_json::from_str::<W>(r#"{"d":"!!"}"#).is_err());
    }

    #[test]
    fn required_time_accepts_null_and_zero_marshals_null() {
        #[derive(Serialize, Deserialize)]
        struct W {
            #[serde(with = "k8s_time_required")]
            t: DateTime<Utc>,
            #[serde(with = "k8s_micro_time_required")]
            m: DateTime<Utc>,
        }
        let w: W = serde_json::from_str(r#"{"t":null,"m":null}"#).unwrap();
        assert_eq!(w.t, go_zero_time());
        assert_eq!(serde_json::to_string(&w).unwrap(), r#"{"t":null,"m":null}"#);
    }

    #[test]
    fn time_family_formats() {
        #[derive(Serialize, Deserialize)]
        struct W {
            #[serde(with = "k8s_time")]
            t: Option<DateTime<Utc>>,
            #[serde(with = "k8s_micro_time")]
            m: Option<DateTime<Utc>>,
        }
        let w: W =
            serde_json::from_str(r#"{"t":"2026-03-29T21:05:27.27Z","m":"2026-03-29T21:05:27Z"}"#)
                .unwrap();
        assert_eq!(
            serde_json::to_string(&w).unwrap(),
            r#"{"t":"2026-03-29T21:05:27Z","m":"2026-03-29T21:05:27.000000Z"}"#
        );
    }

    #[test]
    fn k8s_float_integral_is_integer() {
        #[derive(Serialize)]
        struct W {
            #[serde(with = "k8s_float")]
            f: Option<f64>,
        }
        assert_eq!(
            serde_json::to_string(&W { f: Some(10.0) }).unwrap(),
            r#"{"f":10}"#
        );
        assert_eq!(
            serde_json::to_string(&W { f: Some(1.5) }).unwrap(),
            r#"{"f":1.5}"#
        );
    }
}
