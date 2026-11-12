// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Authenticated KVIndexer observations; absent survival evidence stays absent.
use super::ExecutionFeedbackError;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LossBucket {
    pub lost_tokens: u64,
    pub count: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CacheObservation {
    pub schema_version: u32,
    pub execution_epoch: u64,
    pub observed_at: DateTime<Utc>,
    pub source_kind: String,
    pub backend_kind: String,
    pub source_version: String,
    pub worker_id: u64,
    pub worker_epoch: String,
    #[serde(default)]
    pub dp_rank: u32,
    pub prefix_digest: String,
    pub history_tokens: u64,
    pub device_overlap_tokens: u64,
    pub gap_pending: bool,
    pub event_watermark: u64,
    #[serde(default)]
    pub valid: bool,
    #[serde(default)]
    pub invalid_reason: Option<String>,
    #[serde(default)]
    pub loss_events: Option<u64>,
    #[serde(default)]
    pub exposure_ms: Option<f64>,
    #[serde(default)]
    pub sample_count: Option<u64>,
    #[serde(default)]
    pub stats_observed_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub loss_buckets: Option<Vec<LossBucket>>,
    #[serde(default)]
    pub model_id: Option<String>,
    #[serde(default)]
    pub calibration_key: Option<String>,
}

impl CacheObservation {
    pub fn validate(&self, epoch: u64, now: DateTime<Utc>) -> Result<(), ExecutionFeedbackError> {
        if self.execution_epoch != epoch {
            return Err(ExecutionFeedbackError::StaleEpoch);
        }
        if self.schema_version != 1
            || self.source_kind != "live_kvindexer_observation"
            || !["mocker", "gpu"].contains(&self.backend_kind.as_str())
            || [
                &self.source_version,
                &self.worker_epoch,
                &self.prefix_digest,
            ]
            .iter()
            .any(|s| s.is_empty() || s.len() > 512)
            || self.device_overlap_tokens > self.history_tokens
            || self.observed_at > now + chrono::Duration::milliseconds(250)
            || self.observed_at < now - chrono::Duration::seconds(60)
            || self.invalid_reason.as_ref().is_some_and(|s| s.len() > 512)
            || self
                .model_id
                .as_ref()
                .is_some_and(|s| s.is_empty() || s.len() > 512)
            || self
                .calibration_key
                .as_ref()
                .is_some_and(|s| s.is_empty() || s.len() > 512)
        {
            return Err(ExecutionFeedbackError::InvalidUpdate);
        }
        let stats_present = self.loss_events.is_some()
            || self.exposure_ms.is_some()
            || self.sample_count.is_some()
            || self.stats_observed_at.is_some()
            || self.loss_buckets.is_some();
        if stats_present {
            let (Some(events), Some(exposure), Some(samples), Some(at), Some(buckets)) = (
                self.loss_events,
                self.exposure_ms,
                self.sample_count,
                self.stats_observed_at,
                self.loss_buckets.as_ref(),
            ) else {
                return Err(ExecutionFeedbackError::InvalidUpdate);
            };
            if !exposure.is_finite()
                || exposure <= 0.0
                || samples == 0
                || events > samples
                || at > self.observed_at + chrono::Duration::milliseconds(250)
                || buckets.len() > 128
                || buckets.iter().any(|b| {
                    b.lost_tokens == 0 || b.lost_tokens > self.history_tokens || b.count == 0
                })
                || buckets
                    .iter()
                    .try_fold(0_u64, |total, b| total.checked_add(b.count))
                    != Some(events)
            {
                return Err(ExecutionFeedbackError::InvalidUpdate);
            }
        }
        Ok(())
    }

    pub fn snapshot(&self, now: DateTime<Utc>) -> serde_json::Value {
        let age_ms = (now - self.observed_at).num_milliseconds().max(0);
        let mut value = serde_json::to_value(self).expect("cache observation serializes");
        value["age_ms"] = age_ms.into();
        if self.gap_pending || age_ms > 2000 {
            value["valid"] = false.into();
            value["invalid_reason"] = if self.gap_pending {
                "event_gap"
            } else {
                "stale_observation"
            }
            .into();
        }
        value
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    pub(crate) fn observation(epoch: u64) -> CacheObservation {
        serde_json::from_value(serde_json::json!({"schema_version":1,"execution_epoch":epoch,"observed_at":Utc::now(),"source_kind":"live_kvindexer_observation","backend_kind":"mocker","source_version":"locked-indexer","worker_id":7,"worker_epoch":"run:pid:start","prefix_digest":"prefix-256","history_tokens":256,"device_overlap_tokens":256,"gap_pending":false,"event_watermark":20,"valid":true})).unwrap()
    }
    #[test]
    fn missing_stats_remain_null_and_expiry_gap_epoch_are_fenced() {
        let mut o = observation(3);
        assert!(o.validate(3, Utc::now()).is_ok());
        assert_eq!(
            o.snapshot(Utc::now())["loss_events"],
            serde_json::Value::Null
        );
        assert_eq!(
            o.snapshot(o.observed_at + chrono::Duration::seconds(3))["valid"],
            false
        );
        o.gap_pending = true;
        assert_eq!(o.snapshot(Utc::now())["invalid_reason"], "event_gap");
        assert_eq!(
            o.validate(4, Utc::now()),
            Err(ExecutionFeedbackError::StaleEpoch)
        );
    }
    #[test]
    fn validates_target_prefix_survival_exposure_and_loss_buckets() {
        let mut o = observation(3);
        o.loss_events = Some(2);
        o.exposure_ms = Some(10000.0);
        o.sample_count = Some(4);
        o.stats_observed_at = Some(o.observed_at);
        o.loss_buckets = Some(vec![LossBucket {
            lost_tokens: 32,
            count: 2,
        }]);
        assert!(o.validate(3, Utc::now()).is_ok());
        o.loss_buckets.as_mut().unwrap()[0].lost_tokens = 300;
        assert_eq!(
            o.validate(3, Utc::now()),
            Err(ExecutionFeedbackError::InvalidUpdate)
        );
    }
}
