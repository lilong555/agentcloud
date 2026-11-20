// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionBinding {
    pub source_id: String,
    pub tenant_id: String,
    pub workspace_id: String,
    pub session_id: String,
    pub actor_uid: String,
    pub epoch: u64,
}

impl ExecutionBinding {
    pub(crate) fn validate(&self) -> Result<(), ExecutionFeedbackError> {
        let identifiers = [
            &self.source_id,
            &self.tenant_id,
            &self.workspace_id,
            &self.session_id,
            &self.actor_uid,
        ];
        if self.epoch == 0
            || identifiers.iter().any(|id| {
                id.is_empty()
                    || id.len() > 128
                    || !id
                        .bytes()
                        .all(|c| c.is_ascii_alphanumeric() || b"._:-".contains(&c))
            })
        {
            return Err(ExecutionFeedbackError::InvalidBinding);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SandboxState {
    RunningFast,
    HeavyTool,
    Parked,
    Suspended,
    Resuming,
}

/// The trusted coordinator's version-1 execution signal. Durations are milliseconds.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SandboxStateUpdate {
    pub protocol_version: u32,
    #[serde(flatten)]
    pub binding: ExecutionBinding,
    pub sequence: u64,
    pub observed_at: DateTime<Utc>,
    pub ttl_ms: u64,
    pub state: SandboxState,
    pub estimated_return_ms: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct ExecutionFeedbackPolicy {
    pub parked_multiplier: f64,
    pub suspended_multiplier: f64,
    pub heavy_tool_multiplier: f64,
    pub heavy_tool_minimum_ms: u64,
    pub minimum_residence: Duration,
    pub recovery_hold: Duration,
}

impl Default for ExecutionFeedbackPolicy {
    fn default() -> Self {
        Self {
            parked_multiplier: 0.1,
            suspended_multiplier: 0.0,
            heavy_tool_multiplier: 0.25,
            heavy_tool_minimum_ms: 1_000,
            minimum_residence: Duration::from_millis(250),
            recovery_hold: Duration::from_secs(1),
        }
    }
}

impl ExecutionFeedbackPolicy {
    pub(crate) fn validate(&self) -> Result<(), ExecutionFeedbackError> {
        if [
            self.parked_multiplier,
            self.suspended_multiplier,
            self.heavy_tool_multiplier,
        ]
        .into_iter()
        .any(|n| !n.is_finite() || !(0.0..=1.0).contains(&n))
            || self.minimum_residence > Duration::from_secs(60)
            || self.recovery_hold > Duration::from_secs(60)
            || self.heavy_tool_minimum_ms > 600_000
        {
            return Err(ExecutionFeedbackError::InvalidUpdate);
        }
        Ok(())
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ExecutionFeedbackError {
    #[error("execution identity must contain valid identifiers and a positive epoch")]
    InvalidBinding,
    #[error("session has no active ThunderAgent program or trusted execution binding")]
    UnknownSession,
    #[error("binding replacement requires a strictly greater epoch")]
    StaleEpoch,
    #[error("signal identity or epoch differs from the registered binding")]
    IdentityMismatch,
    #[error("signal sequence and observation time must advance monotonically")]
    Replay,
    #[error("signal version, duration, sequence, or policy value is invalid")]
    InvalidUpdate,
    #[error("execution session still has a program or an active request")]
    SessionActive,
    #[error("signal observation exceeds allowed skew or has expired")]
    Expired,
}

#[derive(Debug, Clone)]
pub(crate) struct ExecutionFeedback {
    pub(crate) binding: ExecutionBinding,
    pub(crate) latest: Option<SandboxStateUpdate>,
    pub(crate) expires_at: Instant,
    pub(crate) admission_history: std::collections::VecDeque<serde_json::Value>,
    state_since: Instant,
    recovery_until: Instant,
}

impl ExecutionFeedback {
    pub(crate) fn new(binding: ExecutionBinding, now: Instant) -> Self {
        Self {
            binding,
            latest: None,
            expires_at: now,
            admission_history: std::collections::VecDeque::new(),
            state_since: now,
            recovery_until: now,
        }
    }

    pub(crate) fn accept(
        &mut self,
        update: SandboxStateUpdate,
        policy: ExecutionFeedbackPolicy,
        now: Instant,
        wall_now: DateTime<Utc>,
    ) -> Result<(), ExecutionFeedbackError> {
        if update.protocol_version != 1
            || update.sequence == 0
            || !(100..=60_000).contains(&update.ttl_ms)
            || update.estimated_return_ms > 600_000
        {
            return Err(ExecutionFeedbackError::InvalidUpdate);
        }
        if update.binding != self.binding {
            return Err(ExecutionFeedbackError::IdentityMismatch);
        }
        if let Some(prior) = &self.latest
            && (update.sequence <= prior.sequence || update.observed_at < prior.observed_at)
        {
            return Err(ExecutionFeedbackError::Replay);
        }
        let age = wall_now.signed_duration_since(update.observed_at);
        if age < chrono::Duration::seconds(-1)
            || age >= chrono::Duration::milliseconds(update.ttl_ms as i64)
        {
            return Err(ExecutionFeedbackError::Expired);
        }
        let expired = now >= self.expires_at;
        let changed = self
            .latest
            .as_ref()
            .is_none_or(|old| old.state != update.state);
        if expired || changed {
            self.state_since = now;
        }
        if update.state == SandboxState::Resuming && (expired || changed) {
            self.recovery_until = now + policy.recovery_hold;
        }
        let remaining = (chrono::Duration::milliseconds(update.ttl_ms as i64) - age)
            .to_std()
            .map_err(|_| ExecutionFeedbackError::Expired)?;
        self.expires_at = now + remaining;
        self.latest = Some(update);
        Ok(())
    }

    pub(crate) fn multiplier(&self, policy: ExecutionFeedbackPolicy, now: Instant) -> f64 {
        let Some(update) = &self.latest else {
            return 1.0;
        };
        if now >= self.expires_at
            || now < self.recovery_until
            || now.saturating_duration_since(self.state_since) < policy.minimum_residence
        {
            return 1.0;
        }
        match update.state {
            SandboxState::Suspended => policy.suspended_multiplier,
            SandboxState::Parked => policy.parked_multiplier,
            SandboxState::HeavyTool
                if update.estimated_return_ms >= policy.heavy_tool_minimum_ms =>
            {
                policy.heavy_tool_multiplier
            }
            _ => 1.0,
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub(crate) fn signal(state: SandboxState) -> SandboxStateUpdate {
        SandboxStateUpdate {
            protocol_version: 1,
            binding: ExecutionBinding {
                source_id: "coordinator".into(),
                tenant_id: "tenant".into(),
                workspace_id: "workspace".into(),
                session_id: "session-a".into(),
                actor_uid: "actor-a".into(),
                epoch: 1,
            },
            sequence: 1,
            observed_at: Utc::now(),
            ttl_ms: 5_000,
            state,
            estimated_return_ms: 2_000,
        }
    }

    #[test]
    fn wire_contract_uses_versioned_flat_identity_and_rfc3339_time() {
        let input = signal(SandboxState::HeavyTool);
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["protocol_version"], 1);
        assert_eq!(json["session_id"], "session-a");
        assert_eq!(json["state"], "HEAVY_TOOL");
        assert!(json["observed_at"].as_str().unwrap().contains('T'));
        let output: SandboxStateUpdate = serde_json::from_value(json).unwrap();
        assert_eq!(output.binding, input.binding);
    }

    #[test]
    fn future_observation_within_skew_keeps_observed_at_plus_ttl_deadline() {
        let now = Instant::now();
        let mut update = signal(SandboxState::Suspended);
        let wall = update.observed_at;
        update.observed_at += chrono::Duration::milliseconds(500);
        let policy = ExecutionFeedbackPolicy {
            minimum_residence: Duration::ZERO,
            ..Default::default()
        };
        let mut feedback = ExecutionFeedback::new(update.binding.clone(), now);
        feedback.accept(update, policy, now, wall).unwrap();
        assert_eq!(
            feedback.multiplier(policy, now + Duration::from_millis(5_499)),
            0.0
        );
        assert_eq!(
            feedback.multiplier(policy, now + Duration::from_millis(5_500)),
            1.0
        );
    }

    #[test]
    fn rejects_replay_identity_expiry_and_invalid_values() {
        let now = Instant::now();
        let update = signal(SandboxState::Suspended);
        let wall = update.observed_at;
        let mut feedback = ExecutionFeedback::new(update.binding.clone(), now);
        let policy = ExecutionFeedbackPolicy::default();
        feedback.accept(update.clone(), policy, now, wall).unwrap();
        assert_eq!(
            feedback.accept(update.clone(), policy, now, wall),
            Err(ExecutionFeedbackError::Replay)
        );
        let mut bad = update.clone();
        bad.sequence = 2;
        bad.binding.epoch = 2;
        assert_eq!(
            feedback.accept(bad, policy, now, wall),
            Err(ExecutionFeedbackError::IdentityMismatch)
        );
        let mut bad = update.clone();
        bad.sequence = 2;
        bad.observed_at = wall - chrono::Duration::seconds(10);
        assert_eq!(
            feedback.accept(bad, policy, now, wall),
            Err(ExecutionFeedbackError::Replay)
        );
        let mut bad = update.clone();
        bad.sequence = 2;
        bad.protocol_version = 2;
        assert_eq!(
            feedback.accept(bad, policy, now, wall),
            Err(ExecutionFeedbackError::InvalidUpdate)
        );
        let mut fresh = ExecutionFeedback::new(update.binding.clone(), now);
        assert_eq!(
            fresh.accept(
                update.clone(),
                policy,
                now,
                wall + chrono::Duration::seconds(6)
            ),
            Err(ExecutionFeedbackError::Expired)
        );
        assert_eq!(
            fresh.accept(update, policy, now, wall - chrono::Duration::seconds(2)),
            Err(ExecutionFeedbackError::Expired)
        );
    }

    #[test]
    fn defaults_debounce_recovery_hold_and_ttl_preserve_replay_watermark() {
        let now = Instant::now();
        let mut update = signal(SandboxState::Suspended);
        let wall = update.observed_at;
        let policy = ExecutionFeedbackPolicy::default();
        let mut feedback = ExecutionFeedback::new(update.binding.clone(), now);
        feedback.accept(update.clone(), policy, now, wall).unwrap();
        assert_eq!(feedback.multiplier(policy, now), 1.0);
        assert_eq!(
            feedback.multiplier(policy, now + Duration::from_millis(250)),
            0.0
        );
        assert_eq!(
            feedback.multiplier(policy, now + Duration::from_secs(5)),
            1.0
        );
        assert_eq!(
            feedback.accept(
                update.clone(),
                policy,
                now + Duration::from_secs(5),
                wall + chrono::Duration::seconds(5)
            ),
            Err(ExecutionFeedbackError::Replay)
        );
        update.sequence = 2;
        update.state = SandboxState::Resuming;
        update.observed_at = wall + chrono::Duration::seconds(5);
        feedback
            .accept(
                update.clone(),
                policy,
                now + Duration::from_secs(5),
                update.observed_at,
            )
            .unwrap();
        update.sequence = 3;
        update.state = SandboxState::Suspended;
        feedback
            .accept(
                update.clone(),
                policy,
                now + Duration::from_secs(5),
                update.observed_at,
            )
            .unwrap();
        assert_eq!(
            feedback.multiplier(policy, now + Duration::from_millis(5_500)),
            1.0
        );
        assert_eq!(
            feedback.multiplier(policy, now + Duration::from_secs(6)),
            0.0
        );
    }
}
