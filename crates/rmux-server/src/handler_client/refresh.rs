use std::collections::HashSet;

use rmux_core::{input::OscColourSlot, LifecycleEvent};
use rmux_proto::request::RefreshClientRequest;
use rmux_proto::{
    ErrorResponse, PaneId, RefreshClientResponse, Response, RmuxError, SessionId, SessionName,
    TerminalSize, WindowTarget,
};

use crate::handler_support::attached_client_required;
use crate::pane_io::AttachControl;
use crate::pane_terminals::HandlerState;

use super::super::{
    client_runtime_support::clipboard_query_sequence,
    control_support::{ControlClientIdentity, ManagedClient},
    QueuedLifecycleEvent, RequestHandler,
};

impl RequestHandler {
    pub(in crate::handler) async fn handle_refresh_client(
        &self,
        requester_pid: u32,
        request: RefreshClientRequest,
    ) -> Response {
        if let Err(error) = validate_refresh_supported_request(&request) {
            return Response::Error(ErrorResponse { error });
        }

        let client = match self
            .resolve_target_managed_client(
                requester_pid,
                request.target_client.as_deref(),
                "refresh-client",
            )
            .await
        {
            Ok(client) => client,
            Err(error) => return Response::Error(ErrorResponse { error }),
        };

        // Any client may report, but only an attached control client gets the
        // colours answered, as in tmux.
        if let Some((pane_id, slot, rgb)) = request
            .colour_report
            .as_deref()
            .and_then(parse_colour_report)
        {
            let mut state = self.state.lock().await;
            let answered = !self.active_control.lock().await.by_pid.is_empty();
            state.report_pane_colour(pane_id, slot, rgb, answered);
        }

        match client {
            ManagedClient::Attach {
                pid: attach_pid,
                attach_id,
            } => {
                self.handle_refresh_attached_client(attach_pid, attach_id, request)
                    .await
            }
            ManagedClient::Control(identity) => {
                self.handle_refresh_control_client(identity, request).await
            }
        }
    }

    async fn handle_refresh_attached_client(
        &self,
        attach_pid: u32,
        expected_attach_id: u64,
        request: RefreshClientRequest,
    ) -> Response {
        let mut needs_full_refresh = !request.status_only;
        let clipboard_query = request.clipboard_query;
        let (session_name, session_id, size_eligibility_changed) = {
            let mut active_attach = self.active_attach.lock().await;
            let Some(active) = active_attach.by_pid.get_mut(&attach_pid).filter(|active| {
                active.id == expected_attach_id
                    && !active.closing.load(std::sync::atomic::Ordering::SeqCst)
            }) else {
                return Response::Error(ErrorResponse {
                    error: attached_client_required("refresh-client"),
                });
            };

            let ignored_size_before = active.flags.contains(super::ClientFlags::IGNORESIZE);
            let raw_flag = request.flags.as_deref().or(request.flags_alias.as_deref());
            if let Some(raw) = raw_flag {
                let mut merged_flags = active.flags;
                for token in raw.split(',').filter(|t| !t.is_empty()) {
                    if let Err(error) = merged_flags.apply_named(token) {
                        return Response::Error(ErrorResponse { error });
                    }
                }
                if !active.can_write {
                    merged_flags = merged_flags.with_read_only();
                }
                active.flags = merged_flags;
            }
            let size_eligibility_changed =
                ignored_size_before != active.flags.contains(super::ClientFlags::IGNORESIZE);
            if size_eligibility_changed {
                self.bump_active_attach_epoch();
            }

            (
                active.session_name.clone(),
                active.session_id,
                size_eligibility_changed,
            )
        };

        if size_eligibility_changed {
            if let Err(error) = self
                .reconcile_refresh_size_eligibility_transition(
                    attach_pid,
                    expected_attach_id,
                    session_id,
                )
                .await
            {
                return Response::Error(ErrorResponse { error });
            }
        }

        if request.status_only {
            if let Err(error) = self
                .refresh_attached_client_status_for_identity(
                    attach_pid,
                    expected_attach_id,
                    &session_name,
                )
                .await
            {
                return Response::Error(ErrorResponse { error });
            }
            needs_full_refresh = false;
        }
        if clipboard_query {
            if let Err(error) = self
                .send_attach_control_for_client_identity(
                    attach_pid,
                    expected_attach_id,
                    AttachControl::Write(clipboard_query_sequence()),
                    "refresh-client",
                )
                .await
            {
                return Response::Error(ErrorResponse { error });
            }
        }
        if needs_full_refresh {
            if let Err(error) = self
                .refresh_attached_client_for_identity(
                    attach_pid,
                    expected_attach_id,
                    &session_name,
                    "refresh-client",
                )
                .await
            {
                return Response::Error(ErrorResponse { error });
            }
        }

        Response::RefreshClient(RefreshClientResponse {
            target_client: attach_pid.to_string(),
        })
    }

    async fn reconcile_refresh_size_eligibility_transition(
        &self,
        attach_pid: u32,
        expected_attach_id: u64,
        expected_session_id: SessionId,
    ) -> Result<(), RmuxError> {
        let identity_is_current = {
            let active_attach = self.active_attach.lock().await;
            active_attach.by_pid.get(&attach_pid).is_some_and(|active| {
                active.id == expected_attach_id
                    && active.session_id == expected_session_id
                    && !active.closing.load(std::sync::atomic::Ordering::SeqCst)
            })
        };
        if !identity_is_current {
            return Err(attached_client_required("refresh-client"));
        }
        self.reconcile_attached_session_identity_size_and_emit(expected_session_id)
            .await
    }

    async fn handle_refresh_control_client(
        &self,
        identity: ControlClientIdentity,
        request: RefreshClientRequest,
    ) -> Response {
        let control_pid = identity.requester_pid();
        if request.has_attach_only_effects() {
            return Response::Error(ErrorResponse {
                error: attached_client_required("refresh-client"),
            });
        }

        let control_size = match request.control_size.as_deref() {
            Some(value) => match parse_control_size(value) {
                Some(size) => Some(size),
                None => {
                    return Response::Error(ErrorResponse {
                        error: RmuxError::Server(format!("invalid refresh-client size '{value}'")),
                    });
                }
            },
            None => None,
        };

        let (session_name, session_id) = {
            let active_control = self.active_control.lock().await;
            let Some(active) = active_control.by_pid.get(&control_pid).filter(|active| {
                active.id == identity.control_id()
                    && !active.closing.load(std::sync::atomic::Ordering::SeqCst)
            }) else {
                return Response::Error(ErrorResponse {
                    error: attached_client_required("refresh-client"),
                });
            };
            (active.session_name.clone(), active.session_id)
        };

        if let (Some(session_name), Some(session_id), Some(size)) =
            (session_name.as_ref(), session_id, control_size)
        {
            #[cfg(windows)]
            self.wait_for_windows_deferred_all_pane_pids().await;
            {
                let mut state = self.state.lock().await;
                let active_attach = self.active_attach.lock().await;
                let mut active_control = self.active_control.lock().await;
                let Some(active) = active_control.by_pid.get(&control_pid).filter(|active| {
                    active.id == identity.control_id()
                        && !active.closing.load(std::sync::atomic::Ordering::SeqCst)
                        && active.session_name.as_ref() == Some(session_name)
                        && active.session_id == Some(session_id)
                }) else {
                    return Response::Error(ErrorResponse {
                        error: attached_client_required("refresh-client"),
                    });
                };
                let size_sequence = active.size_sequence;
                match super::super::attach_support::resize_control_session_for_client(
                    &mut state,
                    super::super::attach_support::ControlResizeClient::new(
                        &active_attach,
                        &active_control,
                        control_pid,
                        Some(size),
                        size_sequence,
                    ),
                    session_name,
                    session_id,
                ) {
                    Ok(()) => {
                        let active = active_control
                            .by_pid
                            .get_mut(&control_pid)
                            .expect("validated control client remains locked");
                        active.client_width = size.cols;
                        active.client_height = size.rows;
                        active.size_declared = true;
                    }
                    Err(error) => return Response::Error(ErrorResponse { error }),
                }
            }
            self.publish_control_size_layout_echoes(session_name, session_id)
                .await;
        } else if let (None, None, Some(size)) = (session_name.as_ref(), session_id, control_size) {
            let mut active_control = self.active_control.lock().await;
            let Some(active) = active_control
                .by_pid
                .get_mut(&control_pid)
                .filter(|active| {
                    active.id == identity.control_id()
                        && !active.closing.load(std::sync::atomic::Ordering::SeqCst)
                        && active.session_name.is_none()
                        && active.session_id.is_none()
                })
            else {
                return Response::Error(ErrorResponse {
                    error: attached_client_required("refresh-client"),
                });
            };
            active.client_width = size.cols;
            active.client_height = size.rows;
            active.size_declared = true;
        } else if control_size.is_none() {
            if let Err(error) = self.refresh_control_client_for_identity(identity).await {
                return Response::Error(ErrorResponse { error });
            }
        } else {
            return Response::Error(ErrorResponse {
                error: attached_client_required("refresh-client"),
            });
        }

        Response::RefreshClient(RefreshClientResponse {
            target_client: control_pid.to_string(),
        })
    }

    /// Publishes the layout echo forced by `refresh-client -C`.
    ///
    /// Frozen tmux 3.7b, measured 2026-07-26, emits one `%layout-change` for
    /// every window in index order on the first declaration, an idempotent
    /// declaration, a changed declaration and its idempotent repeat. It does
    /// the same under `window-size manual` without applying the client's size.
    ///
    /// A real resize is already recorded by the geometry chokepoint. Preparing
    /// that resize and the forced echoes together preserves its
    /// `window-resized` event without duplicating its layout notification.
    async fn publish_control_size_layout_echoes(
        &self,
        session_name: &SessionName,
        session_id: SessionId,
    ) {
        let prepared = {
            let mut state = self.state.lock().await;
            prepare_control_size_layout_echo_events(&mut state, session_name, session_id)
        };
        for event in prepared {
            self.emit_prepared(event).await;
        }
    }
}

fn prepare_control_size_layout_echo_events(
    state: &mut HandlerState,
    session_name: &SessionName,
    session_id: SessionId,
) -> Vec<QueuedLifecycleEvent> {
    let Some(session) = state
        .sessions
        .session(session_name)
        .filter(|session| session.id() == session_id)
    else {
        let applied_resizes = state.take_applied_window_resizes();
        let mut prepared = Vec::with_capacity(applied_resizes.len().saturating_mul(2));
        for resize in applied_resizes {
            let (target, layout_change_prepared) = resize.into_parts();
            prepare_applied_window_resize_pair(
                state,
                &target,
                layout_change_prepared,
                &mut prepared,
            );
        }
        return prepared;
    };
    let layout_targets = session
        .windows()
        .keys()
        .map(|window_index| WindowTarget::with_window(session_name.clone(), *window_index))
        .collect::<Vec<_>>();
    let applied_resizes = state
        .take_applied_window_resizes()
        .into_iter()
        .map(crate::pane_terminals::AppliedWindowResize::into_parts)
        .collect::<Vec<_>>();
    let layout_target_set = layout_targets.iter().cloned().collect::<HashSet<_>>();
    let applied_resize_set = applied_resizes
        .iter()
        .map(|(target, _)| target.clone())
        .collect::<HashSet<_>>();
    let insert_at = applied_resizes
        .iter()
        .position(|(target, _)| layout_target_set.contains(target))
        .unwrap_or(applied_resizes.len());
    let mut prepared = Vec::with_capacity(
        layout_targets
            .len()
            .saturating_add(applied_resizes.len().saturating_mul(2)),
    );

    for (index, (target, layout_change_prepared)) in applied_resizes.iter().enumerate() {
        if index == insert_at {
            prepare_control_size_layout_batch(
                state,
                &layout_targets,
                &applied_resize_set,
                &mut prepared,
            );
        }
        if !layout_target_set.contains(target) {
            prepare_applied_window_resize_pair(
                state,
                target,
                *layout_change_prepared,
                &mut prepared,
            );
        }
    }
    if insert_at == applied_resizes.len() {
        prepare_control_size_layout_batch(
            state,
            &layout_targets,
            &applied_resize_set,
            &mut prepared,
        );
    }
    prepared
}

fn prepare_control_size_layout_batch(
    state: &mut HandlerState,
    layout_targets: &[WindowTarget],
    applied_resizes: &HashSet<WindowTarget>,
    prepared: &mut Vec<QueuedLifecycleEvent>,
) {
    for target in layout_targets {
        prepared.push(super::super::prepare_lifecycle_event(
            state,
            &LifecycleEvent::WindowLayoutChanged {
                target: target.clone(),
            },
        ));
        if applied_resizes.contains(target) {
            prepared.push(super::super::prepare_lifecycle_event(
                state,
                &LifecycleEvent::WindowResized {
                    target: target.clone(),
                },
            ));
        }
    }
}

fn prepare_applied_window_resize_pair(
    state: &mut HandlerState,
    target: &WindowTarget,
    layout_change_prepared: bool,
    prepared: &mut Vec<QueuedLifecycleEvent>,
) {
    if !layout_change_prepared {
        prepared.push(super::super::prepare_lifecycle_event(
            state,
            &LifecycleEvent::WindowLayoutChanged {
                target: target.clone(),
            },
        ));
    }
    prepared.push(super::super::prepare_lifecycle_event(
        state,
        &LifecycleEvent::WindowResized {
            target: target.clone(),
        },
    ));
}

trait RefreshClientControlScope {
    fn has_attach_only_effects(&self) -> bool;
}

impl RefreshClientControlScope for RefreshClientRequest {
    fn has_attach_only_effects(&self) -> bool {
        self.status_only
            || self.clipboard_query
            || self.flags.is_some()
            || self.flags_alias.is_some()
    }
}

fn validate_refresh_supported_request(request: &RefreshClientRequest) -> Result<(), RmuxError> {
    let mut unsupported = Vec::new();
    if request.clear_pan {
        unsupported.push("-c");
    }
    if request.pan_down {
        unsupported.push("-D");
    }
    if request.pan_left {
        unsupported.push("-L");
    }
    if request.pan_right {
        unsupported.push("-R");
    }
    if request.pan_up {
        unsupported.push("-U");
    }
    if request.adjustment.is_some() {
        unsupported.push("adjustment");
    }
    if !request.subscriptions.is_empty() {
        unsupported.push("-A");
    }
    if !request.subscriptions_format.is_empty() {
        unsupported.push("-B");
    }
    if unsupported.is_empty() {
        return Ok(());
    }
    Err(RmuxError::Server(format!(
        "refresh-client {} is not supported",
        unsupported.join("/")
    )))
}

/// A `refresh-client -r` report: `%pane:` and the outer terminal's answer to
/// an OSC 10 or 11 query, ended by BEL or ST. tmux ignores a report it cannot
/// read, and so does this.
fn parse_colour_report(value: &str) -> Option<(PaneId, OscColourSlot, (u8, u8, u8))> {
    let (pane, report) = value.strip_prefix('%')?.split_once(':')?;
    let pane = PaneId::new(pane.parse().ok()?);
    let (slot, answer) = match report.strip_prefix("\x1b]10;") {
        Some(answer) => (OscColourSlot::Foreground, answer),
        None => (OscColourSlot::Background, report.strip_prefix("\x1b]11;")?),
    };
    let spec = answer
        .strip_suffix('\x07')
        .or_else(|| answer.strip_suffix("\x1b\\"))?;
    Some((pane, slot, x11_rgb(spec)?))
}

/// An X11 colour in the `rgb:` and `#` forms tmux's `colour_parseX11` reads,
/// two or four hex digits a channel, of which four keep the high byte. Colour
/// names and CMYK are not read.
fn x11_rgb(spec: &str) -> Option<(u8, u8, u8)> {
    let channels: Vec<&str> = match spec.strip_prefix("rgb:") {
        Some(rgb) => rgb.split('/').collect(),
        None => {
            let hex = spec.strip_prefix('#')?;
            let digits = hex.len() / 3;
            hex.as_bytes()
                .chunks(digits.max(1))
                .map(|chunk| std::str::from_utf8(chunk).ok())
                .collect::<Option<_>>()?
        }
    };
    let [red, green, blue] = channels[..] else {
        return None;
    };
    let digits = red.len();
    if !matches!(digits, 2 | 4) {
        return None;
    }
    let channel = |channel: &str| {
        (channel.len() == digits && channel.bytes().all(|byte| byte.is_ascii_hexdigit()))
            .then(|| u16::from_str_radix(channel, 16).ok())
            .flatten()
            .map(|value| (value >> (4 * (digits - 2))) as u8)
    };
    Some((channel(red)?, channel(green)?, channel(blue)?))
}

fn parse_control_size(value: &str) -> Option<TerminalSize> {
    let (cols, rows) = value.split_once('x').or_else(|| value.split_once(','))?;
    let cols = cols.parse::<u16>().ok()?;
    let rows = rows.parse::<u16>().ok()?;
    (cols > 0 && rows > 0).then_some(TerminalSize { cols, rows })
}

#[cfg(test)]
mod tests {
    use super::{parse_colour_report, parse_control_size};
    use rmux_core::input::OscColourSlot;
    use rmux_proto::{PaneId, TerminalSize};

    #[test]
    fn colour_report_reads_osc_10_and_11_answers_like_tmux() {
        assert_eq!(
            parse_colour_report("%3:\x1b]11;rgb:1e1e/1e1e/2e2e\x1b\\"),
            Some((
                PaneId::new(3),
                OscColourSlot::Background,
                (0x1e, 0x1e, 0x2e)
            ))
        );
        assert_eq!(
            parse_colour_report("%0:\x1b]10;#ff8000\x07"),
            Some((PaneId::new(0), OscColourSlot::Foreground, (255, 128, 0)))
        );
    }

    #[test]
    fn colour_report_keeps_the_high_byte_of_four_digits_like_tmux() {
        for spec in ["rgb:80ff/80ff/80ff", "#80ff80ff80ff"] {
            assert_eq!(
                parse_colour_report(&format!("%0:\x1b]11;{spec}\x07")),
                Some((PaneId::new(0), OscColourSlot::Background, (128, 128, 128))),
                "{spec}"
            );
        }
    }

    #[test]
    fn colour_report_ignores_colour_names_unlike_tmux() {
        assert_eq!(parse_colour_report("%3:\x1b]11;white\x07"), None);
    }

    #[test]
    fn colour_report_ignores_what_tmux_cannot_read() {
        for value in [
            "3:\x1b]11;rgb:0/0/0\x07",
            "%x:\x1b]11;rgb:0/0/0\x07",
            "%3:\x1b]12;rgb:0/0/0\x07",
            "%3:\x1b]11;rgb:0/0/0",
            "%3:\x1b]11;rgb:0/0\x07",
            "%3:\x1b]11;rgb:00000/0/0\x07",
            "%3:\x1b]11;rgb:+1/0/0\x07",
            "%3:\x1b]11;rgb:f/80/0\x07",
            "%3:\x1b]11;rgb:ff/ff/ffff\x07",
            "%3:\x1b]11;#fff\x07",
        ] {
            assert_eq!(parse_colour_report(value), None, "{value:?}");
        }
    }

    #[test]
    fn control_size_accepts_tmux_separators() {
        let expected = Some(TerminalSize {
            cols: 101,
            rows: 31,
        });
        assert_eq!(parse_control_size("101x31"), expected);
        assert_eq!(parse_control_size("101,31"), expected);
    }

    #[test]
    fn control_size_rejects_zero_or_malformed_dimensions() {
        for value in ["", "101", "0x31", "101,0", "101x31,20", "101,31x20"] {
            assert_eq!(parse_control_size(value), None, "{value}");
        }
    }
}
