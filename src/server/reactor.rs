use std::{fmt, sync::Arc, thread};

use polling::{Events, Poller};

use super::{HandleOutgoingMessagesError, HandleSocketEventError, HandleTimersError, Handler};

pub fn spawn(poller: Arc<Poller>, events: Events, handler: Handler) {
    thread::spawn(move || {
        run_event_loop(poller, events, handler).expect("fatal error in network reactor")
    });
}

fn run_event_loop(
    poller: Arc<Poller>,
    mut events: Events,
    mut handler: Handler,
) -> Result<(), ReactorError> {
    loop {
        // ---- Wait ----

        // Wait for either :
        // - Poller event (sockets might be ready).
        // - Caller wake (outgoing messages might be ready).
        // - Timeout (timers might be ready).
        //
        // Can also *spuriously* wake.

        poller.wait(&mut events, handler.next_timeout())?;

        // ---- Handle ----

        // - Handle any timed-out timers.
        // - Handle any pending outgoing messages.
        // - Handle socket events reported by poller.

        handler.check_timers()?;

        handler.check_outgoing_messages(&poller)?;

        for event in events.iter() {
            handler.handle_socket_event(&poller, event)?;
        }
        events.clear();
    }
}

// ---- Errors ----

#[derive(Debug)]
#[non_exhaustive]
pub enum ReactorError {
    HandleSocketEvent(HandleSocketEventError),
    HandleOutgoingMessages(HandleOutgoingMessagesError),
    HandleTimers(HandleTimersError),
    Io(std::io::Error),
}

impl From<std::io::Error> for ReactorError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<HandleSocketEventError> for ReactorError {
    fn from(e: HandleSocketEventError) -> Self {
        Self::HandleSocketEvent(e)
    }
}

impl From<HandleOutgoingMessagesError> for ReactorError {
    fn from(e: HandleOutgoingMessagesError) -> Self {
        Self::HandleOutgoingMessages(e)
    }
}

impl From<HandleTimersError> for ReactorError {
    fn from(e: HandleTimersError) -> Self {
        Self::HandleTimers(e)
    }
}

impl fmt::Display for ReactorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "i/o error while waiting poller: {e}"),
            Self::HandleSocketEvent(e) => write!(f, "error while handling socket event: {e}"),
            Self::HandleOutgoingMessages(e) => {
                write!(f, "error while handling outgoing messages: {e}")
            }
            Self::HandleTimers(e) => write!(f, "error while handling timers: {e}"),
        }
    }
}

impl std::error::Error for ReactorError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            Self::HandleSocketEvent(e) => Some(e),
            Self::HandleOutgoingMessages(e) => Some(e),
            Self::HandleTimers(e) => Some(e),
        }
    }
}
