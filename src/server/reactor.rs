use std::{io, sync::Arc, thread};

use polling::{Events, Poller};
use thiserror::Error;

use super::{HandleEventError, HandleOutgoingMessagesError, HandleTimersError, Handler};

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

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ReactorError {
    #[error("event handling failed: {0}")]
    EventHandling(#[from] HandleEventError),

    #[error("outgoing messages handling failed: {0}")]
    OutgoingMessagesHandling(#[from] HandleOutgoingMessagesError),

    #[error("timers handling failed: {0}")]
    TimersHandling(#[from] HandleTimersError),

    #[error("I/O error while waiting poller: {0}")]
    Io(#[from] io::Error),
}
