use std::{io, sync::Arc, thread};

use polling::{Events, Poller};
use thiserror::Error;

use super::{HandleEventsError, HandleOutgoingMessagesError, HandleTimersError, Handler};

pub fn start(poller: Arc<Poller>, handler: Handler) {
    thread::spawn(move || {
        if let Err(e) = run_event_loop(poller, handler) {
            eprintln!("reactor shutdown: {e}");
        }
    });
}

fn run_event_loop(poller: Arc<Poller>, mut handler: Handler) -> Result<(), ReactorError> {
    let mut events = Events::new();
    loop {
        // ---- Wait ----

        // Wait for either:
        // - Poller events (sockets may be readable/writable).
        // - Caller wake (messages may be available in outgoing).
        // - Timeout (timers may have expired).
        //
        // Can also *spuriously* wake.

        events.clear();
        poller.wait(&mut events, handler.next_timeout())?;

        // ---- Handle ----

        handler.handle_expired_timers()?;
        handler.handle_available_outgoing_messages(&poller)?;
        handler.handle_events(&poller, &events)?;
    }
}

// ---- Errors ----

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ReactorError {
    #[error("failed to handle events: {0}")]
    Events(#[from] HandleEventsError),

    #[error("failed to handle outgoing messages: {0}")]
    OutgoingMessages(#[from] HandleOutgoingMessagesError),

    #[error("failed to handle timers: {0}")]
    Timers(#[from] HandleTimersError),

    #[error("failed to wait: {0}")]
    Wait(#[from] io::Error),
}
