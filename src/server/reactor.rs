use std::{io, sync::Arc, thread};

use polling::{Events, Poller};
use thiserror::Error;

use super::handler::{self, Handler};

pub fn spawn(poller: Arc<Poller>, handler: Handler) -> io::Result<()> {
    thread::Builder::new()
        .name("network reactor".to_string())
        .spawn(move || match run(poller, handler) {
            Ok(()) => println!("reactor shut down"),
            Err(e) => eprintln!("reactor crashed: {e}"),
        })?;
    Ok(())
}

fn run(poller: Arc<Poller>, mut handler: Handler) -> Result<(), ReactorError> {
    let mut socket_events = Events::new();
    while !handler.shutdown {
        // ---- Wait ----

        // Wait for either:
        // - Socket events (sockets may be readable/writable).
        // - Caller wake (outgoings may be available).
        // - Timeout (timers may have expired).
        //
        // Can also *spuriously* wake.

        socket_events.clear();
        poller
            .wait(&mut socket_events, handler.next_timeout())
            .map_err(ReactorError::WaitOnPoller)?;

        // ---- Handle ----

        handler.handle_timers()?;
        handler.handle_outgoing(&poller)?;
        handler.handle_socket_events(&poller, &socket_events)?;
    }
    handler
        .destroy(&poller)
        .map_err(ReactorError::DestroyHandler)?;
    Ok(())
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ReactorError {
    // ---- Handler ----
    #[error("failed to handle socket events: {0}")]
    HandleSocketEvents(#[from] handler::HandleSocketEventsError),

    #[error("failed to handle outgoings: {0}")]
    HandleOutgoings(#[from] handler::HandleOutgoingError),

    #[error("failed to handle timers: {0}")]
    HandleTimers(#[from] handler::HandleTimersError),

    #[error("failed to destroy handler: {0}")]
    DestroyHandler(io::Error),

    // ---- Poller ----
    #[error("failed to wait on poller: {0}")]
    WaitOnPoller(io::Error),
}
