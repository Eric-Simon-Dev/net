use std::{io, sync::Arc, thread};

use polling::{Events, Poller};
use thiserror::Error;

use super::handler::{self, Handler};

pub fn spawn(poller: Arc<Poller>, handler: Handler) -> io::Result<()> {
    thread::Builder::new()
        .name("client-side network reactor".to_string())
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
        // - Caller wake (outgoing messages or commands may be available).
        // - Timeout (timers may have expired).
        //
        // Can also *spuriously* wake.

        socket_events.clear();
        poller.wait(&mut socket_events, handler.next_timeout())?;

        // ---- Handle ----

        handler.handle_timers()?;
        handler.handle_outgoings(&poller)?;
        handler.handle_socket_events(&poller, &socket_events)?;
    }
    handler.destroy(&poller)?;
    Ok(())
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ReactorError {
    // ---- Handler ----
    #[error("failed to handle socket events: {0}")]
    HandleSocketEvents(#[from] handler::HandleSocketEventsError),

    #[error("failed to handle outgoings: {0}")]
    HandleOutgoings(#[from] handler::HandleOutgoingsError),

    #[error("failed to handle timers: {0}")]
    HandleTimers(#[from] handler::HandleTimersError),

    #[error("failed to destroy handler: {0}")]
    DestroyHandler(#[from] handler::DestroyError),

    // ---- Poller ----
    #[error("failed to wait on poller: {0}")]
    WaitOnPoller(#[from] io::Error),
}
