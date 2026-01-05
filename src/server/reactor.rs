use std::{sync::Arc, thread};

use polling::{Events, Poller};

use super::Handler;

type Error = Box<dyn std::error::Error>;
type Result<T> = std::result::Result<T, Error>;

pub fn spawn(poller: Arc<Poller>, events: Events, handler: Handler) {
    thread::spawn(move || {
        run_event_loop(poller, events, handler).expect("fatal error in network reactor")
    });
}

fn run_event_loop(poller: Arc<Poller>, mut events: Events, mut handler: Handler) -> Result<()> {
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
