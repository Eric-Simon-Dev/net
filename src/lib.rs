mod doc {
    //! # TCP / UDP / RUDP
    //!
    //! Both TCP and UDP are standard protocols built on top of IP.
    //!
    //! TCP guarantees:
    //! - **Integrity**: No corrupted payload (resend if needed).  
    //! - **Non-duplication**: Single message sent ⇒ single message received (using sequence numbers).  
    //! - **Delivery**: Notifies sender when delivery occurs (via acknowledgments).  
    //! - **Order**: Messages arrive in the order they were sent (using sequence numbers).  
    //!
    //! UDP guarantees:
    //! - **Integrity**: Ensures payload is not corrupted.  
    //! - **Boundaries**: Packets are not split or merged (unlike TCP).  
    //!
    //! In scenarios like gaming, we may want more flexibility. For example:  
    //! - Non-duplication or delivery without order guarantee.
    //! - Acknowledgments with custom resend rules (e.g., prioritizing certain packets).
    //!
    //! RUDP is a protocol implemented on top of UDP.
    //! Most RUDP protocols implement some degree of reliability,
    //! though the term can broadly refer to any protocol built on UDP.  
    //!
    //! # IPv4 & IPv6
    //!
    //! Ideally, we want to support both IPv4 and IPv6 to reach the maximum player base.
    //!
    //! Options:
    //! - **Dual-stack socket**: Can handle both versions, but not always available.  
    //! - **Separate sockets per version**: The most common approach.  
    //!
    //! Currently, we use a single socket,
    //! so the server/client IP versions must match (both v4 or both v6).  
    //!
    //! # Session & Transport
    //!
    //! These represent different layers with different responsibilities:
    //! - **Transport**: Guarantees (e.g., RUDP, ENet, etc.).  
    //! - **Session**: Continuity (authentication, reconnection, etc.).  
    //!
    //! I'm unsure about the definitions;
    //! this crate may later be split into separate "transport" and "session" modules.  
    //!
    //! # Channels
    //!
    //! Network <--receiver/sender--> Packet <--handler--> Message (App)
    //!
    //! Naming conventions:
    //! - **Message** := Payload + application metadata.  
    //! - **Packet** := Payload + protocol metadata (unseen by the user).  
    //! - **Incoming** := From network to app.  
    //! - **Outgoing** := From app to network.  
    //!
    //! # Threads
    //!
    //! Threads loop continuously on blocking functions. Blocking allows context switching.
    //!
    //! In case of error:
    //! - A thread fails and shuts down.  
    //! - Chain reaction of channel disconnections leads to other thread shutdowns.  
    //! - Finally, the user receives `Err(Disconnected)`.  
    //!
    //! # Buffering strategy
    //!
    //! 1. Reserve RING capacity (multiple contiguous buffers), e.g., 4Mb total (each buffer 1Mb).  
    //! 2. "Eat" (give ownership away) a buffer from the front when buffering packets.  
    //! 3. Once we reach the end of the ring:  
    //!    - Reallocate BUFFER capacity.  
    //!        - Should reallocate at the beginning of the ring (`bytes` crate optimization).  
    //!        - Avoid OS reallocation.  
    //!
    //! Only the BUFFER (1Mb) is reallocated
    //! because the middle/end of the ring may be in use by other threads.  
    //! If the app holds onto packets for too long, this strategy may fail.
    //! The next buffer region will not be free.
    //! Allocations still must do a full "ring tour" before that though.
    //!
    //! This optimized behavior hasn’t been tested.
    //! To test it, monitor whether buffer pointers always stay within the ring range.
}

pub mod client;
pub mod server;

mod protocol;

#[cfg(test)]
mod tests;
