use bytes::BytesMut;

#[test]
fn server_client_connection() {
    use crate::{client::Client, server::Server};

    let server = Server::new(12012).unwrap();
    let mut client = Client::new(12013).unwrap();
    client.connect("0:12012").unwrap();

    //------// Client -> Server //------//

    // send
    let mut msg = BytesMut::zeroed(256);
    msg.fill(100);
    let msg = msg.freeze();
    client.outgoing.unwrap().send(msg).unwrap();

    // recv
    let (msg, client_index) = server.message_incoming.recv().unwrap();
    assert_eq!(msg[..4], [100, 100, 100, 100]);

    //------// Server -> Client //------//

    // send
    let mut msg = BytesMut::zeroed(256);
    msg.fill(200);
    let msg = msg.freeze();
    server.message_outgoing.send((msg, client_index)).unwrap();

    // recv
    let msg = client.incoming.unwrap().recv().unwrap();
    assert_eq!(msg[..4], [200, 200, 200, 200]);
}
