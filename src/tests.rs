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
    let msg = server.incoming.recv().unwrap();
    assert_eq!(msg[..8], [127, 0, 0, 1, 237, 46, 100, 100]);

    //------// Server -> Client //------//

    // send
    let mut msg = BytesMut::zeroed(256);
    msg[..6].copy_from_slice(&[127, 0, 0, 1, 237, 46]);
    msg[6..256].fill(200);
    let msg = msg.freeze();
    server.outgoing.send(msg).unwrap();

    // recv
    let msg = client.incoming.unwrap().recv().unwrap();
    assert_eq!(msg[..8], [200, 200, 200, 200, 200, 200, 200, 200]);
}
