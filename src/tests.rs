use bytes::BytesMut;

use crate::{client, server};

#[test]
fn connection() {
    let _server = server::listen("0:12012").unwrap();
    let _client = client::connect("0:12013", "0:12013").unwrap();
}

#[test]
fn multiple_exchanges() {
    // create
    let mut server = server::listen("0:12014").unwrap();
    let mut client = client::connect("0:12015", "0:12014").unwrap();

    // client : first send
    let msg = BytesMut::zeroed(1);
    client.outgoing().send(msg).unwrap();

    // server & client : recv, add 1, return (16 times)
    for _ in 0..16 {
        let (mut msg, client_index) = server.incoming().recv().unwrap();
        msg[0] += 1;
        server.outgoing().send((msg, client_index)).unwrap();

        let mut msg = client.incoming().recv().unwrap();
        msg[0] += 1;
        client.outgoing().send(msg).unwrap();
    }

    // server: last recv
    let (msg, _) = server.incoming().recv().unwrap();

    assert_eq!(msg[0], 32);
}
