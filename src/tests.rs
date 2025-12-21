use bytes::BytesMut;

use crate::{Client, Server};

#[test]
fn connection() {
    let _server = Server::new("0:12012").unwrap();
    let _client = Client::new("0:12013", "0:12013").unwrap();
}

#[test]
fn multiple_exchanges() {
    // create
    let mut server = Server::new("0:12014").unwrap();
    let mut client = Client::new("0:12015", "0:12014").unwrap();

    // client : first send
    let msg = BytesMut::zeroed(1);
    client.outgoing().send((msg, 0)).unwrap();

    // server & client : recv, add 1, return (16 times)
    for _ in 0..16 {
        let (mut msg, client_index, channel) = server.incoming().recv().unwrap();
        msg[0] += 1;
        server
            .outgoing()
            .send((msg, client_index, channel))
            .unwrap();

        let (mut msg, channel) = client.incoming().recv().unwrap();
        msg[0] += 1;
        client.outgoing().send((msg, channel)).unwrap();
    }

    // server: last recv
    let (msg, _, _) = server.incoming().recv().unwrap();

    assert_eq!(msg[0], 32);
}
