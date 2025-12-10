use bytes::BytesMut;

use crate::{client::Client, server::Server};

#[test]
fn connection() {
    let mut server = Server::new("0:12012").unwrap();
    let mut client = Client::new("0:12013").unwrap();
    server.listen().unwrap();
    client.connect("0:12012").unwrap();
}

#[test]
fn multiple_exchanges() {
    // create
    let mut server = Server::new("0:12014").unwrap();
    let mut client = Client::new("0:12015").unwrap();
    server.listen().unwrap();
    client.connect("0:12014").unwrap();

    // client : first send
    let msg = BytesMut::zeroed(1);
    client.outgoing().unwrap().send(msg).unwrap();

    // server & client : recv, add 1, return (16 times)
    for _ in 0..16 {
        let (mut msg, client_index) = server.incoming().unwrap().recv().unwrap();
        msg[0] += 1;
        server
            .outgoing()
            .unwrap()
            .send((msg, client_index))
            .unwrap();

        let mut msg = client.incoming().unwrap().recv().unwrap();
        msg[0] += 1;
        client.outgoing().unwrap().send(msg).unwrap();
    }

    // server: last recv
    let (msg, _) = server.incoming().unwrap().recv().unwrap();

    assert_eq!(msg[0], 32);
}
