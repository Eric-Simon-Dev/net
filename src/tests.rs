use bytes::BytesMut;

#[test]
fn server_client_connection() {
    use crate::{client::Client, server::Server};

    let _server = Server::new(12012).unwrap();
    let mut client = Client::new(12013).unwrap();
    client.connect("0:12012").unwrap();
}

#[test]
fn multiple_exchanges() {
    use crate::{client::Client, server::Server};

    let mut server = Server::new(12014).unwrap();
    let mut client = Client::new(12015).unwrap();
    client.connect("0:12014").unwrap();

    // first send
    let msg = BytesMut::zeroed(1);
    client.outgoing_message().unwrap().send(msg).unwrap();

    // exchange and add 1 each return
    for _ in 0..16 {
        let (mut msg, client_index) = server.incoming_message().recv().unwrap();
        msg[0] += 1;
        server.outgoing_message().send((msg, client_index)).unwrap();

        let mut msg = client.incoming_message().unwrap().recv().unwrap();
        msg[0] += 1;
        client.outgoing_message().unwrap().send(msg).unwrap();
    }

    let (msg, _) = server.incoming_message().recv().unwrap();
    assert_eq!(msg[0], 32);
}
