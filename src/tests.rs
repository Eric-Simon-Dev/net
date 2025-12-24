use bytes::BytesMut;

use crate::{client, server};

#[test]
fn connection() {
    let mut server =
        server::Server::new("0:12012", server::Configuration { max_clients: 256 }).unwrap();
    let (_, _) = server.listen().unwrap();
    let mut client = client::Client::new("0:12013").unwrap();
    let (_, _) = client.connect("0:12012").unwrap();
}

#[test]
fn multiple_exchanges() {
    // create
    let mut server =
        server::Server::new("0:12014", server::Configuration { max_clients: 256 }).unwrap();
    let (server_sender, server_receiver) = server.listen().unwrap();
    let mut client = client::Client::new("0:12015").unwrap();
    let (client_sender, client_receiver) = client.connect("0:12014").unwrap();

    // client : first send
    let data = BytesMut::zeroed(1).freeze();
    client_sender
        .send(client::Message {
            data,
            channel: 0,
            guarantees: client::Guarantees::None,
        })
        .unwrap();

    // server & client : recv, add 1, return (16 times)
    for _ in 0..16 {
        let server::Message {
            data,
            client,
            channel,
            guarantees,
        } = server_receiver.recv().unwrap();
        let mut data = BytesMut::from(data);
        data[0] += 1;
        let data = data.freeze();
        server_sender
            .send(server::Message {
                data,
                client,
                channel,
                guarantees,
            })
            .unwrap();

        let client::Message {
            data,
            channel,
            guarantees,
        } = client_receiver.recv().unwrap();
        let mut data = BytesMut::from(data);
        data[0] += 1;
        let data = data.freeze();
        client_sender
            .send(client::Message {
                data,
                channel,
                guarantees,
            })
            .unwrap();
    }

    // server: last recv
    let server::Message {
        data,
        client: _,
        channel: _,
        guarantees: _,
    } = server_receiver.recv().unwrap();

    assert_eq!(data[0], 32);
}
