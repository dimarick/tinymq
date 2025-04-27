Build
======

`go build`

Run
===

Minimal:

`./tinymq --exchanges="exchanges.json"`

Custom port and storage path:

`./tinymq --exchanges="exchanges.json" --port=8081 --storagePath=/var/lib/tinymq`

Enable deduplication:

`./tinymq --exchanges="exchanges.json" --hashtable="hash.db"`

Test
====

Run tests:

`go test ./...`

Check for service is work:

Run `./tinymq --exchanges="exchanges.json"`

Open in browser https://websocket.tech/

Enter url `ws://127.0.0.1:8080/consume/queue1/100`

Run 
```
curl --location 'http://127.0.0.1:8080/publish/exchange1' \
--header 'Content-Type: application/json' \
--data '[
{
"Id": 1,
"Data": "Some message 1"
},
{
"Id": 2,
"Data": "Another message 2"
}
]'
```

You are should immediatelly see this messages in browser.

TODO
====

1. Stat collection and api
2. Cluster.
3. More strict garantee.
4. Support XA.
5. Security.
6. Optimize flushStatus.
7. Configurable status files storage.
8. Optimize file rotation.
9. More configuration options.