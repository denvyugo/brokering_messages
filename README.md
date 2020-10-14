# Server for messages

## Commands

_GET /messages/{number}_

It will return JSON with list of messages. A `{number}` is an optional limit for list length.

_GET /message/{id}_

It will return JSON of message with corresponding `{id}`.

_POST /messages_

Create a new message:

`curl -XPOST http://0.0.0.0:8000/messages -H "Content-Type: application/json" 
-d "{'recipient': 'recipient', 'source': 1, 'body': 'some text message'`

It will return JSON `{'id': 1}` if operation succeed or `{'error': 'error description'}`.
 