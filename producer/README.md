### Introduce
It's a demo to show how to create a simple sync/async producer，and use custom serializer to serialize our message

It is not recommended to use a custom serializer， because of this, both the production side and the client side 
need to maintain this custom serializer， if upstream changes, downstream needs to change too. Despite this，we can
use JSON，Thrift or ProtoBuf to serialize our message