import json
import bson
import functools
import collections
import traceback

class MongoIdCodec:
    def call_recursive(self, key, value, func):
        t = type(value)
        if t == dict:
            for key_, value_ in value.items():
                value[key_] = self.call_recursive(key_, value_, func)
        elif t == list:
            for i in range(len(value)):
                value[i] = self.call_recursive(i, value[i], func)
        else:
            return func(key, value)
        return value
    

    def encode(self, data):
        def replace_id(key, value):
            if key != '_id':
                return value
            return str(value)

        return self.call_recursive(None, data, replace_id)

    def decode(self, data):
        def replace_id(key, value):
            if key != '_id':
                return value
            return bson.ObjectId(value)

        return self.call_recursive(None, data, replace_id)

class NatsJsonRPC():
    class RPCProxy:
        def __init__(self, jsonRPC) -> None:
            self._rpc = jsonRPC

        def __getattr__(self, name: str):
            return functools.partial(self._rpc.rpc_call, f'{self._rpc._prefix}{name}')

    def __init__(self, nats, prefix):
        self._nats = nats
        self._prefix = prefix
        self._rpc_funcs = []
        self.proxy = self.RPCProxy(self)
        self.codecs = [
            collections.namedtuple('BinaryCodec', ['encode', 'decode'], defaults=[str.encode, bytes.decode])(),
            collections.namedtuple('JsonCodec', ['encode', 'decode'], defaults=[json.dumps, json.loads])(),
        ]

    def rpc_register(self, func):
        self._rpc_funcs.append(func)
        return func
    
    def _codecs_encode(self, value):
        for codec in self.codecs[::-1]:
            value = codec.encode(value)
        return value
    
    def _codecs_decode(self, value):
        for codec in self.codecs:
            value = codec.decode(value)
        return value
    
    async def rpc_call(self, subject, **args):
        result = await self._nats.request(subject, self._codecs_encode(args), timeout=20)

        result = self._codecs_decode(result.data)
        try:
            exception = result['exception']
            exception['args'].insert(0, f'RPC Exception "{exception["type"]}"')
            raise Exception(exception['args'])
        except KeyError:
            pass
        return result['result']
        

    async def rpc_init(self):
        for rpc_func in self._rpc_funcs:
            async def func(callback, message):
                payload = self._codecs_decode(message.data)

                try:
                    result = {
                        'result': await callback(**payload)
                    }
                except Exception as e:
                    result = {
                        'exception': {
                            'type': type(e).__name__,
                            'args': e.args,
                            'stack': list(map(lambda frame: {
                                'file': frame.filename,
                                'lineno': frame.lineno,
                                'line': frame.line
                            }, traceback.extract_tb(e.__traceback__)))
                        }
                    }
                reply = message.reply
                if not reply:
                    return
                
                await self._nats.publish(reply, self._codecs_encode(result))

            topic = f'{self._prefix}{rpc_func.__name__}'
            await self._nats.subscribe(topic, cb=functools.partial(func, rpc_func), queue=topic)
