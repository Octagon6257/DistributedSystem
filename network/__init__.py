__all__ = ['MessageProtocol', 'ChordMessage', 'RPCClient', 'SocketServer']

def __getattr__(name):
    if name == 'MessageProtocol':
        from .MessageProtocol import MessageProtocol
        return MessageProtocol
    elif name == 'ChordMessage':
        from .MessageProtocol import ChordMessage
        return ChordMessage
    elif name == 'RPCClient':
        from .RpcClient import RPCClient
        return RPCClient
    elif name == 'SocketServer':
        from .SocketServer import SocketServer
        return SocketServer
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")