#!/usr/bin/env python
"""
swiftnbd. server module

Changed to support backy2 blocks instead of swift in 2015 by
Daniel Kraft <daniel.kraft@d9t.de>

Copyright (C) 2013-2015 by Juan J. Martinez <jjm@usebox.net>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
"""

import asyncio
import errno
import logging
import math
import signal
import struct
import traceback

from backy2.exception import NbdServerAbortedNegotiationError


class Server(object):
    """
    Class implementing the server.
    """

    # NBD's magic
    NBD_HANDSHAKE = 0x49484156454F5054
    NBD_REPLY = 0x3e889045565a9

    NBD_REQUEST = 0x25609513
    NBD_RESPONSE = 0x67446698

    NBD_OPT_EXPORTNAME = 1
    NBD_OPT_ABORT = 2
    NBD_OPT_LIST = 3

    NBD_REP_ACK = 1
    NBD_REP_SERVER = 2
    NBD_REP_ERR_UNSUP = 2**31 + 1

    NBD_CMD_READ = 0
    NBD_CMD_WRITE = 1
    NBD_CMD_DISC = 2
    NBD_CMD_FLUSH = 3
    NBD_CMD_TRIM = 4
    NBD_CMD_WRITE_ZEROES = 6

    # fixed newstyle handshake
    NBD_HANDSHAKE_FLAGS = (1 << 0)

    NBD_FLAG_HAS_FLAGS = (1<< 0)
    NBD_FLAG_READ_ONLY = (1 << 1)
    NBD_FLAG_SEND_FLUSH = (1 << 2)
    NBD_FLAG_SEND_FUA = (1 << 3)
    NBD_FLAG_ROTATIONAL = (1 << 4)
    NBD_FLAG_SEND_TRIM = (1 << 5)
    NBD_FLAG_SEND_WRITE_ZEROES = (1 << 6)
    NBD_FLAG_CAN_MULTI_CONN = (1 << 8)

    # has flags, supports flush
    NBD_EXPORT_FLAGS = NBD_FLAG_HAS_FLAGS ^ NBD_FLAG_SEND_FLUSH

    # command flags (upper 16 bit of request type)
    NBD_CMD_FLAG_FUA = (1 << 16)

    def __init__(self, addr, store, read_only=True):
        self.log = logging.getLogger(__package__)

        self.address = addr
        self.store = store
        self.read_only = read_only

        if asyncio.get_event_loop().is_closed():
            asyncio.set_event_loop(asyncio.new_event_loop())
        self.loop = asyncio.get_event_loop()

    @asyncio.coroutine
    def nbd_response(self, writer, handle, error=0, data=None):
        writer.write(struct.pack('>LLQ', self.NBD_RESPONSE, error, handle))
        if data:
            writer.write(data)
        yield from writer.drain()

    @asyncio.coroutine
    def handler(self, reader, writer):
        """Handle the connection"""
        try:
            host, port = writer.get_extra_info("peername")
            version, cow_version = None, None
            self.log.info("Incoming connection from %s:%s" % (host,port))

            # initial handshake
            writer.write(b"NBDMAGIC" + struct.pack(">QH", self.NBD_HANDSHAKE, self.NBD_HANDSHAKE_FLAGS))
            yield from writer.drain()

            data = yield from reader.readexactly(4)
            try:
                client_flag = struct.unpack(">L", data)[0]
            except struct.error:
                raise IOError("Handshake failed, disconnecting")

            # we support both fixed and unfixed new-style handshake
            if client_flag == 0:
                fixed = False
                self.log.warning("Client using new-style non-fixed handshake")
            elif client_flag & 1:
                fixed = True
            else:
                raise IOError("Handshake failed, disconnecting")

            # negotiation phase
            while True:
                header = yield from reader.readexactly(16)
                try:
                    (magic, opt, length) = struct.unpack(">QLL", header)
                except struct.error as ex:
                    raise IOError("Negotiation failed: Invalid request, disconnecting")

                if magic != self.NBD_HANDSHAKE:
                    raise IOError("Negotiation failed: bad magic number: %s" % magic)

                if length:
                    data = yield from reader.readexactly(length)
                    if(len(data) != length):
                        raise IOError("Negotiation failed: %s bytes expected" % length)
                else:
                    data = None

                self.log.debug("[%s:%s]: opt=%s, len=%s, data=%s" % (host, port, opt, length, data))

                if opt == self.NBD_OPT_EXPORTNAME:
                    if not data:
                        raise IOError("Negotiation failed: no export name was provided")

                    data = data.decode("utf-8")
                    if data not in [v.uid for v in self.store.get_versions()]:
                        if not fixed:
                            raise IOError("Negotiation failed: unknown export name")

                        writer.write(struct.pack(">QLLL", self.NBD_REPLY, opt, self.NBD_REP_ERR_UNSUP, 0))
                        yield from writer.drain()
                        continue

                    # we have negotiated a version and it will be used
                    # until the client disconnects
                    version = self.store.get_version(data)

                    self.log.info("[%s:%s] Negotiated export: %s" % (host, port, version.uid))

                    export_flags = self.NBD_EXPORT_FLAGS
                    if self.read_only:
                        export_flags ^= self.NBD_FLAG_READ_ONLY
                        self.log.info("nbd is read only.")
                    else:
                        self.log.info("nbd is read/write.")

                    # In case size is not a multiple of 4096 we extend it to the the maximum support block
                    # size of 4096
                    size = math.ceil(version.size / 4096) * 4096
                    writer.write(struct.pack('>QH', size, export_flags))
                    writer.write(b"\x00"*124)
                    yield from writer.drain()

                    # Transition to transmission phase
                    break

                elif opt == self.NBD_OPT_LIST:
                    for _version in self.store.get_versions():
                        writer.write(struct.pack(">QLLL", self.NBD_REPLY, opt, self.NBD_REP_SERVER, len(_version.uid) + 4))
                        version_encoded = _version.uid.encode("utf-8")
                        writer.write(struct.pack(">L", len(version_encoded)))
                        writer.write(version_encoded)
                        yield from writer.drain()

                    writer.write(struct.pack(">QLLL", self.NBD_REPLY, opt, self.NBD_REP_ACK, 0))
                    yield from writer.drain()

                elif opt == self.NBD_OPT_ABORT:
                    writer.write(struct.pack(">QLLL", self.NBD_REPLY, opt, self.NBD_REP_ACK, 0))
                    yield from writer.drain()

                    raise NbdServerAbortedNegotiationError()
                else:
                    # we don't support any other option
                    if not fixed:
                        raise IOError("Unsupported option")

                    writer.write(struct.pack(">QLLL", self.NBD_REPLY, opt, self.NBD_REP_ERR_UNSUP, 0))
                    yield from writer.drain()

            # operation phase
            while True:
                header = yield from reader.readexactly(28)
                try:
                    (magic, cmd, handle, offset, length) = struct.unpack(">LLQQL", header)
                except struct.error:
                    raise IOError("Invalid request, disconnecting")

                if magic != self.NBD_REQUEST:
                    raise IOError("Bad magic number, disconnecting")

                self.log.debug("[%s:%s]: cmd=%s, handle=%s, offset=%s, len=%s" % (host, port, cmd, handle, offset, length))

                if cmd == self.NBD_CMD_DISC:
                    self.log.info("[%s:%s] disconnecting" % (host, port))
                    break

                elif cmd == self.NBD_CMD_WRITE:
                    data = yield from reader.readexactly(length)
                    if(len(data) != length):
                        raise IOError("%s bytes expected, disconnecting" % length)

                    if self.read_only:
                        yield from self.nbd_response(writer, handle, error=errno.EPERM)
                        continue

                    if not cow_version:
                        cow_version = self.store.get_cow_version(version)
                    try:
                        self.store.write(cow_version, offset, data)
                    except Exception as ex:
                        self.log.error("[%s:%s] NBD_CMD_WRITE: %s\n%s" % (host, port, ex, traceback.format_exc()))
                        yield from self.nbd_response(writer, handle, error=ex.errno if hasattr(ex, 'errno') else errno.EIO)
                        continue

                    yield from self.nbd_response(writer, handle)

                elif cmd == self.NBD_CMD_READ:
                    try:
                        if cow_version:
                            data = self.store.read(cow_version, offset, length)
                        else:
                            data = self.store.read(version, offset, length)
                    except Exception as ex:
                        self.log.error("[%s:%s] NBD_CMD_READ: %s\n%s" % (host, port, ex, traceback.format_exc()))
                        yield from self.nbd_response(writer, handle, error=ex.errno if hasattr(ex, 'errno') else errno.EIO)
                        continue

                    yield from self.nbd_response(writer, handle, data=data)

                elif cmd == self.NBD_CMD_FLUSH:
                    if self.read_only or not cow_version:
                        yield from self.nbd_response(writer, handle)
                        continue

                    try:
                        self.store.flush(cow_version)
                    except Exception as ex:
                        self.log.error("[%s:%s] NBD_CMD_FLUSH: %s\n%s" % (host, port, ex, traceback.format_exc()))
                        yield from self.nbd_response(writer, handle, error=ex.errno if hasattr(ex, 'errno') else errno.EIO)
                        continue

                    yield from self.nbd_response(writer, handle)

                else:
                    self.log.warning("[%s:%s] Unknown cmd %s, disconnecting" % (host, port, cmd))
                    break

        except NbdServerAbortedNegotiationError:
            self.log.info("[%s:%s] Client aborted negotiation" % (host, port))

        except (asyncio.IncompleteReadError, IOError) as ex:
            self.log.error("[%s:%s] %s" % (host, port, ex))

        finally:
            if cow_version:
                self.store.fixate(cow_version)
            writer.close()


    def serve_forever(self):
        """Create and run the asyncio loop"""
        addr, port = self.address

        loop = self.loop
        coro = asyncio.start_server(self.handler, addr, port, loop=loop)
        server = loop.run_until_complete(coro)

        loop.add_signal_handler(signal.SIGTERM, loop.stop)
        loop.add_signal_handler(signal.SIGINT, loop.stop)

        loop.run_forever()

        server.close()
        loop.run_until_complete(server.wait_closed())
        loop.close()
        
    def stop(self):
        if not self.loop.is_closed():
            self.loop.call_soon_threadsafe(self.loop.stop)
