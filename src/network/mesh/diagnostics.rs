//! Sibling libp2p request-response behaviour for mesh diagnostics over the
//! gossip port (3382). This is intentionally SEPARATE from the Malachite `sync`
//! rpc (which owns its own request/response types) so the crawl can extend the
//! wire format freely without touching consensus sync.
//!
//! The per-node query reuses the local-view types verbatim: request
//! [`proto::GetMeshViewRequest`], response [`proto::MeshView`]. Messages are
//! prost-encoded (consistent with the rest of the codebase) and framed as a
//! single length-bounded blob per substream.

use crate::proto;
use async_trait::async_trait;
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use libp2p::request_response::{self, Codec};
use libp2p::StreamProtocol;
use prost::Message;
use std::io;

/// Protocol id for the mesh diagnostics request-response behaviour.
pub const MESH_DIAGNOSTICS_PROTOCOL: StreamProtocol =
    StreamProtocol::new("/snapchain/diagnostics/mesh/1.0.0");

/// Upper bound on a single diagnostics message. A full `MeshView` is small
/// (peer facts + rates); 8 MiB is generous headroom and a DoS guard.
const MAX_MESSAGE_SIZE: u64 = 8 * 1024 * 1024;

/// Behaviour alias: a request-response behaviour speaking the mesh diagnostics
/// protocol with prost-encoded `GetMeshViewRequest` -> `MeshView`.
pub type Behaviour = request_response::Behaviour<MeshDiagnosticsCodec>;
/// Event alias for the diagnostics behaviour.
pub type Event = request_response::Event<proto::GetMeshViewRequest, proto::MeshView>;

/// prost codec for the diagnostics protocol. Each request/response is a single
/// substream, so we read to end (capped) and decode.
#[derive(Clone, Default)]
pub struct MeshDiagnosticsCodec;

#[async_trait]
impl Codec for MeshDiagnosticsCodec {
    type Protocol = StreamProtocol;
    type Request = proto::GetMeshViewRequest;
    type Response = proto::MeshView;

    async fn read_request<T>(&mut self, _: &StreamProtocol, io: &mut T) -> io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        let bytes = read_to_end(io).await?;
        proto::GetMeshViewRequest::decode(bytes.as_slice()).map_err(decode_err)
    }

    async fn read_response<T>(
        &mut self,
        _: &StreamProtocol,
        io: &mut T,
    ) -> io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        let bytes = read_to_end(io).await?;
        // A real MeshView always carries a `local` block, so it never encodes to
        // zero bytes. An empty body means the responder closed the stream without
        // answering (e.g. a non-validator request the responder refused) — treat
        // it as a failure rather than letting prost decode it to a default view.
        if bytes.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "empty mesh diagnostics response (request refused)",
            ));
        }
        proto::MeshView::decode(bytes.as_slice()).map_err(decode_err)
    }

    async fn write_request<T>(
        &mut self,
        _: &StreamProtocol,
        io: &mut T,
        req: Self::Request,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        io.write_all(&req.encode_to_vec()).await?;
        io.close().await
    }

    async fn write_response<T>(
        &mut self,
        _: &StreamProtocol,
        io: &mut T,
        res: Self::Response,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        io.write_all(&res.encode_to_vec()).await?;
        io.close().await
    }
}

async fn read_to_end<T>(io: &mut T) -> io::Result<Vec<u8>>
where
    T: AsyncRead + Unpin + Send,
{
    let mut buf = Vec::new();
    AsyncReadExt::take(&mut *io, MAX_MESSAGE_SIZE)
        .read_to_end(&mut buf)
        .await?;
    Ok(buf)
}

fn decode_err(e: prost::DecodeError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, e)
}
