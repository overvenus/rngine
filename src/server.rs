use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::Mutex;

use futures::sync::mpsc::UnboundedReceiver;
use futures::sync::oneshot;
use futures::{stream, Async, Future, Sink, Stream};
use grpcio::{
    ChannelBuilder, ClientStreamingSink, DuplexSink, Environment, RequestStream, RpcContext,
    RpcStatus, RpcStatusCode, Server as GrpcServer, ServerBuilder, WriteFlags,
};
use kvproto::enginepb::{CommandRequestBatch, CommandResponseBatch, SnapshotDone, SnapshotRequest};
use kvproto::enginepb_grpc::*;

use super::engine::{ApplyTask, Engine as Rngine, RegionTask};
use super::worker::Scheduler;

#[derive(Clone)]
pub struct Service {
    apply: Scheduler<ApplyTask>,
    applied_receiver: Arc<Mutex<Option<UnboundedReceiver<CommandResponseBatch>>>>,

    region: Scheduler<RegionTask>,
}

impl Service {
    pub fn new(rg: &mut Rngine) -> Service {
        Service {
            apply: rg.apply_scheduler(),
            applied_receiver: Arc::new(Mutex::new(rg.take_apply_receiver())),
            region: rg.region_scheduler(),
        }
    }
}

impl Engine for Service {
    fn apply_command_batch(
        &mut self,
        ctx: RpcContext,
        stream: RequestStream<CommandRequestBatch>,
        sink: DuplexSink<CommandResponseBatch>,
    ) {
        let mut applied_receiver = self.applied_receiver.lock().unwrap().take();
        if applied_receiver.is_none() {
            let fail = sink.fail(RpcStatus::new(
                RpcStatusCode::Unavailable,
                Some("apply already scheduled".to_owned()),
            ));
            ctx.spawn(fail.map_err(|e| {
                error!("{:?}", e);
            }));
            return;
        }

        let apply = self.apply.clone();
        let (tx, mut done) = oneshot::channel();
        let reqs = stream
            .for_each(move |cmds| {
                apply.schedule(ApplyTask::commands(cmds)).unwrap();
                Ok(())
            })
            .map_err(|e| {
                error!("{:?}", e);
            })
            .then(move |_| tx.send(()));

        let applied_receiver_holder = self.applied_receiver.clone();
        let poll_and_put_back = stream::poll_fn(move || {
            let res = match done.poll() {
                Ok(Async::Ready(_)) | Err(_) => Ok(Async::Ready(None)),
                Ok(Async::NotReady) => {
                    match applied_receiver.as_mut().expect("already finished").poll() {
                        Ok(Async::Ready(Some(item))) => {
                            debug!("polled a request");
                            return Ok(Async::Ready(Some((item, WriteFlags::default()))));
                        }
                        Ok(Async::NotReady) => return Ok(Async::NotReady),
                        Ok(Async::Ready(None)) => Ok(Async::Ready(None)),
                        Err(e) => {
                            error!("{:?}", e);
                            Err(e)
                        }
                    }
                }
            };
            debug!("put receiver back");
            *applied_receiver_holder.lock().unwrap() = applied_receiver.take();
            res
        });
        let resps = sink
            .sink_map_err(|e| {
                error!("{:?}", e);
            })
            .send_all(poll_and_put_back)
            .map(|_| ())
            .map_err(|e| error!("{:?}", e));

        ctx.spawn(reqs);
        ctx.spawn(resps);
    }

    fn apply_snapshot(
        &mut self,
        ctx: RpcContext,
        stream: RequestStream<SnapshotRequest>,
        sink: ClientStreamingSink<SnapshotDone>,
    ) {
        let (tx, rx) = oneshot::channel();
        let (sender, task) = RegionTask::snapshot(tx);
        let reqs = stream
            .for_each(move |chunk| {
                sender.send(chunk).unwrap();
                Ok(())
            })
            .map_err(|e| {
                error!("{:?}", e);
            });

        let resp = rx
            .then(move |_| sink.success(Default::default()))
            .map_err(|e| error!("{:?}", e));

        self.region.schedule(task).unwrap();
        ctx.spawn(reqs);
        ctx.spawn(resp);
    }
}

pub struct Server {
    server: GrpcServer,
    _env: Arc<Environment>,
}

impl Server {
    pub fn start(env: Arc<Environment>, addr: &str, svc: Service) -> Server {
        let args = ChannelBuilder::new(env.clone())
            .max_receive_message_len(-1)
            .max_send_message_len(-1)
            .build_args();
        let addr = SocketAddr::from_str(addr).unwrap();
        info!("listening on {}", addr);
        let ip = format!("{}", addr.ip());
        let mut server = ServerBuilder::new(env.clone())
            .register_service(create_engine(svc))
            .channel_args(args)
            .bind(ip, addr.port())
            .build()
            .unwrap();
        server.start();
        Server { server, _env: env }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.server.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use grpcio::EnvBuilder;

    use super::*;
    use crate::worker;

    use futures::sync::mpsc::unbounded;

    #[test]
    fn test_reconnect() {
        env_logger::init();

        let (apply, apply_rx) = worker::dummy_scheduler();
        let (region, _region_rx) = worker::dummy_scheduler();
        let (mut applied_tx, applied_receiver) = unbounded();
        let svc = Service {
            apply,
            applied_receiver: Arc::new(Mutex::new(Some(applied_receiver))),
            region,
        };

        let env = Arc::new(EnvBuilder::new().cq_count(2).name_prefix("rg").build());
        let server = Server::start(env.clone(), &"127.0.0.1:0", svc);
        let port = server.server.bind_addrs()[0].1;
        let ch = ChannelBuilder::new(env).connect(&format!("127.0.0.1:{}", port));
        let client = EngineClient::new(ch);

        {
            // The first connection.
            let (sender1, receiver1) = client.apply_command_batch().unwrap();
            let _sender1 = sender1
                .send((CommandRequestBatch::new(), WriteFlags::default()))
                .wait()
                .unwrap();
            apply_rx.recv_timeout(Duration::from_millis(100)).unwrap();
            applied_tx = applied_tx.send(CommandResponseBatch::new()).wait().unwrap();
            let (resp, _receiver1) = receiver1.into_future().wait().map_err(|(e, _)| e).unwrap();
            resp.unwrap();

            // The second connection should fail.
            let (_sender2, receiver2) = client.apply_command_batch().unwrap();
            match receiver2.into_future().wait().map_err(|(e, _)| e) {
                Err(grpcio::Error::RpcFailure(status)) => {
                    assert_eq!(status.status, RpcStatusCode::Unavailable);
                    assert_eq!(
                        status.details.unwrap(),
                        "apply already scheduled".to_owned()
                    );
                }
                Err(e) => panic!("unexpected {:?}", e),
                _ => panic!("unexpected"),
            }

            // Drop the first and the second connections.
        }

        // A new connection.
        let (sender1, receiver1) = client.apply_command_batch().unwrap();
        let _sender1 = sender1
            .send((CommandRequestBatch::new(), WriteFlags::default()))
            .wait()
            .unwrap();
        apply_rx.recv_timeout(Duration::from_millis(100)).unwrap();
        let _applied_tx = applied_tx.send(CommandResponseBatch::new()).wait().unwrap();
        let (resp, _receiver1) = receiver1.into_future().wait().map_err(|(e, _)| e).unwrap();
        resp.unwrap();
    }
}
