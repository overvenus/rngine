use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::Mutex;

use futures::sync::mpsc::UnboundedReceiver;
use futures::{Future, Sink, Stream};
use grpcio::{
    ClientStreamingSink, DuplexSink, EnvBuilder, Environment, RequestStream, RpcContext,
    Server as GrpcServer, ServerBuilder, WriteFlags,
};
use kvproto::enginepb::{CommandRequestBatch, CommandResponseBatch, SnapshotDone, SnapshotRequest};
use kvproto::enginepb_grpc::*;

use super::engine::{ApplyTask, Engine as Rngine};
use super::worker::Scheduler;

#[derive(Clone)]
pub struct Service {
    apply: Scheduler<ApplyTask>,
    applied_receiver: Arc<Mutex<Option<UnboundedReceiver<CommandResponseBatch>>>>,
}

impl Service {
    pub fn new(rg: &mut Rngine) -> Service {
        Service {
            apply: rg.apply_scheduler(),
            applied_receiver: Arc::new(Mutex::new(rg.take_apply_receiver())),
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
        let apply = self.apply.clone();
        let reqs = stream
            .for_each(move |cmds| {
                apply.schedule(ApplyTask::new(cmds)).unwrap();
                Ok(())
            }).map_err(|e| {
                error!("{:?}", e);
            });

        let applied_receiver = self
            .applied_receiver
            .lock()
            .unwrap()
            .take()
            .expect("apply already started");
        let resps = sink
            .sink_map_err(|e| {
                error!("{:?}", e);
            }).send_all(applied_receiver.map(|resp| (resp, WriteFlags::default())))
            .map(|_| ())
            .map_err(|e| error!("{:?}", e));

        ctx.spawn(reqs);
        ctx.spawn(resps);
    }

    fn apply_snapshot(
        &mut self,
        _ctx: RpcContext,
        _stream: RequestStream<SnapshotRequest>,
        _sink: ClientStreamingSink<SnapshotDone>,
    ) {
        unimplemented!()
    }
}

pub struct Server {
    server: GrpcServer,
    _env: Arc<Environment>,
}

impl Server {
    pub fn start(addr: &str, svc: Service) -> Server {
        let env = EnvBuilder::new().cq_count(1).name_prefix("rg").build();
        let env = Arc::new(env);
        let addr = SocketAddr::from_str(addr).unwrap();
        info!("listening on {}", addr);
        let ip = format!("{}", addr.ip());
        let mut server = ServerBuilder::new(env.clone())
            .register_service(create_engine(svc))
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
