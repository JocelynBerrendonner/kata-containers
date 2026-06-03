// Copyright (c) 2019-2022 Alibaba Cloud
// Copyright (c) 2019-2022 Ant Group
//
// SPDX-License-Identifier: Apache-2.0
//

use std::os::unix::io::RawFd;

use anyhow::{Context, Result};
use kata_sys_util::spec::get_bundle_path;

use crate::{
    core_sched, logger,
    shim::{ShimExecutor, ENV_KATA_RUNTIME_BIND_FD},
    Error,
};

impl ShimExecutor {
    pub async fn run(&mut self) -> Result<()> {
        crate::panic_hook::set_panic_hook();
        let sid = self.args.id.clone();
        debug_breadcrumb("run:begin", &format!("sid={}", sid));
        let bundle_path = get_bundle_path().context("get bundle")?;
        debug_breadcrumb("run:bundle", &format!("bundle={}", bundle_path.display()));
        let path = bundle_path.join("log");
        let _logger_guard =
            logger::set_logger(path.to_str().unwrap(), &sid, self.args.debug).context("set logger");
        debug_breadcrumb("run:logger_ready", &format!("path={}", path.display()));
        // Regist shim logger for later use.
        logging::register_subsystem_logger("runtimes", "shim");

        if try_core_sched().is_err() {
            warn!(
                sl!(),
                "Failed to enable core sched since prctl() returns non-zero value."
            );
        }

        debug_breadcrumb("run:calling_do_run", "");
        self.do_run()
            .await
            .map_err(|err| {
                error!(sl!(), "failed run shim {:?}", err);
                debug_breadcrumb("run:do_run_failed", &format!("err={:?}", err));
                err
            })
            .context("run shim")?;
        debug_breadcrumb("run:do_run_returned", "");

        Ok(())
    }

    async fn do_run(&mut self) -> Result<()> {
        info!(sl!(), "start to run");
        debug_breadcrumb("do_run:begin", "");
        self.args.validate(false).context("validate")?;
        debug_breadcrumb("do_run:args_validated", "");

        let server_fd = get_server_fd().context("get server fd")?;
        debug_breadcrumb("do_run:server_fd", &format!("fd={}", server_fd));
        let service_manager = service::ServiceManager::new(
            &self.args.id,
            &self.args.publish_binary,
            &self.args.address,
            &self.args.namespace,
            server_fd,
        )
        .await
        .context("new shim server")?;
        debug_breadcrumb("do_run:service_manager_built", "");
        service_manager.run().await.context("run")?;
        debug_breadcrumb("do_run:service_manager_run_returned", "");

        Ok(())
    }
}

// === DEBUG (temporary): persistent breadcrumb for the run/do_run pipeline.
// Useful because the slog logger isn't installed until partway through
// `run()`, and we want timestamps for every step even on cold start.
fn debug_breadcrumb(stage: &str, extra: &str) {
    use std::io::Write;
    let _ = std::fs::create_dir_all("/var/log/kata-shim");
    if let Ok(mut f) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open("/var/log/kata-shim/run.log")
    {
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);
        let _ = writeln!(
            f,
            "[{:.3}] pid={} stage={} {}",
            ts,
            std::process::id(),
            stage,
            extra
        );
    }
    eprintln!("kata-shim-debug: run stage={} {}", stage, extra);
}

fn get_server_fd() -> Result<RawFd> {
    let env_fd = std::env::var(ENV_KATA_RUNTIME_BIND_FD).map_err(Error::EnvVar)?;
    let fd = env_fd
        .parse::<RawFd>()
        .map_err(|_| Error::ServerFd(env_fd))?;
    Ok(fd)
}

// TODO: currently we log a warning on fail (i.e. kernel version < 5.14), maybe just exit
// TODO: more test on higher version of kernel
fn try_core_sched() -> Result<()> {
    if let Ok(v) = std::env::var("SCHED_CORE") {
        if !v.is_empty() {
            core_sched::core_sched_create(core_sched::PROCESS_GROUP)?
        }
    }
    Ok(())
}
