// Copyright (c) 2019-2022 Alibaba Cloud
// Copyright (c) 2019-2022 Ant Group
//
// SPDX-License-Identifier: Apache-2.0
//

use std::{boxed::Box, fs::OpenOptions, io::Write, ops::Deref};

use backtrace::Backtrace;

const KMESG_DEVICE: &str = "/dev/kmsg";

// TODO: the Kata 1.x runtime had a SIGUSR1 handler that would log a formatted backtrace on
// receiving that signal. It could be useful to re-add that feature.
pub(crate) fn set_panic_hook() {
    std::panic::set_hook(Box::new(move |panic_info| {
        let (filename, line) = panic_info
            .location()
            .map(|loc| (loc.file(), loc.line()))
            .unwrap_or(("<unknown>", 0));

        let cause = panic_info
            .payload()
            .downcast_ref::<String>()
            .map(std::string::String::deref);

        let cause = cause.unwrap_or_else(|| {
            panic_info
                .payload()
                .downcast_ref::<&str>()
                .copied()
                .unwrap_or("<cause unknown>")
        });
        let bt = Backtrace::new();
        let bt_data = format!("{bt:?}");
        error!(
            sl!(),
            "A panic occurred at {}:{}: {}\r\n{:?}", filename, line, cause, bt_data
        );

        // === DEBUG (temporary): also persist panics to a stable file on
        // disk so that we capture them even when slog isn't usable
        // (early-init failures) or when journald is misconfigured.
        let _ = std::fs::create_dir_all("/var/log/kata-shim");
        if let Ok(mut f) = OpenOptions::new()
            .create(true)
            .append(true)
            .open("/var/log/kata-shim/panics.log")
        {
            let ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs_f64())
                .unwrap_or(0.0);
            let pid = std::process::id();
            let tid = std::thread::current().id();
            let tname = std::thread::current()
                .name()
                .unwrap_or("<unnamed>")
                .to_string();
            let _ = writeln!(
                f,
                "[{:.3}] pid={} tid={:?} tname={} panic at {}:{}: {}",
                ts, pid, tid, tname, filename, line, cause
            );
            let _ = writeln!(f, "{bt_data}");
            let _ = writeln!(f, "----");
            let _ = f.flush();
        }

        // print panic log to dmesg
        // The panic log size is too large to /dev/kmsg, so write by line.
        if let Ok(mut file) = OpenOptions::new().write(true).open(KMESG_DEVICE) {
            file.write_all(format!("A panic occurred at {filename}:{line}: {cause}").as_bytes())
                .ok();
            let lines: Vec<&str> = bt_data.split('\n').collect();
            for line in lines {
                file.write_all(line.as_bytes()).ok();
            }

            file.flush().ok();
        }
        std::process::abort();
    }));
}
