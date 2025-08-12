extern crate libc;

use super::daemon::*;
use std::io::Error;
use std::sync::mpsc::channel;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::Sender;

declare_singleton!(
    singleton,
    DaemonHolder,
    DaemonHolder {
        holder: std::ptr::null_mut::<DaemonStatic>()
    }
);

struct DaemonHolder {
    holder: *mut DaemonStatic,
}

struct DaemonStatic {
    holder: Box<dyn DaemonFunc>,
}

trait DaemonFunc {
    fn exec(&mut self) -> Result<(), Error>;
    fn send(&mut self, state: State) -> Result<(), Error>;
    fn take_tx(&mut self) -> Option<Sender<State>>;
}

struct DaemonFuncHolder<F: FnOnce(Receiver<State>)> {
    tx: Option<Sender<State>>,
    func: Option<(F, Receiver<State>)>,
}

impl<F: FnOnce(Receiver<State>)> DaemonFunc for DaemonFuncHolder<F> {
    fn exec(&mut self) -> Result<(), Error> {
        match self.func.take() {
            Some((func, rx)) => {
                func(rx);
                Ok(())
            }
            None => Err(Error::other("INTERNAL ERROR: Can't unwrap daemon function")),
        }
    }

    fn send(&mut self, state: State) -> Result<(), Error> {
        match self.tx {
            Some(ref tx) => match tx.send(state) {
                Ok(_) => Ok(()),
                Err(e) => Err(Error::other(e)),
            },
            None => Err(Error::other("Service is already exited")),
        }
    }

    fn take_tx(&mut self) -> Option<Sender<State>> {
        self.tx.take()
    }
}

fn daemon_wrapper<R, F: FnOnce(&mut DaemonHolder) -> R>(func: F) -> R {
    let singleton = singleton();
    let result = match singleton.lock() {
        Ok(ref mut daemon) => func(daemon),
        Err(e) => {
            panic!("Mutex error: {:?}", e);
        }
    };
    result
}

impl DaemonRunner for Daemon {
    fn run<F: 'static + FnOnce(Receiver<State>)>(&self, func: F) -> Result<(), Error> {
        let (tx, rx) = channel();
        tx.send(State::Start).unwrap();
        let mut daemon = DaemonStatic {
            holder: Box::new(DaemonFuncHolder {
                tx: Some(tx),
                func: Some((func, rx)),
            }),
        };
        guard_compare_and_swap(daemon_null(), &mut daemon)?;
        let result = daemon_console(&mut daemon);
        guard_compare_and_swap(&mut daemon, daemon_null())?;
        result
    }
}

fn guard_compare_and_swap(
    old_value: *mut DaemonStatic,
    new_value: *mut DaemonStatic,
) -> Result<(), Error> {
    daemon_wrapper(|daemon_static: &mut DaemonHolder| -> Result<(), Error> {
        if daemon_static.holder != old_value {
            return Err(Error::other("This function is not reentrant."));
        }
        daemon_static.holder = new_value;
        Ok(())
    })
}

fn daemon_console(daemon: &mut DaemonStatic) -> Result<(), Error> {
    daemon.holder.exec()
}

fn daemon_null() -> *mut DaemonStatic {
    std::ptr::null_mut::<DaemonStatic>()
}
