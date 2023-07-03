use std::thread;
use std::sync::mpsc::{self, Sender, Receiver};
use std::sync::Arc;
use std::sync::Mutex;
use serde_json::error;
use std::process::exit;
use log::debug;

use crate::{Result, ThreadPool};

pub struct SharedQueueThreadPool {
    _worker_num: u32,
    producer: Sender<Box<dyn FnOnce() + Send + 'static>>,
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(worker_num: u32) -> Result<SharedQueueThreadPool> {
        let (producer, consumer) = mpsc::channel();
        let consumer = Arc::new(Mutex::new(consumer));
        for _ in 0..worker_num {
            let n_consumer = Arc::clone(&consumer);
            thread::spawn(move||{
                worker_loop(n_consumer);
            });
        }
        Ok(SharedQueueThreadPool {
            _worker_num: worker_num,
            producer,
        })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let handle = Box::new(job);
        self.producer.send(handle).unwrap();
    }
}

fn worker_loop(consumer: Arc<Mutex<Receiver<Box<dyn FnOnce() + Send + 'static>>>>) {
    loop {
        match consumer.lock().unwrap().recv() {
            Ok(job) => {
                job();
            }
            Err(_) => {
                debug!("Error fetching jobs");
                exit(0);
            }
        }
    }
}
