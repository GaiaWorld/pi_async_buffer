use std::pin::Pin;
use std::alloc::Global;
use std::cell::UnsafeCell;
use std::io::{Error, ErrorKind};
use std::task::{Poll, Context, Waker};
use std::sync::{Arc, atomic::{AtomicBool, AtomicU8, Ordering}};

use futures::{stream::{Stream, FusedStream, StreamExt},
              sink::Sink};
use crossbeam_queue::ArrayQueue;

use pi_async_rt::lock::spin;

///
/// 允许跨线程的动态异步发送器
/// 动态异步发送器的发送不要求线程安全，所以如果动态异步发送器同时在多个线程中发送，则需要由外部保证动态异步发送器的线程安全
///
pub type BoxSender<'a, T, E = Error> = Pin<Box<dyn AsyncSender<T, E> + Send + 'a, Global>>;

///
/// 异步发送器
///
pub trait AsyncSender<T, Err = Error>: Sink<T, Error = Err> {
    /// 获取已发送未接收的队列长度
    fn current_len(&self) -> Option<usize> {
        None
    }
}

///
/// 扩展的异步发送器
///
pub trait AsyncSenderExt<T>: AsyncSender<T> {
    /// 封装AsyncSender为Box，并将它固定
    fn pin_boxed<'a>(self) -> BoxSender<'a, T>
        where Self: Sized + Send + 'a {
        Box::pin(self)
    }
}

impl<T, U> AsyncSenderExt<U> for T where T: AsyncSender<U> {}

pub struct PipeSender<T> {
    is_terminated:  Arc<AtomicBool>,                //是否已终止管道
    queue:          Arc<ArrayQueue<T>>,             //发送队列
    waker:          Arc<UnsafeCell<Option<Waker>>>, //发送器的唤醒器
    status:         Arc<AtomicU8>,                  //发送器的状态
}

unsafe impl<T> Send for PipeSender<T> {}
unsafe impl<T> Sync for PipeSender<T> {}

impl<T> Sink<T> for PipeSender<T> {
    type Error = Error;

    // 用户检查写流是否已满，如果当前写流已满，则异步等待对端读流消耗了写流的帧以后，再唤醒异步等待
    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self
            .is_terminated
            .load(Ordering::Acquire) {
            //当前的写流已终止
            return Poll::Ready(Err(Error::new(ErrorKind::BrokenPipe, format!("Poll ready failed, reason: pipeline already terminated"))));
        }

        if self.queue.is_full() {
            //当前的写流已满
            let mut spin_len = 1;
            loop {
                match self.status.compare_exchange(0,
                                                   1,
                                                   Ordering::AcqRel,
                                                   Ordering::Relaxed) {
                    Err(current) => {
                        //对端读流或当前写流已经在异步等待唤醒，或者已被非当前调用锁住，则尝试指定次数的内部重试，重试失败则立即返回失败，但允许外部重试
                        if spin_len >= 3 {
                            //内部重试次数已达限制，则立即返回失败
                            return Poll::Ready(Err(Error::new(ErrorKind::WouldBlock, format!("Poll ready failed, current: {}, reason: pipeline busy", current))));
                        }

                        //内部重试次数未达限制，则休眠后继续内部重试
                        spin_len = spin(spin_len);
                        continue;
                    },
                    Ok(_) => {
                        //对端读流或当前写流未异步等待唤醒，且已被当前调用锁住
                        unsafe {
                            *self.waker.get() = Some(cx.waker().clone()); //设置写流的唤醒器
                        }
                        self.status.store(3, Ordering::Release); //设置等待写流未满的唤醒器状态为已就绪

                        return Poll::Pending;
                    },
                }
            }
        } else {
            //当前的写流未满，则立即返回写流已就绪
            //对端读流或当前写流即使已经在异步等待唤醒，也不会唤醒正在等待唤醒的对端读流或当前写流
            Poll::Ready(Ok(()))
        }
    }

    // 同步向写流写入帧，外部调用必须保证在每次调用start_send前调用poll_ready且返回成功
    fn start_send(self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
        if self
            .is_terminated
            .load(Ordering::Acquire) {
            //当前的写流已终止
            return Err(Error::new(ErrorKind::BrokenPipe, format!("Start send failed, reason: pipeline already terminated")));
        }

        if let Err(_) = self.queue.push(item) {
            //写流已满，则立即返回失败，但允许重试
            return Err(Error::new(ErrorKind::WouldBlock, format!("Start send failed, reason: pipeline already full")));
        }

        Ok(())
    }

    // 刷新只会唤醒对端读流的异步等待，即通知对端读流有新帧，刷新不会异步等待唤醒
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self
            .is_terminated
            .load(Ordering::Acquire) {
            //当前的写流已终止
            return Poll::Ready(Err(Error::new(ErrorKind::BrokenPipe, format!("Poll flush failed, reason: pipelinne already terminated"))));
        }

        let mut spin_len = 1;
        loop {
            match self.status.compare_exchange(0,
                                               1,
                                               Ordering::AcqRel,
                                               Ordering::Relaxed) {
                Err(1) => {
                    //已被非当前调用锁住，则尝试指定次数的内部重试，重试失败则立即返回失败，但允许外部重试
                    if spin_len >= 3 {
                        //内部重试次数已达限制，则立即返回失败
                        return Poll::Ready(Err(Error::new(ErrorKind::WouldBlock, format!("Poll flush failed, current: 1, reason: pipeline busy"))));
                    }

                    //内部重试次数未达限制，则休眠后继续内部重试
                    spin_len = spin(spin_len);
                    continue;
                },
                Err(2) => {
                    //对端读流已经在异步等待唤醒，则立即唤醒对端的读流，并返回刷新成功
                    unsafe {
                        if let Some(waker) = (*self.waker.get()).take() {
                            //对端设置的唤醒器存在，则立即唤醒对端的读流
                            waker.wake();
                            self.status.store(0, Ordering::Release); //设置对端读流和当前写流的状态为已初始化
                        }
                    }

                    return Poll::Ready(Ok(()));
                },
                Err(3) => {
                    //当前写流已经在异步等待唤醒，则忽略刷新，并立即返回刷新成功
                    return Poll::Ready(Ok(()));
                },
                _ => {
                    //已被当前调用锁住，则忽略刷新，并立即返回刷新成功
                    self.status.store(0, Ordering::Release); //设置对端读流和当前写流的状态为已初始化
                    return Poll::Ready(Ok(()));
                },
            }
        }
    }

    // 终止当前写流，在终止前会尝试刷新一次写流，并唤醒对端读流或当前写流的任何异步等待
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self
            .is_terminated
            .load(Ordering::Acquire) {
            //当前的写流已终止
            return Poll::Ready(Ok(()));
        }

        let mut spin_len = 1;
        loop {
            match self.status.compare_exchange(0,
                                               1,
                                               Ordering::AcqRel,
                                               Ordering::Relaxed) {
                Err(1) => {
                    //已被非当前调用锁住，则尝试指定次数的内部重试，重试失败则立即返回失败，但允许外部重试
                    if spin_len >= 3 {
                        //内部重试次数已达限制，则立即返回失败
                        return Poll::Ready(Err(Error::new(ErrorKind::WouldBlock, format!("Poll close failed, current: 1, reason: pipeline busy"))));
                    }

                    //内部重试次数未达限制，则休眠后继续内部重试
                    spin_len = spin(spin_len);
                    continue;
                },
                Err(_) => {
                    //对端读流或当前写流已经在异步等待唤醒，则立即终止对端读流和当前写流，然后唤醒对端读流或当前写流，并立即返回成功
                    //对端需要从对端读流获取所有剩余帧后，再终止对端读流
                    let _ = self
                        .is_terminated
                        .compare_exchange(false,
                                          true,
                                          Ordering::AcqRel,
                                          Ordering::Relaxed);

                    unsafe {
                        if let Some(waker) = (*self.waker.get()).take() {
                            //对端设置的唤醒器存在，则立即唤醒对端的读流
                            waker.wake();
                            self.status.store(0, Ordering::Release); //设置对端读流和当前写流的状态为已初始化
                        }
                    }

                    return Poll::Ready(Ok(()));
                },
                Ok(_) => {
                    //已被当前调用锁住，则立即终止对端读流和当前写流，并立即返回成功
                    let _ = self
                        .is_terminated
                        .compare_exchange(false,
                                          true,
                                          Ordering::AcqRel,
                                          Ordering::Relaxed);
                    self.status.store(0, Ordering::Release); //设置对端读流和当前写流的状态为已初始化

                    return Poll::Ready(Ok(()));
                },
            }
        }
    }
}

impl<T> AsyncSender<T> for PipeSender<T> {
    fn current_len(&self) -> Option<usize> {
        Some(self.queue.len())
    }
}

///
/// 允许跨线程的动态异步接收器
/// 动态异步接收器的接收不要求线程安全，所以如果动态异步接收器同时在多个线程中接收，则需要由外部保证动态异步接收器的线程安全
///
pub type BoxReceiver<'a, T> = Pin<Box<dyn AsyncReceiver<T> + Send + 'a, Global>>;

///
/// 异步接收器
///
pub trait AsyncReceiver<T>: Stream<Item = T> + FusedStream {
    /// 获取已发送未接收的队列长度
    fn current_len(&self) -> Option<usize> {
        None
    }
}

///
/// 扩展的异步接收器
///
pub trait AsyncReceiverExt<T>: AsyncReceiver<T> {
    /// 封装AsyncReceiver为Box，并将它固定
    fn pin_boxed<'a>(self) -> BoxReceiver<'a, T>
        where Self: Sized + Send + 'a {
        Box::pin(self)
    }
}

impl<T, U> AsyncReceiverExt<U> for T where T: AsyncReceiver<U> {}

pub struct PipeReceiver<T> {
    is_terminated:  Arc<AtomicBool>,                //是否已终止管道
    queue:          Arc<ArrayQueue<T>>,             //接收队列
    waker:          Arc<UnsafeCell<Option<Waker>>>, //接收器的唤醒器
    status:         Arc<AtomicU8>,                  //接收器的状态
}

unsafe impl<T> Send for PipeReceiver<T> {}
unsafe impl<T> Sync for PipeReceiver<T> {}

impl<T> Stream for PipeReceiver<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.is_terminated() {
            //当前的接收器已终止
            return Poll::Ready(None);
        }

        loop {
            if let Some(frame) = self.queue.pop() {
                //当前的读流有数据
                return Poll::Ready(Some(frame));
            } else {
                //当前的读流无数据
                let mut spin_len = 1;
                match self.status.compare_exchange(0,
                                                   1,
                                                   Ordering::AcqRel,
                                                   Ordering::Relaxed) {
                    Err(3) => {
                        //对端写流已经在异步等待唤醒，则立即唤醒对端写流，并继续内部重试
                        unsafe {
                            if let Some(waker) = (*self.waker.get()).take() {
                                //对端设置的唤醒器存在，则立即唤醒对端的写流
                                waker.wake();
                                self.status.store(0, Ordering::Release); //设置对端写流和当前读流的状态为已初始化
                            }
                        }
                        continue;
                    },
                    Err(_) => {
                        //当前读流已经在异步等待唤醒，或者已被非当前调用锁住，则休眠后继续内部重试
                        spin_len = spin(spin_len);
                        continue;
                    },
                    Ok(_) => {
                        //对端写流或当前读流未异步等待唤醒，且已被当前调用锁住
                        unsafe {
                            *self.waker.get() = Some(cx.waker().clone()); //设置读流的唤醒器
                        }
                        self.status.store(2, Ordering::Release); //设置对端写流和当前读流的唤醒器状态为已就绪

                        return Poll::Pending;
                    },
                }
            }
        }
    }
}

impl<T> FusedStream for PipeReceiver<T> {
    // 判断接收器是否真的已终止
    #[inline]
    fn is_terminated(&self) -> bool {
        if self
            .is_terminated
            .load(Ordering::Acquire) {
            //读流已终止，则继续判断当前读流是否为空
            self.queue.is_empty()
        } else {
            //读流未终止
            false
        }
    }
}

impl<T> AsyncReceiver<T> for PipeReceiver<T> {
    fn current_len(&self) -> Option<usize> {
        Some(self.queue.len())
    }
}

///
/// 创建一对指定容量的异步通道
///
pub fn channel<T>(capacity: usize) -> (PipeSender<T>, PipeReceiver<T>) {
    let is_terminated = Arc::new(AtomicBool::new(false));
    let queue = Arc::new(ArrayQueue::new(capacity));
    let waker = Arc::new(UnsafeCell::new(None));
    let status = Arc::new(AtomicU8::new(0));

    let sender = PipeSender {
        is_terminated: is_terminated.clone(),
        queue: queue.clone(),
        waker: waker.clone(),
        status: status.clone(),
    };

    let receiver = PipeReceiver {
        is_terminated,
        queue,
        waker,
        status,
    };

    (sender, receiver)
}

///
/// 允许跨线程的动态异步管道
/// 动态异步管道的获取和写入不要求线程安全，所以如果动态异步管道同时在多个线程中处理获取和写入，则需要由外部保证动态异步管道的线程安全
///
pub type BoxPipeline<'a, T, U = T, E = Error> = Pin<Box<dyn AsyncPipeLine<T, U, E> + Send + 'a, Global>>;

///
/// 异步管道，可以从异步管道中获取帧，也可以向异步管道中写入帧
///
pub trait AsyncPipeLine<
    StreamFrame,
    SinkFrame = StreamFrame,
    Err = Error
>: Sink<SinkFrame, Error = Err> + Stream<Item = StreamFrame> + FusedStream {}

///
/// 扩展的异步管道
///
pub trait AsyncPipeLineExt<
    StreamFrame,
    SinkFrame = StreamFrame,
    Err = Error
>: AsyncPipeLine<StreamFrame, SinkFrame, Err> {
    /// 封装AsyncPipeLine为Box，并将它固定
    fn pin_boxed<'a>(self) -> BoxPipeline<'a, StreamFrame, SinkFrame, Err>
        where Self: Sized + Send + 'a {
        Box::pin(self)
    }
}

impl<
    StreamFrame,
    SinkFrame,
    Err,
    T: ?Sized,
> AsyncPipeLineExt<StreamFrame, SinkFrame, Err> for T where T: AsyncPipeLine<StreamFrame, SinkFrame, Err> {}

///
/// 创建一对指定容量的异步管道
///
pub fn pipeline<T, U>(capacity: usize) -> (AsyncDownStream<T, U>, AsyncUpStream<U, T>) {
    let is_terminated_down_stream = Arc::new(AtomicBool::new(false));
    let down_stream = Arc::new(ArrayQueue::new(capacity));
    let down_stream_waker = Arc::new(UnsafeCell::new(None));
    let down_stream_status = Arc::new(AtomicU8::new(0));
    let is_terminated_down_sink = Arc::new(AtomicBool::new(false));
    let down_sink = Arc::new(ArrayQueue::new(capacity));
    let down_sink_waker = Arc::new(UnsafeCell::new(None));
    let down_sink_status = Arc::new(AtomicU8::new(0));
    let down_inner = InnerAsyncFlow {
        is_terminated_stream: is_terminated_down_stream.clone(),
        stream: down_stream.clone(),
        stream_waker: down_stream_waker.clone(),
        stream_status: down_stream_status.clone(),
        is_terminated_sink: is_terminated_down_sink.clone(),
        sink: down_sink.clone(),
        sink_waker: down_sink_waker.clone(),
        sink_status: down_sink_status.clone(),
    };
    let async_down_stream = AsyncDownStream(Arc::new(down_inner));

    let is_terminated_stream = is_terminated_down_sink;
    let up_stream = down_sink;
    let stream_waker = down_sink_waker;
    let stream_status = down_sink_status;
    let is_terminated_sink = is_terminated_down_stream;
    let up_sink = down_stream;
    let sink_waker = down_stream_waker;
    let sink_status = down_stream_status;
    let up_inner = InnerAsyncFlow {
        is_terminated_stream,
        stream: up_stream,
        stream_waker,
        stream_status,
        is_terminated_sink,
        sink: up_sink,
        sink_waker,
        sink_status,
    };
    let async_up_stream = AsyncUpStream(Arc::new(up_inner));

    (async_down_stream, async_up_stream)
}

///
/// 线程安全的异步下游
///
pub struct AsyncDownStream<T, U = T>(Arc<InnerAsyncFlow<T, U>>);

unsafe impl<T, U> Send for AsyncDownStream<T, U> {}
unsafe impl<T, U> Sync for AsyncDownStream<T, U> {}

impl<T, U> Clone for AsyncDownStream<T, U> {
    fn clone(&self) -> Self {
        AsyncDownStream(self.0.clone())
    }
}

impl<T, U> Stream for AsyncDownStream<T, U> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.is_terminated() {
            //当前的读流已终止
            return Poll::Ready(None);
        }

        loop {
            if let Some(frame) = self.0.stream.pop() {
                //当前的读流有数据
                return Poll::Ready(Some(frame));
            } else {
                //当前的读流无数据
                let mut spin_len = 1;
                match self.0.stream_status.compare_exchange(0,
                                                            1,
                                                            Ordering::AcqRel,
                                                            Ordering::Relaxed) {
                    Err(3) => {
                        //对端写流已经在异步等待唤醒，则立即唤醒对端写流，并继续内部重试
                        unsafe {
                            if let Some(waker) = (*self.0.stream_waker.get()).take() {
                                //对端设置的唤醒器存在，则立即唤醒对端的写流
                                waker.wake();
                                self.0.stream_status.store(0, Ordering::Release); //设置对端写流和当前读流的状态为已初始化
                            }
                        }
                        continue;
                    },
                    Err(_) => {
                        //当前读流已经在异步等待唤醒，或者已被非当前调用锁住，则休眠后继续内部重试
                        spin_len = spin(spin_len);
                        continue;
                    },
                    Ok(_) => {
                        //对端写流或当前读流未异步等待唤醒，且已被当前调用锁住
                        unsafe {
                            *self.0.stream_waker.get() = Some(cx.waker().clone()); //设置读流的唤醒器
                        }
                        self.0.stream_status.store(2, Ordering::Release); //设置对端写流和当前读流的唤醒器状态为已就绪

                        return Poll::Pending;
                    },
                }
            }
        }
    }
}

impl<T, U> FusedStream for AsyncDownStream<T, U> {
    // 判断读流是否真的已终止
    #[inline]
    fn is_terminated(&self) -> bool {
        if self.
            0
            .is_terminated_stream
            .load(Ordering::Acquire) {
            //读流已终止，则继续判断当前读流是否为空
            self.0.stream.is_empty()
        } else {
            //读流未终止
            false
        }
    }
}

impl<T, U> Sink<U> for AsyncDownStream<T, U> {
    type Error = Error;

    // 用户检查写流是否已满，如果当前写流已满，则异步等待对端读流消耗了写流的帧以后，再唤醒异步等待
    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self
            .0
            .is_terminated_sink
            .load(Ordering::Acquire) {
            //当前的写流已终止
            return Poll::Ready(Err(Error::new(ErrorKind::BrokenPipe, format!("Poll ready failed, reason: sink already terminated"))));
        }

        if self.0.sink.is_full() {
            //当前的写流已满
            let mut spin_len = 1;
            loop {
                match self.0.sink_status.compare_exchange(0,
                                                          1,
                                                          Ordering::AcqRel,
                                                          Ordering::Relaxed) {
                    Err(current) => {
                        //对端读流或当前写流已经在异步等待唤醒，或者已被非当前调用锁住，则尝试指定次数的内部重试，重试失败则立即返回失败，但允许外部重试
                        if spin_len >= 3 {
                            //内部重试次数已达限制，则立即返回失败
                            return Poll::Ready(Err(Error::new(ErrorKind::WouldBlock, format!("Poll ready failed, current: {}, reason: sink or peer busy", current))));
                        }

                        //内部重试次数未达限制，则休眠后继续内部重试
                        spin_len = spin(spin_len);
                        continue;
                    },
                    Ok(_) => {
                        //对端读流或当前写流未异步等待唤醒，且已被当前调用锁住
                        unsafe {
                            *self.0.sink_waker.get() = Some(cx.waker().clone()); //设置写流的唤醒器
                        }
                        self.0.sink_status.store(3, Ordering::Release); //设置等待写流未满的唤醒器状态为已就绪

                        return Poll::Pending;
                    },
                }
            }
        } else {
            //当前的写流未满，则立即返回写流已就绪
            //对端读流或当前写流即使已经在异步等待唤醒，也不会唤醒正在等待唤醒的对端读流或当前写流
            Poll::Ready(Ok(()))
        }
    }

    // 同步向写流写入帧，外部调用必须保证在每次调用start_send前调用poll_ready且返回成功
    fn start_send(self: Pin<&mut Self>, item: U) -> Result<(), Self::Error> {
        if self
            .0
            .is_terminated_sink
            .load(Ordering::Acquire) {
            //当前的写流已终止
            return Err(Error::new(ErrorKind::BrokenPipe, format!("Start send failed, reason: sink already terminated")));
        }

        if let Err(_) = self.0.sink.push(item) {
            //写流已满，则立即返回失败，但允许重试
            return Err(Error::new(ErrorKind::WouldBlock, format!("Start send failed, reason: buffer already full")));
        }

        Ok(())
    }

    // 刷新只会唤醒对端读流的异步等待，即通知对端读流有新帧，刷新不会异步等待唤醒
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self
            .0
            .is_terminated_sink
            .load(Ordering::Acquire) {
            //当前的写流已终止
            return Poll::Ready(Err(Error::new(ErrorKind::BrokenPipe, format!("Poll flush failed, reason: sink already terminated"))));
        }

        let mut spin_len = 1;
        loop {
            match self.0.sink_status.compare_exchange(0,
                                                      1,
                                                      Ordering::AcqRel,
                                                      Ordering::Relaxed) {
                Err(1) => {
                    //已被非当前调用锁住，则尝试指定次数的内部重试，重试失败则立即返回失败，但允许外部重试
                    if spin_len >= 3 {
                        //内部重试次数已达限制，则立即返回失败
                        return Poll::Ready(Err(Error::new(ErrorKind::WouldBlock, format!("Poll flush failed, current: 1, reason: sink or peer busy"))));
                    }

                    //内部重试次数未达限制，则休眠后继续内部重试
                    spin_len = spin(spin_len);
                    continue;
                },
                Err(2) => {
                    //对端读流已经在异步等待唤醒，则立即唤醒对端的读流，并返回刷新成功
                    unsafe {
                        if let Some(waker) = (*self.0.sink_waker.get()).take() {
                            //对端设置的唤醒器存在，则立即唤醒对端的读流
                            waker.wake();
                            self.0.sink_status.store(0, Ordering::Release); //设置对端读流和当前写流的状态为已初始化
                        }
                    }

                    return Poll::Ready(Ok(()));
                },
                Err(3) => {
                    //当前写流已经在异步等待唤醒，则忽略刷新，并立即返回刷新成功
                    return Poll::Ready(Ok(()));
                },
                _ => {
                    //已被当前调用锁住，则忽略刷新，并立即返回刷新成功
                    self.0.sink_status.store(0, Ordering::Release); //设置对端读流和当前写流的状态为已初始化
                    return Poll::Ready(Ok(()));
                },
            }
        }
    }

    // 终止当前写流，在终止前会尝试刷新一次写流，并唤醒对端读流或当前写流的任何异步等待
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self
            .0
            .is_terminated_sink
            .load(Ordering::Acquire) {
            //当前的写流已终止
            return Poll::Ready(Ok(()));
        }

        let mut spin_len = 1;
        loop {
            match self.0.sink_status.compare_exchange(0,
                                                      1,
                                                      Ordering::AcqRel,
                                                      Ordering::Relaxed) {
                Err(1) => {
                    //已被非当前调用锁住，则尝试指定次数的内部重试，重试失败则立即返回失败，但允许外部重试
                    if spin_len >= 3 {
                        //内部重试次数已达限制，则立即返回失败
                        return Poll::Ready(Err(Error::new(ErrorKind::WouldBlock, format!("Poll close failed, current: 1, reason: sink or peer busy"))));
                    }

                    //内部重试次数未达限制，则休眠后继续内部重试
                    spin_len = spin(spin_len);
                    continue;
                },
                Err(_) => {
                    //对端读流或当前写流已经在异步等待唤醒，则立即终止对端读流和当前写流，然后唤醒对端读流或当前写流，并立即返回成功
                    //对端需要从对端读流获取所有剩余帧后，再终止对端读流
                    let _ = self
                        .0
                        .is_terminated_sink
                        .compare_exchange(false,
                                          true,
                                          Ordering::AcqRel,
                                          Ordering::Relaxed);

                    unsafe {
                        if let Some(waker) = (*self.0.sink_waker.get()).take() {
                            //对端设置的唤醒器存在，则立即唤醒对端的读流
                            waker.wake();
                            self.0.sink_status.store(0, Ordering::Release); //设置对端读流和当前写流的状态为已初始化
                        }
                    }

                    return Poll::Ready(Ok(()));
                },
                Ok(_) => {
                    //已被当前调用锁住，则立即终止对端读流和当前写流，并立即返回成功
                    let _ = self
                        .0
                        .is_terminated_sink
                        .compare_exchange(false,
                                          true,
                                          Ordering::AcqRel,
                                          Ordering::Relaxed);
                    self.0.sink_status.store(0, Ordering::Release); //设置对端读流和当前写流的状态为已初始化

                    return Poll::Ready(Ok(()));
                },
            }
        }
    }
}

impl<T, U> AsyncPipeLine<T, U> for AsyncDownStream<T, U> {}

///
/// 线程安全的异步上游
///
pub struct AsyncUpStream<U, T = U>(Arc<InnerAsyncFlow<U, T>>);

unsafe impl<U, T> Send for AsyncUpStream<U, T> {}
unsafe impl<U, T> Sync for AsyncUpStream<U, T> {}

impl<U, T> Clone for AsyncUpStream<U, T> {
    fn clone(&self) -> Self {
        AsyncUpStream(self.0.clone())
    }
}

impl<U, T> Stream for AsyncUpStream<U, T> {
    type Item = U;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.is_terminated() {
            //当前的读流已终止
            return Poll::Ready(None);
        }

        loop {
            if let Some(frame) = self.0.stream.pop() {
                //当前的读流有数据
                return Poll::Ready(Some(frame));
            } else {
                //当前的读流无数据
                let mut spin_len = 1;
                match self.0.stream_status.compare_exchange(0,
                                                            1,
                                                            Ordering::AcqRel,
                                                            Ordering::Relaxed) {
                    Err(3) => {
                        //对端写流已经在异步等待唤醒，则立即唤醒对端写流，并继续内部重试
                        unsafe {
                            if let Some(waker) = (*self.0.stream_waker.get()).take() {
                                //对端设置的唤醒器存在，则立即唤醒对端的写流
                                waker.wake();
                                self.0.stream_status.store(0, Ordering::Release); //设置对端写流和当前读流的状态为已初始化
                            }
                        }
                        continue;
                    },
                    Err(_) => {
                        //当前读流已经在异步等待唤醒，或者已被非当前调用锁住，则休眠后继续内部重试
                        spin_len = spin(spin_len);
                        continue;
                    },
                    Ok(_) => {
                        //对端写流或当前读流未异步等待唤醒，且已被当前调用锁住
                        unsafe {
                            *self.0.stream_waker.get() = Some(cx.waker().clone()); //设置读流的唤醒器
                        }
                        self.0.stream_status.store(2, Ordering::Release); //设置对端写流和当前读流的唤醒器状态为已就绪

                        return Poll::Pending;
                    },
                }
            }
        }
    }
}

impl<T, U> FusedStream for AsyncUpStream<U, T> {
    // 判断读流是否真的已终止
    #[inline]
    fn is_terminated(&self) -> bool {
        if self.
            0
            .is_terminated_stream
            .load(Ordering::Acquire) {
            //读流已终止，则继续判断当前读流是否为空
            self.0.stream.is_empty()
        } else {
            //读流未终止
            false
        }
    }
}

impl<U, T> Sink<T> for AsyncUpStream<U, T> {
    type Error = Error;

    // 用户检查写流是否已满，如果当前写流已满，则异步等待对端读流消耗了写流的帧以后，再唤醒异步等待
    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self
            .0
            .is_terminated_sink
            .load(Ordering::Acquire) {
            //当前的写流已终止
            return Poll::Ready(Err(Error::new(ErrorKind::BrokenPipe, format!("Poll ready failed, reason: sink already terminated"))));
        }

        if self.0.sink.is_full() {
            //当前的写流已满
            let mut spin_len = 1;
            loop {
                match self.0.sink_status.compare_exchange(0,
                                                          1,
                                                          Ordering::AcqRel,
                                                          Ordering::Relaxed) {
                    Err(current) => {
                        //对端读流或当前写流已经在异步等待唤醒，或者已被非当前调用锁住，则尝试指定次数的内部重试，重试失败则立即返回失败，但允许外部重试
                        if spin_len >= 3 {
                            //内部重试次数已达限制，则立即返回失败
                            return Poll::Ready(Err(Error::new(ErrorKind::WouldBlock, format!("Poll ready failed, current: {}, reason: sink or peer busy", current))));
                        }

                        //内部重试次数未达限制，则休眠后继续内部重试
                        spin_len = spin(spin_len);
                        continue;
                    },
                    Ok(_) => {
                        //对端读流或当前写流未异步等待唤醒，且已被当前调用锁住
                        unsafe {
                            *self.0.sink_waker.get() = Some(cx.waker().clone()); //设置写流的唤醒器
                        }
                        self.0.sink_status.store(3, Ordering::Release); //设置等待写流未满的唤醒器状态为已就绪

                        return Poll::Pending;
                    },
                }
            }
        } else {
            //当前的写流未满，则立即返回写流已就绪
            //对端读流或当前写流即使已经在异步等待唤醒，也不会唤醒正在等待唤醒的对端读流或当前写流
            Poll::Ready(Ok(()))
        }
    }

    // 同步向写流写入帧，外部调用必须保证在每次调用start_send前调用poll_ready且返回成功
    fn start_send(self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
        if self
            .0
            .is_terminated_sink
            .load(Ordering::Acquire) {
            //当前的写流已终止
            return Err(Error::new(ErrorKind::BrokenPipe, format!("Start send failed, reason: sink already terminated")));
        }

        if let Err(_) = self.0.sink.push(item) {
            //写流已满，则立即返回失败，但允许重试
            return Err(Error::new(ErrorKind::WouldBlock, format!("Start send failed, reason: buffer already full")));
        }

        Ok(())
    }

    // 刷新只会唤醒对端读流的异步等待，即通知对端读流有新帧，刷新不会异步等待唤醒
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self
            .0
            .is_terminated_sink
            .load(Ordering::Acquire) {
            //当前的写流已终止
            return Poll::Ready(Err(Error::new(ErrorKind::BrokenPipe, format!("Poll flush failed, reason: sink already terminated"))));
        }

        let mut spin_len = 1;
        loop {
            match self.0.sink_status.compare_exchange(0,
                                                      1,
                                                      Ordering::AcqRel,
                                                      Ordering::Relaxed) {
                Err(1) => {
                    //已被非当前调用锁住，则尝试指定次数的内部重试，重试失败则立即返回失败，但允许外部重试
                    if spin_len >= 3 {
                        //内部重试次数已达限制，则立即返回失败
                        return Poll::Ready(Err(Error::new(ErrorKind::WouldBlock, format!("Poll flush failed, current: 1, reason: sink or peer busy"))));
                    }

                    //内部重试次数未达限制，则休眠后继续内部重试
                    spin_len = spin(spin_len);
                    continue;
                },
                Err(2) => {
                    //对端读流已经在异步等待唤醒，则立即唤醒对端的读流，并返回刷新成功
                    unsafe {
                        if let Some(waker) = (*self.0.sink_waker.get()).take() {
                            //对端设置的唤醒器存在，则立即唤醒对端的读流
                            waker.wake();
                            self.0.sink_status.store(0, Ordering::Release); //设置对端读流和当前写流的状态为已初始化
                        }
                    }

                    return Poll::Ready(Ok(()));
                },
                Err(3) => {
                    //当前写流已经在异步等待唤醒，则忽略刷新，并立即返回刷新成功
                    return Poll::Ready(Ok(()));
                },
                _ => {
                    //已被当前调用锁住，则忽略刷新，并立即返回刷新成功
                    self.0.sink_status.store(0, Ordering::Release); //设置对端读流和当前写流的状态为已初始化
                    return Poll::Ready(Ok(()));
                },
            }
        }
    }

    // 终止当前写流，在终止前会尝试刷新一次写流，并唤醒对端读流或当前写流的任何异步等待
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self
            .0
            .is_terminated_sink
            .load(Ordering::Acquire) {
            //当前的写流已终止
            return Poll::Ready(Ok(()));
        }

        let mut spin_len = 1;
        loop {
            match self.0.sink_status.compare_exchange(0,
                                                      1,
                                                      Ordering::AcqRel,
                                                      Ordering::Relaxed) {
                Err(1) => {
                    //已被非当前调用锁住，则尝试指定次数的内部重试，重试失败则立即返回失败，但允许外部重试
                    if spin_len >= 3 {
                        //内部重试次数已达限制，则立即返回失败
                        return Poll::Ready(Err(Error::new(ErrorKind::WouldBlock, format!("Poll close failed, current: 1, reason: sink or peer busy"))));
                    }

                    //内部重试次数未达限制，则休眠后继续内部重试
                    spin_len = spin(spin_len);
                    continue;
                },
                Err(_) => {
                    //对端读流或当前写流已经在异步等待唤醒，则立即终止对端读流和当前写流，然后唤醒对端读流或当前写流，并立即返回成功
                    //对端需要从对端读流获取所有剩余帧后，再终止对端读流
                    let _ = self
                        .0
                        .is_terminated_sink
                        .compare_exchange(false,
                                          true,
                                          Ordering::AcqRel,
                                          Ordering::Relaxed);

                    unsafe {
                        if let Some(waker) = (*self.0.sink_waker.get()).take() {
                            //对端设置的唤醒器存在，则立即唤醒对端的读流
                            waker.wake();
                            self.0.sink_status.store(0, Ordering::Release); //设置对端读流和当前写流的状态为已初始化
                        }
                    }

                    return Poll::Ready(Ok(()));
                },
                Ok(_) => {
                    //已被当前调用锁住，则立即终止对端读流和当前写流，并立即返回成功
                    let _ = self
                        .0
                        .is_terminated_sink
                        .compare_exchange(false,
                                          true,
                                          Ordering::AcqRel,
                                          Ordering::Relaxed);
                    self.0.sink_status.store(0, Ordering::Release); //设置对端读流和当前写流的状态为已初始化

                    return Poll::Ready(Ok(()));
                },
            }
        }
    }
}

impl<U, T> AsyncPipeLine<U, T> for AsyncUpStream<U, T> {}

// 内部线程安全的异步流
struct InnerAsyncFlow<X, Y = X> {
    is_terminated_stream:   Arc<AtomicBool>,                //是否已终止读流
    stream:                 Arc<ArrayQueue<X>>,             //读流
    stream_waker:           Arc<UnsafeCell<Option<Waker>>>, //读流唤醒器
    stream_status:          Arc<AtomicU8>,                  //读流的唤醒器状态
    is_terminated_sink:     Arc<AtomicBool>,                //是否已终止写流
    sink:                   Arc<ArrayQueue<Y>>,             //写流
    sink_waker:             Arc<UnsafeCell<Option<Waker>>>, //写流唤醒器
    sink_status:            Arc<AtomicU8>,                  //写流的唤醒器状态
}

