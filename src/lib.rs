#![feature(negative_impls)]
use std::sync::Arc;

use futures::stream::{Stream, FusedStream, StreamExt};
use bytes::{Buf, BufMut, Bytes, BytesMut};

use pi_async::rt::async_pipeline::BoxReceiver;

///
/// 部分缓冲区
///
pub struct PartBuffer(Bytes);

unsafe impl Send for PartBuffer {}
unsafe impl Sync for PartBuffer {}

impl AsRef<[u8]> for PartBuffer {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl PartBuffer {
    /// 判断部分缓冲区是否为空
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// 获取部分缓冲区包含的所有字节长度
    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// 获取部分缓冲区剩余可读字节长度
    #[inline]
    pub fn remaining(&self) -> usize {
        self.0.remaining()
    }

    /// 获取一个i8
    #[inline]
    pub fn get_i8(&mut self) -> i8 {
        self.0.get_i8()
    }

    /// 获取一个u8
    #[inline]
    pub fn get_u8(&mut self) -> u8 {
        self.0.get_u8()
    }

    /// 获取一个大端i16
    #[inline]
    pub fn get_i16(&mut self) -> i16 {
        self.0.get_i16()
    }

    /// 获取一个小端i16
    #[inline]
    pub fn get_i16_le(&mut self) -> i16 {
        self.0.get_i16_le()
    }

    /// 获取一个大端u16
    #[inline]
    pub fn get_u16(&mut self) -> u16 {
        self.0.get_u16()
    }

    /// 获取一个小端u16
    #[inline]
    pub fn get_u16_le(&mut self) -> u16 {
        self.0.get_u16_le()
    }

    /// 获取一个大端i32
    #[inline]
    pub fn get_i32(&mut self) -> i32 {
        self.0.get_i32()
    }

    /// 获取一个小端i32
    #[inline]
    pub fn get_i32_le(&mut self) -> i32 {
        self.0.get_i32_le()
    }

    /// 获取一个大端u32
    #[inline]
    pub fn get_u32(&mut self) -> u32 {
        self.0.get_u32()
    }

    /// 获取一个小端u32
    #[inline]
    pub fn get_u32_le(&mut self) -> u32 {
        self.0.get_u32_le()
    }

    /// 获取一个大端i64
    #[inline]
    pub fn get_i64(&mut self) -> i64 {
        self.0.get_i64()
    }

    /// 获取一个小端i64
    #[inline]
    pub fn get_i64_le(&mut self) -> i64 {
        self.0.get_i64_le()
    }

    /// 获取一个大端u64
    #[inline]
    pub fn get_u64(&mut self) -> u64 {
        self.0.get_u64()
    }

    /// 获取一个小端u64
    #[inline]
    pub fn get_u64_le(&mut self) -> u64 {
        self.0.get_u64_le()
    }

    /// 获取一个大端i128
    #[inline]
    pub fn get_i128(&mut self) -> i128 {
        self.0.get_i128()
    }

    /// 获取一个小端i128
    #[inline]
    pub fn get_i128_le(&mut self) -> i128 {
        self.0.get_i128_le()
    }

    /// 获取一个大端u128
    #[inline]
    pub fn get_u128(&mut self) -> u128 {
        self.0.get_u128()
    }

    /// 获取一个小端u128
    #[inline]
    pub fn get_u128_le(&mut self) -> u128 {
        self.0.get_u128_le()
    }

    /// 获取一个大端isize
    #[inline]
    pub fn get_isize(&mut self) -> isize {
        self.0.get_int(isize::BITS as usize / 8) as isize
    }

    /// 获取一个小端isize
    #[inline]
    pub fn get_isize_le(&mut self) -> isize {
        self.0.get_int_le(isize::BITS as usize / 8) as isize
    }

    /// 获取一个大端usize
    #[inline]
    pub fn get_usize(&mut self) -> usize {
        self.0.get_uint(usize::BITS as usize / 8) as usize
    }

    /// 获取一个小端usize
    #[inline]
    pub fn get_usize_le(&mut self) -> usize {
        self.0.get_uint_le(usize::BITS as usize / 8) as usize
    }

    /// 获取一个大端f32
    #[inline]
    pub fn get_f32(&mut self) -> f32 {
        self.0.get_f32()
    }

    /// 获取一个小端f32
    #[inline]
    pub fn get_f32_le(&mut self) -> f32 {
        self.0.get_f32_le()
    }

    /// 获取一个大端f64
    #[inline]
    pub fn get_f64(&mut self) -> f64 {
        self.0.get_f64()
    }

    /// 获取一个小端f64
    #[inline]
    pub fn get_f64_le(&mut self) -> f64 {
        self.0.get_f64_le()
    }

    /// 获取指定长度的部分缓冲区
    #[inline]
    pub fn get(&mut self, len: usize) -> Self {
        PartBuffer(self.0.copy_to_bytes(len))
    }
}

///
/// 异步字节缓冲区
///
pub struct ByteBuffer {
    stream: BoxReceiver<'static, Arc<Vec<u8>>>, //流
    buf:    Option<BytesMut>,                   //缓冲区
    readed: usize,                              //已读字节
}

unsafe impl Send for ByteBuffer {}
impl !Sync for ByteBuffer {}

impl AsRef<[u8]> for ByteBuffer {
    fn as_ref(&self) -> &[u8] {
        self
            .buf
            .as_ref()
            .unwrap()
            .as_ref()
    }
}

impl ByteBuffer {
    /// 创建一个异步字节缓冲区
    pub fn new(stream: BoxReceiver<'static, Arc<Vec<u8>>>) -> Self {
        let buf = match stream.size_hint() {
            (0, _) => Some(BytesMut::default()),
            (size, _) => Some(BytesMut::with_capacity(size)),
        };

        ByteBuffer {
            stream,
            buf,
            readed: 0,
        }
    }

    /// 获取当前流是否已终止
    #[inline]
    pub fn is_terminated(&self) -> bool {
        self.stream.is_terminated()
    }

    /// 判断当前缓冲区是否为空
    #[inline]
    pub fn is_empty(&self) -> bool {
        self
            .buf
            .as_ref()
            .unwrap()
            .is_empty()
    }

    /// 获取当前缓冲区包含的所有字节长度
    #[inline]
    pub fn len(&self) -> usize {
        self
            .buf
            .as_ref()
            .unwrap()
            .len()
    }

    /// 获取当前缓冲区剩余可读字节长度
    #[inline]
    pub fn remaining(&self) -> usize {
        self
            .buf
            .as_ref()
            .unwrap()
            .remaining()
    }

    /// 流中已发送未接收的数据条目长度
    #[inline]
    pub fn unreceived(&self) -> Option<usize> {
        self.stream.current_len()
    }

    /// 清理前已读字节的总数量
    #[inline]
    pub fn readed(&self) -> usize {
        self.readed
    }

    /// 异步获取一个i8
    pub async fn get_i8(&mut self) -> Option<i8> {
        if !try_fill_buffer(self, 1).await {
            //流已结束，则立即返回空
            return None;
        }

        self.readed += 1;
        Some(self.buf.as_mut().unwrap().get_i8())
    }

    /// 异步获取一个u8
    pub async fn get_u8(&mut self) -> Option<u8> {
        if !try_fill_buffer(self, 1).await {
            //流已结束，则立即返回空
            return None;
        }

        self.readed += 1;
        Some(self.buf.as_mut().unwrap().get_u8())
    }

    /// 异步获取一个大端i16
    pub async fn get_i16(&mut self) -> Option<i16> {
        if !try_fill_buffer(self, 2).await {
            //流已结束，则立即返回空
            return None;
        }

        self.readed += 2;
        Some(self.buf.as_mut().unwrap().get_i16())
    }

    /// 异步获取一个小端i16
    pub async fn get_i16_le(&mut self) -> Option<i16> {
        if !try_fill_buffer(self, 2).await {
            //流已结束，则立即返回空
            return None;
        }

        self.readed += 2;
        Some(self.buf.as_mut().unwrap().get_i16_le())
    }

    /// 异步获取一个大端u16
    pub async fn get_u16(&mut self) -> Option<u16> {
        if !try_fill_buffer(self, 2).await {
            //流已结束，则立即返回空
            return None;
        }

        self.readed += 2;
        Some(self.buf.as_mut().unwrap().get_u16())
    }

    /// 异步获取一个小端u16
    pub async fn get_u16_le(&mut self) -> Option<u16> {
        if !try_fill_buffer(self, 2).await {
            //流已结束，则立即返回空
            return None;
        }

        self.readed += 2;
        Some(self.buf.as_mut().unwrap().get_u16_le())
    }

    /// 异步获取一个大端i32
    pub async fn get_i32(&mut self) -> Option<i32> {
        if !try_fill_buffer(self, 4).await {
            //流已结束，则立即返回空
            return None;
        }

        self.readed += 4;
        Some(self.buf.as_mut().unwrap().get_i32())
    }

    /// 异步获取一个小端i32
    pub async fn get_i32_le(&mut self) -> Option<i32> {
        if !try_fill_buffer(self, 4).await {
            //流已结束，则立即返回空
            return None;
        }

        self.readed += 4;
        Some(self.buf.as_mut().unwrap().get_i32_le())
    }

    /// 异步获取一个大端u32
    pub async fn get_u32(&mut self) -> Option<u32> {
        if !try_fill_buffer(self, 4).await {
            //流已结束，则立即返回空
            return None;
        }

        self.readed += 4;
        Some(self.buf.as_mut().unwrap().get_u32())
    }

    /// 异步获取一个小端u32
    pub async fn get_u32_le(&mut self) -> Option<u32> {
        if !try_fill_buffer(self, 4).await {
            //流已结束，则立即返回空
            return None;
        }

        self.readed += 4;
        Some(self.buf.as_mut().unwrap().get_u32_le())
    }

    /// 异步获取一个大端i64
    pub async fn get_i64(&mut self) -> Option<i64> {
        if !try_fill_buffer(self, 8).await {
            //流已结束，则立即返回空
            return None;
        }

        self.readed += 8;
        Some(self.buf.as_mut().unwrap().get_i64())
    }

    /// 异步获取一个小端i64
    pub async fn get_i64_le(&mut self) -> Option<i64> {
        if !try_fill_buffer(self, 8).await {
            //流已结束，则立即返回空
            return None;
        }

        self.readed += 8;
        Some(self.buf.as_mut().unwrap().get_i64_le())
    }

    /// 异步获取一个大端u64
    pub async fn get_u64(&mut self) -> Option<u64> {
        if !try_fill_buffer(self, 8).await {
            //流已结束，则立即返回空
            return None;
        }

        self.readed += 8;
        Some(self.buf.as_mut().unwrap().get_u64())
    }

    /// 异步获取一个小端u64
    pub async fn get_u64_le(&mut self) -> Option<u64> {
        if !try_fill_buffer(self, 8).await {
            //流已结束，则立即返回空
            return None;
        }

        self.readed += 8;
        Some(self.buf.as_mut().unwrap().get_u64_le())
    }

    /// 异步获取一个大端i128
    pub async fn get_i128(&mut self) -> Option<i128> {
        if !try_fill_buffer(self, 16).await {
            //流已结束，则立即返回空
            return None;
        }

        self.readed += 16;
        Some(self.buf.as_mut().unwrap().get_i128())
    }

    /// 异步获取一个小端i128
    pub async fn get_i128_le(&mut self) -> Option<i128> {
        if !try_fill_buffer(self, 16).await {
            //流已结束，则立即返回空
            return None;
        }

        self.readed += 16;
        Some(self.buf.as_mut().unwrap().get_i128_le())
    }

    /// 异步获取一个大端u128
    pub async fn get_u128(&mut self) -> Option<u128> {
        if !try_fill_buffer(self, 16).await {
            //流已结束，则立即返回空
            return None;
        }

        self.readed += 16;
        Some(self.buf.as_mut().unwrap().get_u128())
    }

    /// 异步获取一个小端u128
    pub async fn get_u128_le(&mut self) -> Option<u128> {
        if !try_fill_buffer(self, 16).await {
            //流已结束，则立即返回空
            return None;
        }

        self.readed += 16;
        Some(self.buf.as_mut().unwrap().get_u128_le())
    }

    /// 异步获取一个大端isize
    pub async fn get_isize(&mut self) -> Option<isize> {
        let require = isize::BITS as usize / 8;

        if !try_fill_buffer(self, require).await {
            //流已结束，则立即返回空
            return None;
        }

        self.readed += isize::BITS as usize / 8;
        Some(self.buf.as_mut().unwrap().get_int(require) as isize)
    }

    /// 异步获取一个小端isize
    pub async fn get_isize_le(&mut self) -> Option<isize> {
        let require = isize::BITS as usize / 8;

        if !try_fill_buffer(self, require).await {
            //流已结束，则立即返回空
            return None;
        }

        self.readed += isize::BITS as usize / 8;
        Some(self.buf.as_mut().unwrap().get_int_le(require) as isize)
    }

    /// 异步获取一个大端usize
    pub async fn get_usize(&mut self) -> Option<usize> {
        let require = usize::BITS as usize / 8;
        if !try_fill_buffer(self, require).await {
            //流已结束，则立即返回空
            return None;
        }

        self.readed += usize::BITS as usize / 8;
        Some(self.buf.as_mut().unwrap().get_uint(require) as usize)
    }

    /// 异步获取一个小端usize
    pub async fn get_usize_le(&mut self) -> Option<usize> {
        let require = usize::BITS as usize / 8;
        if !try_fill_buffer(self, require).await {
            //流已结束，则立即返回空
            return None;
        }

        self.readed += usize::BITS as usize / 8;
        Some(self.buf.as_mut().unwrap().get_uint_le(require) as usize)
    }

    /// 异步获取一个大端f32
    pub async fn get_f32(&mut self) -> Option<f32> {
        if !try_fill_buffer(self, 4).await {
            //流已结束，则立即返回空
            return None;
        }

        self.readed += 4;
        Some(self.buf.as_mut().unwrap().get_f32())
    }

    /// 异步获取一个小端f32
    pub async fn get_f32_le(&mut self) -> Option<f32> {
        if !try_fill_buffer(self, 4).await {
            //流已结束，则立即返回空
            return None;
        }

        self.readed += 4;
        Some(self.buf.as_mut().unwrap().get_f32_le())
    }

    /// 异步获取一个大端f64
    pub async fn get_f64(&mut self) -> Option<f64> {
        if !try_fill_buffer(self, 8).await {
            //流已结束，则立即返回空
            return None;
        }

        self.readed += 8;
        Some(self.buf.as_mut().unwrap().get_f64())
    }

    /// 异步获取一个小端f64
    pub async fn get_f64_le(&mut self) -> Option<f64> {
        if !try_fill_buffer(self, 8).await {
            //流已结束，则立即返回空
            return None;
        }

        self.readed += 8;
        Some(self.buf.as_mut().unwrap().get_f64_le())
    }

    /// 异步获取指定长度的部分缓冲区
    pub async fn get(&mut self, len: usize) -> Option<PartBuffer> {
        if !try_fill_buffer(self, len).await {
            //流已结束，则立即返回空
            return None;
        }

        self.readed += len; //更新已读字节数量
        Some(PartBuffer(self.buf.as_mut().unwrap().copy_to_bytes(len)))
    }

    /// 尝试异步获取指定长度的部分缓冲区，
    /// 此方法保证不会导致异步阻塞，同时此方法也不保证一定可以获取到不小于指定长度的部分缓冲区
    /// 当指定获取的字节数量为0，表示尝试获取当前流和当前缓冲区中所有剩余的未读字节
    pub async fn try_get(&mut self, len: usize) -> Option<PartBuffer> {
        let mut remaining = self.remaining();
        if (remaining == 0) && !try_fill_buffer_by_non_blocking(self, len).await {
            //流已结束且当前缓冲区剩余可读字节长度为0，则立即返回空
            return None;
        }

        remaining = self.remaining(); //填充后需要再次获取最新的缓冲区剩余可读字节数
        if remaining < len {
            //当前缓冲区剩余可读字节长度小于指定长度，则返回所有剩余可读字节
            self.readed += remaining; //更新已读字节数量
            Some(PartBuffer(self.buf.as_mut().unwrap().copy_to_bytes(remaining)))
        } else {
            //当前缓冲区剩余可读字节长度大于等于指定长度，则返回指定长度的可读字节
            self.readed += len; //更新已读字节数量
            Some(PartBuffer(self.buf.as_mut().unwrap().copy_to_bytes(len)))
        }
    }

    /// 截断当前所有已读缓冲区
    /// 这会释放已读缓冲区的内存，并重置已读字节的总数量
    pub fn truncate(&mut self) -> usize {
        let old_buf = self.buf.take().unwrap();
        let mut new_buf = BytesMut::with_capacity(old_buf.remaining());
        new_buf.put(old_buf);
        self.buf = Some(new_buf);
        let result = self.readed();
        self.readed = 0;

        result
    }

    /// 清空当前缓冲区
    /// 这会释放当前缓冲区的所有内存，并重置已读字节的总数量
    pub fn clear(&mut self) {
        let _ = self.buf.take().unwrap();
        self.buf = Some(BytesMut::new());
        self.readed = 0;
    }
}

// 如果当前缓冲区没有至少指定字节长度的数据，则尝试从流中获取数据，并填充至少指定字节长度的数据到当前缓冲区，
// 无需填充或成功填充则返回真，流已结束则返回假
#[inline]
async fn try_fill_buffer(buffer: &mut ByteBuffer,
                         require: usize) -> bool {
    let mut ready_len = buffer.remaining(); //初始化已就绪的字节长度
    while ready_len < require {
        match buffer.stream.next().await {
            None => {
                //流已结束，则立即返回空
                return false;
            },
            Some(bin) => {
                //从流中获取数据
                buffer
                    .buf
                    .as_mut()
                    .unwrap()
                    .put_slice(bin.as_ref()); //写入当前缓冲区
                ready_len += bin.len(); //更新已就绪字节的长度
            },
        }
    }

    true
}

// 如果当前缓冲区没有至少指定字节长度的数据，则尝试从流中获取数据，并填充至少指定字节长度的数据到当前缓冲区，
// 无需填充或成功填充则返回真，流已结束则返回假
// 如果指定长度为0，则在保证不异步阻塞的前提下，尽可能从流中获取数据
#[inline]
async fn try_fill_buffer_by_non_blocking(buffer: &mut ByteBuffer,
                                         require: usize) -> bool {
    let mut ready_len = buffer.remaining(); //初始化已就绪的字节长度
    while (require == 0)
        || (ready_len < require && buffer.stream.current_len().unwrap() > 0) {
        match buffer.stream.next().await {
            None => {
                //流已结束，则立即返回空
                return false;
            },
            Some(bin) => {
                //从流中获取数据
                buffer
                    .buf
                    .as_mut()
                    .unwrap()
                    .put_slice(bin.as_ref()); //写入当前缓冲区
                ready_len += bin.len(); //更新已就绪字节的长度
            },
        }
    }

    true
}

