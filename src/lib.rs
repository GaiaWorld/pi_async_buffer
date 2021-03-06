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
    buf:    BytesMut,                           //缓冲区
}

unsafe impl Send for ByteBuffer {}
impl !Sync for ByteBuffer {}

impl AsRef<[u8]> for ByteBuffer {
    fn as_ref(&self) -> &[u8] {
        self.buf.as_ref()
    }
}

impl ByteBuffer {
    /// 创建一个异步字节缓冲区
    pub fn new(stream: BoxReceiver<'static, Arc<Vec<u8>>>) -> Self {
        let buf = match stream.size_hint() {
            (0, _) => BytesMut::default(),
            (size, _) => BytesMut::with_capacity(size),
        };

        ByteBuffer {
            stream,
            buf,
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
        self.buf.is_empty()
    }

    /// 获取当前缓冲区包含的所有字节长度
    #[inline]
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    /// 获取当前缓冲区剩余可读字节长度
    #[inline]
    pub fn remaining(&self) -> usize {
        self.buf.remaining()
    }

    /// 异步获取一个i8
    pub async fn get_i8(&mut self) -> Option<i8> {
        if !try_fill_buffer(self, 1).await {
            //流已结束，则立即返回空
            return None;
        }

        Some(self.buf.get_i8())
    }

    /// 异步获取一个u8
    pub async fn get_u8(&mut self) -> Option<u8> {
        if !try_fill_buffer(self, 1).await {
            //流已结束，则立即返回空
            return None;
        }

        Some(self.buf.get_u8())
    }

    /// 异步获取一个大端i16
    pub async fn get_i16(&mut self) -> Option<i16> {
        if !try_fill_buffer(self, 2).await {
            //流已结束，则立即返回空
            return None;
        }

        Some(self.buf.get_i16())
    }

    /// 异步获取一个小端i16
    pub async fn get_i16_le(&mut self) -> Option<i16> {
        if !try_fill_buffer(self, 2).await {
            //流已结束，则立即返回空
            return None;
        }

        Some(self.buf.get_i16_le())
    }

    /// 异步获取一个大端u16
    pub async fn get_u16(&mut self) -> Option<u16> {
        if !try_fill_buffer(self, 2).await {
            //流已结束，则立即返回空
            return None;
        }

        Some(self.buf.get_u16())
    }

    /// 异步获取一个小端u16
    pub async fn get_u16_le(&mut self) -> Option<u16> {
        if !try_fill_buffer(self, 2).await {
            //流已结束，则立即返回空
            return None;
        }

        Some(self.buf.get_u16_le())
    }

    /// 异步获取一个大端i32
    pub async fn get_i32(&mut self) -> Option<i32> {
        if !try_fill_buffer(self, 4).await {
            //流已结束，则立即返回空
            return None;
        }

        Some(self.buf.get_i32())
    }

    /// 异步获取一个小端i32
    pub async fn get_i32_le(&mut self) -> Option<i32> {
        if !try_fill_buffer(self, 4).await {
            //流已结束，则立即返回空
            return None;
        }

        Some(self.buf.get_i32_le())
    }

    /// 异步获取一个大端u32
    pub async fn get_u32(&mut self) -> Option<u32> {
        if !try_fill_buffer(self, 4).await {
            //流已结束，则立即返回空
            return None;
        }

        Some(self.buf.get_u32())
    }

    /// 异步获取一个小端u32
    pub async fn get_u32_le(&mut self) -> Option<u32> {
        if !try_fill_buffer(self, 4).await {
            //流已结束，则立即返回空
            return None;
        }

        Some(self.buf.get_u32_le())
    }

    /// 异步获取一个大端i64
    pub async fn get_i64(&mut self) -> Option<i64> {
        if !try_fill_buffer(self, 8).await {
            //流已结束，则立即返回空
            return None;
        }

        Some(self.buf.get_i64())
    }

    /// 异步获取一个小端i64
    pub async fn get_i64_le(&mut self) -> Option<i64> {
        if !try_fill_buffer(self, 8).await {
            //流已结束，则立即返回空
            return None;
        }

        Some(self.buf.get_i64_le())
    }

    /// 异步获取一个大端u64
    pub async fn get_u64(&mut self) -> Option<u64> {
        if !try_fill_buffer(self, 8).await {
            //流已结束，则立即返回空
            return None;
        }

        Some(self.buf.get_u64())
    }

    /// 异步获取一个小端u64
    pub async fn get_u64_le(&mut self) -> Option<u64> {
        if !try_fill_buffer(self, 8).await {
            //流已结束，则立即返回空
            return None;
        }

        Some(self.buf.get_u64_le())
    }

    /// 异步获取一个大端i128
    pub async fn get_i128(&mut self) -> Option<i128> {
        if !try_fill_buffer(self, 16).await {
            //流已结束，则立即返回空
            return None;
        }

        Some(self.buf.get_i128())
    }

    /// 异步获取一个小端i128
    pub async fn get_i128_le(&mut self) -> Option<i128> {
        if !try_fill_buffer(self, 16).await {
            //流已结束，则立即返回空
            return None;
        }

        Some(self.buf.get_i128_le())
    }

    /// 异步获取一个大端u128
    pub async fn get_u128(&mut self) -> Option<u128> {
        if !try_fill_buffer(self, 16).await {
            //流已结束，则立即返回空
            return None;
        }

        Some(self.buf.get_u128())
    }

    /// 异步获取一个小端u128
    pub async fn get_u128_le(&mut self) -> Option<u128> {
        if !try_fill_buffer(self, 16).await {
            //流已结束，则立即返回空
            return None;
        }

        Some(self.buf.get_u128_le())
    }

    /// 异步获取一个大端isize
    pub async fn get_isize(&mut self) -> Option<isize> {
        let require = isize::BITS as usize / 8;

        if !try_fill_buffer(self, require).await {
            //流已结束，则立即返回空
            return None;
        }

        Some(self.buf.get_int(require) as isize)
    }

    /// 异步获取一个小端isize
    pub async fn get_isize_le(&mut self) -> Option<isize> {
        let require = isize::BITS as usize / 8;

        if !try_fill_buffer(self, require).await {
            //流已结束，则立即返回空
            return None;
        }

        Some(self.buf.get_int_le(require) as isize)
    }

    /// 异步获取一个大端usize
    pub async fn get_usize(&mut self) -> Option<usize> {
        let require = usize::BITS as usize / 8;
        if !try_fill_buffer(self, require).await {
            //流已结束，则立即返回空
            return None;
        }

        Some(self.buf.get_uint(require) as usize)
    }

    /// 异步获取一个小端usize
    pub async fn get_usize_le(&mut self) -> Option<usize> {
        let require = usize::BITS as usize / 8;
        if !try_fill_buffer(self, require).await {
            //流已结束，则立即返回空
            return None;
        }

        Some(self.buf.get_uint_le(require) as usize)
    }

    /// 异步获取一个大端f32
    pub async fn get_f32(&mut self) -> Option<f32> {
        if !try_fill_buffer(self, 4).await {
            //流已结束，则立即返回空
            return None;
        }

        Some(self.buf.get_f32())
    }

    /// 异步获取一个小端f32
    pub async fn get_f32_le(&mut self) -> Option<f32> {
        if !try_fill_buffer(self, 4).await {
            //流已结束，则立即返回空
            return None;
        }

        Some(self.buf.get_f32_le())
    }

    /// 异步获取一个大端f64
    pub async fn get_f64(&mut self) -> Option<f64> {
        if !try_fill_buffer(self, 8).await {
            //流已结束，则立即返回空
            return None;
        }

        Some(self.buf.get_f64())
    }

    /// 异步获取一个小端f64
    pub async fn get_f64_le(&mut self) -> Option<f64> {
        if !try_fill_buffer(self, 8).await {
            //流已结束，则立即返回空
            return None;
        }

        Some(self.buf.get_f64_le())
    }

    /// 异步获取指定长度的部分缓冲区
    pub async fn get(&mut self, len: usize) -> Option<PartBuffer> {
        if !try_fill_buffer(self, len).await {
            //流已结束，则立即返回空
            return None;
        }

        Some(PartBuffer(self.buf.copy_to_bytes(len)))
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
                buffer.buf.put_slice(bin.as_ref()); //写入当前缓冲区
                ready_len += bin.len(); //更新已就绪字节的长度
            },
        }
    }

    true
}

