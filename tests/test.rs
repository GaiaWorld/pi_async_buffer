use std::thread;
use std::sync::Arc;
use std::path::PathBuf;
use std::time::Duration;
use std::collections::VecDeque;
use std::io::{IoSlice, ErrorKind};

use futures::SinkExt;
use bytes::{Buf, BufMut, BytesMut};

use pi_async::rt::{AsyncRuntime, AsyncValue,
                   multi_thread::MultiTaskRuntimeBuilder,
                   async_pipeline::{AsyncReceiverExt, channel}};
use pi_async::rt::async_pipeline::AsyncPipeLineExt;
use pi_async_file::file::{AsyncFileOptions, AsyncFile, WriteOptions};
use pi_async_buffer::ByteBuffer;

#[test]
fn test_bytes() {
    let mut buf = BytesMut::new();

    let bin = "Hello World!".as_bytes();
    if buf.remaining() == 0 {
        buf.put_slice(bin);
    }
    assert_eq!(buf.remaining(), bin.len());

    let mut tmp = Vec::new();
    for _ in 0..8 {
        tmp.push(buf.get_u8());
    }
    assert_eq!(buf.remaining(), 4);
    assert_eq!(String::from_utf8_lossy(tmp.as_slice()).as_ref(), "Hello Wo");
    tmp.clear();

    buf.put_slice(bin);
    assert_eq!(buf.remaining(), 16);

    for _ in 0..16 {
        tmp.push(buf.get_u8());
    }
    assert_eq!(buf.remaining(), 0);
    assert_eq!(String::from_utf8_lossy(tmp.as_slice()).as_ref(), "rld!Hello World!");
    tmp.clear();

    buf.put_slice(bin);
    assert_eq!(buf.remaining(), bin.len());

    let part = buf.copy_to_bytes(10);
    assert_eq!(String::from_utf8_lossy(part.as_ref()).as_ref(), "Hello Worl");
    assert_eq!(String::from_utf8_lossy(buf.as_ref()).as_ref(), "d!");
}

#[test]
fn test_bytes_io_slice() {
    let mut buf = BytesMut::new();

    let bin = "Hello World!".as_bytes();
    buf.put(bin);

    let bin = vec![255; 16];
    buf.put(&bin[..]);

    buf.put_u16_le(0xffff);
    buf.put_u32_le(0x7fffffff);
    buf.put_u64_le(0x7fffffffffffffff);

    let mut io_list = [IoSlice::new(&[]); 32];
    let len = buf.chunks_vectored(&mut io_list);
    println!("!!!!!!remaining: {:?}, slice len: {:?}, io_list: {:?}", buf.remaining(), len, io_list);
}

struct TestBin(VecDeque<u8>);

impl Drop for TestBin {
    fn drop(&mut self) {
        println!("!!!!!!drop test bin");
    }
}

impl Buf for TestBin {
    #[inline]
    fn advance(&mut self, cnt: usize) {
        self.0.advance(cnt);
    }

    #[inline]
    fn chunk(&self) -> &[u8] {
        self.0.chunk()
    }

    #[inline]
    fn remaining(&self) -> usize {
        self.0.remaining()
    }
}

// 测试BytesMut是否在消耗指定字节后会自动释放内存
#[test]
fn test_free_bytes() {
    let mut buf = BytesMut::new();

    {
        let bin = "Hello World!".as_bytes();
        buf.put_slice(bin);
    }

    {
        let bin = vec![0xff; 100 * 1024 * 1024];
        buf.put_slice(bin.as_slice());
    }

    {
        let bin = TestBin(vec!(0x7f; 100 * 1024 * 1024).into());
        buf.put(bin);
    }

    println!("!!!!!!capacity: {}, len: {}, remaining: {}", buf.capacity(), buf.len(), buf.remaining());
    thread::sleep(Duration::from_millis(10000));

    let _ = buf.copy_to_bytes(12);
    println!("!!!!!!capacity: {}, len: {}, remaining: {}", buf.capacity(), buf.len(), buf.remaining());
    let _ = buf.copy_to_bytes(100 * 1024 * 1024);
    println!("!!!!!!capacity: {}, len: {}, remaining: {}", buf.capacity(), buf.len(), buf.remaining());

    let mut tmp = BytesMut::new();
    tmp.put(buf);
    let mut buf = tmp;
    thread::sleep(Duration::from_millis(3000));

    {
        let part = buf.copy_to_bytes(50 * 1024 * 1024);
        println!("!!!!!!part0: {:?}", part.len());
        println!("!!!!!!capacity: {}, len: {}, remaining: {}", buf.capacity(), buf.len(), buf.remaining());
    }

    let mut tmp = BytesMut::new();
    tmp.put(buf);
    let mut buf = tmp;
    thread::sleep(Duration::from_millis(3000));

    {
        let part = buf.copy_to_bytes(50 * 1024 * 1024);
        println!("!!!!!!part1: {:?}", part.len());
        println!("!!!!!!capacity: {}, len: {}, remaining: {}", buf.capacity(), buf.len(), buf.remaining());
    }

    let mut tmp = BytesMut::new();
    tmp.put(buf);
    thread::sleep(Duration::from_millis(1000000000));
}

#[test]
fn test_async_buffer() {
    let rt = MultiTaskRuntimeBuilder::default().build();
    let rt_copy = rt.clone();

    let value = AsyncValue::new();
    let value_copy = value.clone();
    let (mut sender, receiver) = channel(512);
    let mut buffer = ByteBuffer::new(receiver.pin_boxed());
    rt.spawn(rt.alloc(), async move {
        assert_eq!(buffer.is_terminated(), false);

        if let Some(num) = buffer.get_i8().await {
            assert_eq!(buffer.truncate(), 1);
            assert_eq!(num, -125);
        } else {
            panic!("Get i8 failed, reason: stream already closed");
        }
        if let Some(num) = buffer.get_u8().await {
            assert_eq!(buffer.truncate(), 1);
            assert_eq!(num, 0xff);
        } else {
            panic!("Get u8 failed, reason: stream already closed");
        }
        if let Some(num) = buffer.get_i16().await {
            assert_eq!(buffer.truncate(), 2);
            assert_eq!(num, -30000);
        } else {
            panic!("Get i16 failed, reason: stream already closed");
        }
        if let Some(num) = buffer.get_i16_le().await {
            assert_eq!(buffer.truncate(), 2);
            assert_eq!(num, -30000);
        } else {
            panic!("Get i16_le failed, reason: stream already closed");
        }
        if let Some(num) = buffer.get_u16().await {
            assert_eq!(buffer.truncate(), 2);
            assert_eq!(num, 0xffff);
        } else {
            panic!("Get u16 failed, reason: stream already closed");
        }
        if let Some(num) = buffer.get_u16_le().await {
            assert_eq!(buffer.truncate(), 2);
            assert_eq!(num, 0xffff);
        } else {
            panic!("Get u16_le failed, reason: stream already closed");
        }
        if let Some(num) = buffer.get_i32().await {
            assert_eq!(buffer.truncate(), 4);
            assert_eq!(num, -2000000000);
        } else {
            panic!("Get i32 failed, reason: stream already closed");
        }
        if let Some(num) = buffer.get_i32_le().await {
            assert_eq!(buffer.truncate(), 4);
            assert_eq!(num, -2000000000);
        } else {
            panic!("Get i32_le failed, reason: stream already closed");
        }
        if let Some(num) = buffer.get_u32().await {
            assert_eq!(buffer.truncate(), 4);
            assert_eq!(num, 0xffffffff);
        } else {
            panic!("Get u32 failed, reason: stream already closed");
        }
        if let Some(num) = buffer.get_u32_le().await {
            assert_eq!(buffer.truncate(), 4);
            assert_eq!(num, 0xffffffff);
        } else {
            panic!("Get u32_le failed, reason: stream already closed");
        }
        if let Some(num) = buffer.get_i64().await {
            assert_eq!(buffer.truncate(), 8);
            assert_eq!(num, -9223372036854775805);
        } else {
            panic!("Get i64 failed, reason: stream already closed");
        }
        if let Some(num) = buffer.get_i64_le().await {
            assert_eq!(buffer.truncate(), 8);
            assert_eq!(num, -9223372036854775805);
        } else {
            panic!("Get i64_le failed, reason: stream already closed");
        }
        if let Some(num) = buffer.get_u64().await {
            assert_eq!(buffer.truncate(), 8);
            assert_eq!(num, 0xffffffffffffffff);
        } else {
            panic!("Get u64 failed, reason: stream already closed");
        }
        if let Some(num) = buffer.get_u64_le().await {
            assert_eq!(buffer.truncate(), 8);
            assert_eq!(num, 0xffffffffffffffff);
        } else {
            panic!("Get u64_le failed, reason: stream already closed");
        }
        if let Some(num) = buffer.get_i128().await {
            assert_eq!(buffer.truncate(), 16);
            assert_eq!(num, -170141183460469231731687303715884105726);
        } else {
            panic!("Get i128 failed, reason: stream already closed");
        }
        if let Some(num) = buffer.get_i128_le().await {
            assert_eq!(buffer.truncate(), 16);
            assert_eq!(num, -170141183460469231731687303715884105726);
        } else {
            panic!("Get i128_le failed, reason: stream already closed");
        }
        if let Some(num) = buffer.get_u128().await {
            assert_eq!(buffer.truncate(), 16);
            assert_eq!(num, 0xffffffffffffffffffffffffffffffff);
        } else {
            panic!("Get u128 failed, reason: stream already closed");
        }
        if let Some(num) = buffer.get_u128_le().await {
            assert_eq!(buffer.truncate(), 16);
            assert_eq!(num, 0xffffffffffffffffffffffffffffffff);
        } else {
            panic!("Get u128_le failed, reason: stream already closed");
        }
        if let Some(num) = buffer.get_isize().await {
            assert_eq!(buffer.truncate(), isize::BITS as usize / 8);
            assert_eq!(num, -2000000000);
        } else {
            panic!("Get isize failed, reason: stream already closed");
        }
        if let Some(num) = buffer.get_isize_le().await {
            assert_eq!(buffer.truncate(), isize::BITS as usize / 8);
            assert_eq!(num, -2000000000);
        } else {
            panic!("Get isize_le failed, reason: stream already closed");
        }
        if let Some(num) = buffer.get_usize().await {
            assert_eq!(buffer.truncate(), usize::BITS as usize / 8);
            assert_eq!(num, 0xffffffff);
        } else {
            panic!("Get usize failed, reason: stream already closed");
        }
        if let Some(num) = buffer.get_usize_le().await {
            assert_eq!(buffer.truncate(), usize::BITS as usize / 8);
            assert_eq!(num, 0xffffffff);
        } else {
            panic!("Get usize_le failed, reason: stream already closed");
        }
        if let Some(num) = buffer.get_f32().await {
            assert_eq!(buffer.truncate(), 4);
            assert_eq!(num, 0.999999);
        } else {
            panic!("Get f32 failed, reason: stream already closed");
        }
        if let Some(num) = buffer.get_f32_le().await {
            assert_eq!(buffer.truncate(), 4);
            assert_eq!(num, 0.999999);
        } else {
            panic!("Get f32_le failed, reason: stream already closed");
        }
        if let Some(num) = buffer.get_f64().await {
            assert_eq!(buffer.truncate(), 8);
            assert_eq!(num, 0.9999999999999999);
        } else {
            panic!("Get f64 failed, reason: stream already closed");
        }
        if let Some(num) = buffer.get_f64_le().await {
            assert_eq!(buffer.truncate(), 8);
            assert_eq!(num, 0.9999999999999999);
        } else {
            panic!("Get f64_le failed, reason: stream already closed");
        }
        if let Some(part) = buffer.get(120).await {
            assert_eq!(buffer.truncate(), 120);
            assert_eq!(String::from_utf8_lossy(part.as_ref()).as_ref(), "Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!");
        } else {
            panic!("Get part failed, reason: stream already closed");
        }

        value.set(()); //通知发送端准备关闭流
        if let Some(_) = buffer.get(0xffffffff).await {
            panic!("Get part failed, reason: invalid buffer");
        }

        assert_eq!(buffer.truncate(), 0);
        assert_eq!(buffer.is_terminated(), true);

        println!("Buffer stream closed");
    });

    rt.spawn(rt.alloc(), async move {
        let mut buf: Vec<u8> = Vec::new();
        buf.put_i8(-125);
        buf.put_u8(0xff);
        buf.put_i16(-30000);
        let bin = Arc::new(buf);
        loop {
            if let Err(e) = sender.feed(bin.clone()).await {
                if e.kind() != ErrorKind::WouldBlock {
                    panic!("Sender feed failed, frame: {:?}, reason: {:?}", bin, e);
                }
                continue;
            }

            if let Err(e) = sender.flush().await {
                if e.kind() != ErrorKind::WouldBlock {
                     panic!("Sender flush failed, frame: {:?}, reason: {:?}", bin, e);
                }
                continue;
            }

            break;
        }

        let mut buf: Vec<u8> = Vec::new();
        buf.put_i16_le(-30000);
        buf.put_u16(0xffff);
        buf.put_u16_le(0xffff);
        let bin = Arc::new(buf);
        loop {
            if let Err(e) = sender.feed(bin.clone()).await {
                if e.kind() != ErrorKind::WouldBlock {
                    panic!("Sender feed failed, frame: {:?}, reason: {:?}", bin, e);
                }
                continue;
            }

            if let Err(e) = sender.flush().await {
                if e.kind() != ErrorKind::WouldBlock {
                     panic!("Sender flush failed, frame: {:?}, reason: {:?}", bin, e);
                }
                continue;
            }

            break;
        }

        let mut buf: Vec<u8> = Vec::new();
        buf.put_i32(-2000000000);
        buf.put_i32_le(-2000000000);
        buf.put_u32(0xffffffff);
        let bin = Arc::new(buf);
        loop {
            if let Err(e) = sender.feed(bin.clone()).await {
                if e.kind() != ErrorKind::WouldBlock {
                    panic!("Sender feed failed, frame: {:?}, reason: {:?}", bin, e);
                }
                continue;
            }

            if let Err(e) = sender.flush().await {
                if e.kind() != ErrorKind::WouldBlock {
                     panic!("Sender flush failed, frame: {:?}, reason: {:?}", bin, e);
                }
                continue;
            }

            break;
        }

        let mut buf: Vec<u8> = Vec::new();
        buf.put_u32_le(0xffffffff);
        buf.put_i64(-9223372036854775805);
        buf.put_i64_le(-9223372036854775805);
        let bin = Arc::new(buf);
        loop {
            if let Err(e) = sender.feed(bin.clone()).await {
                if e.kind() != ErrorKind::WouldBlock {
                    panic!("Sender feed failed, frame: {:?}, reason: {:?}", bin, e);
                }
                continue;
            }

            if let Err(e) = sender.flush().await {
                if e.kind() != ErrorKind::WouldBlock {
                     panic!("Sender flush failed, frame: {:?}, reason: {:?}", bin, e);
                }
                continue;
            }

            break;
        }

        let mut buf: Vec<u8> = Vec::new();
        buf.put_u64(0xffffffffffffffff);
        buf.put_u64_le(0xffffffffffffffff);
        buf.put_i128(-170141183460469231731687303715884105726);
        let bin = Arc::new(buf);
        loop {
            if let Err(e) = sender.feed(bin.clone()).await {
                if e.kind() != ErrorKind::WouldBlock {
                    panic!("Sender feed failed, frame: {:?}, reason: {:?}", bin, e);
                }
                continue;
            }

            if let Err(e) = sender.flush().await {
                if e.kind() != ErrorKind::WouldBlock {
                     panic!("Sender flush failed, frame: {:?}, reason: {:?}", bin, e);
                }
                continue;
            }

            break;
        }

        let mut buf: Vec<u8> = Vec::new();
        buf.put_i128_le(-170141183460469231731687303715884105726);
        buf.put_u128(0xffffffffffffffffffffffffffffffff);
        buf.put_u128_le(0xffffffffffffffffffffffffffffffff);
        let bin = Arc::new(buf);
        loop {
            if let Err(e) = sender.feed(bin.clone()).await {
                if e.kind() != ErrorKind::WouldBlock {
                    panic!("Sender feed failed, frame: {:?}, reason: {:?}", bin, e);
                }
                continue;
            }

            if let Err(e) = sender.flush().await {
                if e.kind() != ErrorKind::WouldBlock {
                     panic!("Sender flush failed, frame: {:?}, reason: {:?}", bin, e);
                }
                continue;
            }

            break;
        }

        let mut buf: Vec<u8> = Vec::new();
        buf.put_int(-2000000000, (isize::BITS / 8) as usize);
        buf.put_int_le(-2000000000, (isize::BITS / 8) as usize);
        buf.put_uint(0xffffffff, (usize::BITS / 8) as usize);
        let bin = Arc::new(buf);
        loop {
            if let Err(e) = sender.feed(bin.clone()).await {
                if e.kind() != ErrorKind::WouldBlock {
                    panic!("Sender feed failed, frame: {:?}, reason: {:?}", bin, e);
                }
                continue;
            }

            if let Err(e) = sender.flush().await {
                if e.kind() != ErrorKind::WouldBlock {
                     panic!("Sender flush failed, frame: {:?}, reason: {:?}", bin, e);
                }
                continue;
            }

            break;
        }

        let mut buf: Vec<u8> = Vec::new();
        buf.put_uint_le(0xffffffff, (usize::BITS / 8) as usize);
        buf.put_f32(0.999999);
        buf.put_f32_le(0.999999);
        let bin = Arc::new(buf);
        loop {
            if let Err(e) = sender.feed(bin.clone()).await {
                if e.kind() != ErrorKind::WouldBlock {
                    panic!("Sender feed failed, frame: {:?}, reason: {:?}", bin, e);
                }
                continue;
            }

            if let Err(e) = sender.flush().await {
                if e.kind() != ErrorKind::WouldBlock {
                     panic!("Sender flush failed, frame: {:?}, reason: {:?}", bin, e);
                }
                continue;
            }

            break;
        }

        let mut buf: Vec<u8> = Vec::new();
        buf.put_f64(0.9999999999999999);
        buf.put_f64_le(0.9999999999999999);
        let bin = Arc::new(buf);
        loop {
            if let Err(e) = sender.feed(bin.clone()).await {
                if e.kind() != ErrorKind::WouldBlock {
                    panic!("Sender feed failed, frame: {:?}, reason: {:?}", bin, e);
                }
                continue;
            }

            if let Err(e) = sender.flush().await {
                if e.kind() != ErrorKind::WouldBlock {
                     panic!("Sender flush failed, frame: {:?}, reason: {:?}", bin, e);
                }
                continue;
            }

            break;
        }

        let mut buf: Vec<u8> = Vec::new();
        let string = "Hello World!Hello World!Hello World!Hello World!Hello ";
        buf.put_slice(string.as_bytes());
        let bin = Arc::new(buf);
        loop {
            if let Err(e) = sender.feed(bin.clone()).await {
                if e.kind() != ErrorKind::WouldBlock {
                    panic!("Sender feed failed, frame: {:?}, reason: {:?}", bin, e);
                }
                continue;
            }

            if let Err(e) = sender.flush().await {
                if e.kind() != ErrorKind::WouldBlock {
                     panic!("Sender flush failed, frame: {:?}, reason: {:?}", bin, e);
                }
                continue;
            }

            break;
        }

        let mut buf: Vec<u8> = Vec::new();
        let string = "World!Hello World!Hello World!Hello World!Hello World!Hello World!";
        buf.put_slice(string.as_bytes());
        let bin = Arc::new(buf);
        loop {
            if let Err(e) = sender.feed(bin.clone()).await {
                if e.kind() != ErrorKind::WouldBlock {
                    panic!("Sender feed failed, frame: {:?}, reason: {:?}", bin, e);
                }
                continue;
            }

            if let Err(e) = sender.flush().await {
                if e.kind() != ErrorKind::WouldBlock {
                     panic!("Sender flush failed, frame: {:?}, reason: {:?}", bin, e);
                }
                continue;
            }

            break;
        }

        //等待指定时间，保证接收端异步阻塞后再关闭流
        value_copy.await;
        rt_copy.timeout(1000).await;
        loop {
            if let Err(e) = sender.close().await {
                if e.kind() != ErrorKind::WouldBlock {
                    panic!("Sender close failed, reason: {:?}", e);
                }
                continue;
            }

            break;
        }
        println!("Sender closed");
    });

    thread::sleep(Duration::from_millis(1000000000));
}

#[test]
fn test_file_stream() {
    let rt = MultiTaskRuntimeBuilder::default().build();
    let rt_copy = rt.clone();

    let (mut sender, receiver) = channel(1);
    let mut buffer = ByteBuffer::new(receiver.pin_boxed());

    rt.spawn(rt.alloc(), async move {
        let mut vec = Vec::new();
        while let Some(b) = buffer.get_u8().await {
            println!("Bytes remaining: {}", buffer.remaining());
            vec.push(b);
        }
        assert_eq!(buffer.truncate(), vec.len());
        assert_eq!(String::from_utf8(vec).unwrap().as_str(), "Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!");

        println!("Receive finish");
    });

    rt.spawn(rt.alloc(), async move {
        //打开测试用文件
        let mut file = match AsyncFile::open(rt_copy.clone(),
                                             "./tests/test_file.txt".to_string(),
                                             AsyncFileOptions::TruncateReadWrite).await {
            Err(e) => panic!("Open test file failed, reason: {:?}", e),
            Ok(file) => {
                file
            },
        };

        //初始化测试用文件
        let buf = "Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!";
        if let Err(e) = file.write(0,
                                   buf.as_bytes(),
                                   WriteOptions::Sync(true)).await {
            panic!("Init test file failed, reason: {:?}", e);
        }

        let mut pos = 0;
        loop {
            match file.read(pos, 8).await {
                Err(e) => {
                    panic!("Read test file failed, pos: {:?}, reason: {:?}", pos, e);
                },
                Ok(r) => {
                    println!("Read file ok, pos: {}, len: {}", pos, r.len());
                    if r.is_empty() {
                        //已读完，则结束文件的读取
                        break;
                    }

                    pos += 8; //更新位置
                    let bin = Arc::new(r);
                    loop {
                        if let Err(e) = sender.feed(bin.clone()).await {
                            if e.kind() != ErrorKind::WouldBlock {
                                panic!("Sender feed failed, frame: {:?}, reason: {:?}", bin, e);
                            }
                            continue;
                        }

                        if let Err(e) = sender.flush().await {
                            if e.kind() != ErrorKind::WouldBlock {
                                panic!("Sender flush failed, frame: {:?}, reason: {:?}", bin, e);
                            }
                            continue;
                        }

                        break;
                    }
                },
            }
        }

        loop {
            if let Err(e) = sender.close().await {
                if e.kind() != ErrorKind::WouldBlock {
                    panic!("Sender close failed, reason: {:?}", e);
                }
                continue;
            }

            break;
        }
        println!("Sender closed");
    });

    thread::sleep(Duration::from_millis(1000000000));
}