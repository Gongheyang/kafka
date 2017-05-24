/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import static org.junit.Assert.*;

@RunWith(PowerMockRunner.class)
public class MmapBufferPoolTest {
    private final MockTime time = new MockTime();
    private final long maxBlockTimeMs = 2000;

    @Test
    public void testSimple() throws Exception {
        long totalMemory = 64 * 1024;
        int size = 1024;
        File storefile = new File(System.getProperty("java.io.tmpdir"), "kafka-producer-data-" + new Random().nextInt(Integer.MAX_VALUE) + ".dat");
        BufferPool pool = new MmapBufferPool(totalMemory, size, time, storefile);
        ByteBuffer buffer = pool.allocate(size, maxBlockTimeMs);
        assertEquals("Buffer size should equal requested size.", size, buffer.limit());
        assertEquals("Unallocated memory should have shrunk", totalMemory - size, pool.unallocatedMemory());
        assertEquals("Available memory should have shrunk", totalMemory - size, pool.availableMemory());
//        buffer.putInt(1);
//        buffer.flip();
        pool.deallocate(buffer);
        assertEquals("All memory should be available", totalMemory, pool.availableMemory());
        assertEquals("But now some is on the free list", totalMemory - size, pool.unallocatedMemory());
        buffer = pool.allocate(size, maxBlockTimeMs);
//        assertEquals("Recycled buffer should be cleared.", 0, buffer.position());
//        assertEquals("Recycled buffer should be cleared.", buffer.capacity(), buffer.limit());
        pool.deallocate(buffer);
        assertEquals("All memory should be available", totalMemory, pool.availableMemory());
        assertEquals("Still a single buffer on the free list", totalMemory - size, pool.unallocatedMemory());
        buffer = pool.allocate(2 * size, maxBlockTimeMs);
        pool.deallocate(buffer);
        assertEquals("All memory should be available", totalMemory, pool.availableMemory());
        assertEquals("Non-standard size didn't go to the free list.", totalMemory - size, pool.unallocatedMemory());
    }

    @Test
    public void testMultipleAllocationsAndDeallocations() throws Exception {
        long totalMemory = 64 * 1024;
        int size = 1024;
        File storefile = new File(System.getProperty("java.io.tmpdir"), "kafka-producer-data-" + new Random().nextInt(Integer.MAX_VALUE) + ".dat");
        MmapBufferPool pool = new MmapBufferPool(totalMemory, size, time, storefile);
        List<ByteBuffer> buffers = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            ByteBuffer buffer = pool.allocate(size, maxBlockTimeMs);
            buffers.add(buffer);

            int expectedPosition = i * size;
            assertEquals("buffer " + i + " should have position " + expectedPosition, expectedPosition, buffer.position() );
        }

        for (int i = 0; i < 2; i++)
            pool.deallocate(buffers.get(i));
        assertEquals("All memory should be available", totalMemory, pool.availableMemory());
        assertEquals("Two buffers on the free list", 2, pool.freeSize());
    }

    /**
     * Test that we cannot try to allocate more memory then we have in the whole pool
     */
    @Test(expected = IllegalArgumentException.class)
    public void testCantAllocateMoreMemoryThanWeHave() throws Exception {
        File storefile = new File(System.getProperty("java.io.tmpdir"), "kafka-producer-data-" + new Random().nextInt(Integer.MAX_VALUE) + ".dat");
        BufferPool pool = new MmapBufferPool(1024, 512, time, storefile);
        ByteBuffer buffer = pool.allocate(1024, maxBlockTimeMs);
        assertEquals(1024, buffer.limit());
        pool.deallocate(buffer);
        pool.allocate(1025, maxBlockTimeMs);
    }

    /**
     * Test that delayed allocation blocks
     */
    @Test
    public void testDelayedAllocation() throws Exception {
        File storefile = new File(System.getProperty("java.io.tmpdir"), "kafka-producer-data-" + new Random().nextInt(Integer.MAX_VALUE) + ".dat");
        BufferPool pool = new MmapBufferPool(5 * 1024, 1024, time, storefile);
        ByteBuffer buffer = pool.allocate(1024, maxBlockTimeMs);
        CountDownLatch doDealloc = asyncDeallocate(pool, buffer);
        CountDownLatch allocation = asyncAllocate(pool, 5 * 1024);
        assertEquals("Allocation shouldn't have happened yet, waiting on memory.", 1L, allocation.getCount());
        doDealloc.countDown(); // return the memory
        assertTrue("Allocation should succeed soon after de-allocation", allocation.await(1, TimeUnit.SECONDS));
    }

    private CountDownLatch asyncDeallocate(final BufferPool pool, final ByteBuffer buffer) {
        final CountDownLatch latch = new CountDownLatch(1);
        Thread thread = new Thread() {
            public void run() {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                pool.deallocate(buffer);
            }
        };
        thread.start();
        return latch;
    }

    private void delayedDeallocate(final BufferPool pool, final ByteBuffer buffer, final long delayMs) {
        Thread thread = new Thread() {
            public void run() {
                Time.SYSTEM.sleep(delayMs);
                pool.deallocate(buffer);
            }
        };
        thread.start();
    }

    private CountDownLatch asyncAllocate(final BufferPool pool, final int size) {
        final CountDownLatch completed = new CountDownLatch(1);
        Thread thread = new Thread() {
            public void run() {
                try {
                    pool.allocate(size, maxBlockTimeMs);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    completed.countDown();
                }
            }
        };
        thread.start();
        return completed;
    }

    /**
     * Test if Timeout exception is thrown when there is not enough memory to allocate and the elapsed time is greater than the max specified block time.
     * And verify that the allocation should finish soon after the maxBlockTimeMs.
     */
    @Test
    public void testBlockTimeout() throws Exception {
        File storefile = new File(System.getProperty("java.io.tmpdir"), "kafka-producer-data-" + new Random().nextInt(Integer.MAX_VALUE) + ".dat");
        BufferPool pool = new MmapBufferPool(10, 1, time, storefile);
        ByteBuffer buffer1 = pool.allocate(1, maxBlockTimeMs);
        ByteBuffer buffer2 = pool.allocate(1, maxBlockTimeMs);
        ByteBuffer buffer3 = pool.allocate(1, maxBlockTimeMs);
        // First two buffers will be de-allocated within maxBlockTimeMs since the most recent de-allocation
        delayedDeallocate(pool, buffer1, maxBlockTimeMs / 2);
        delayedDeallocate(pool, buffer2, maxBlockTimeMs);
        // The third buffer will be de-allocated after maxBlockTimeMs since the most recent de-allocation
        delayedDeallocate(pool, buffer3, maxBlockTimeMs / 2 * 5);

        long beginTimeMs = Time.SYSTEM.milliseconds();
        try {
            pool.allocate(10, maxBlockTimeMs);
            fail("The buffer allocated more memory than its maximum value 10");
        } catch (TimeoutException e) {
            // this is good
        }
        assertTrue("available memory" + pool.availableMemory(), pool.availableMemory() >= 9 && pool.availableMemory() <= 10);
        long endTimeMs = Time.SYSTEM.milliseconds();
        assertTrue("Allocation should finish not much later than maxBlockTimeMs", endTimeMs - beginTimeMs < maxBlockTimeMs + 1000);
    }

    /**
     * Test if the  waiter that is waiting on availability of more memory is cleaned up when a timeout occurs
     */
    @Test
    public void testCleanupMemoryAvailabilityWaiterOnBlockTimeout() throws Exception {
        File storefile = new File(System.getProperty("java.io.tmpdir"), "kafka-producer-data-" + new Random().nextInt(Integer.MAX_VALUE) + ".dat");
        BufferPool pool = new MmapBufferPool(2, 1, time, storefile);
        pool.allocate(1, maxBlockTimeMs);
        try {
            pool.allocate(2, maxBlockTimeMs);
            fail("The buffer allocated more memory than its maximum value 2");
        } catch (TimeoutException e) {
            // this is good
        }
        assertTrue(pool.queued() == 0);
    }

    /**
     * Test if the  waiter that is waiting on availability of more memory is cleaned up when an interruption occurs
     */
    @Test
    public void testCleanupMemoryAvailabilityWaiterOnInterruption() throws Exception {
        File storefile = new File(System.getProperty("java.io.tmpdir"), "kafka-producer-data-" + new Random().nextInt(Integer.MAX_VALUE) + ".dat");
        MmapBufferPool pool = new MmapBufferPool(2, 1, time, storefile);
        long blockTime = 5000;
        pool.allocate(1, maxBlockTimeMs);
        Thread t1 = new Thread(new BufferPoolTest.BufferPoolAllocator(pool, blockTime));
        Thread t2 = new Thread(new BufferPoolTest.BufferPoolAllocator(pool, blockTime));
        // start thread t1 which will try to allocate more memory on to the Buffer pool
        t1.start();
        // sleep for 500ms. Condition variable c1 associated with pool.allocate() by thread t1 will be inserted in the waiters queue.
        Thread.sleep(500);
        Deque<Condition> waiters = pool.waiters();
        // get the condition object associated with pool.allocate() by thread t1
        Condition c1 = waiters.getFirst();
        // start thread t2 which will try to allocate more memory on to the Buffer pool
        t2.start();
        // sleep for 500ms. Condition variable c2 associated with pool.allocate() by thread t2 will be inserted in the waiters queue. The waiters queue will have 2 entries c1 and c2.
        Thread.sleep(500);
        t1.interrupt();
        // sleep for 500ms.
        Thread.sleep(500);
        // get the condition object associated with allocate() by thread t2
        Condition c2 = waiters.getLast();
        t2.interrupt();
        assertNotEquals(c1, c2);
        t1.join();
        t2.join();
        // both the allocate() called by threads t1 and t2 should have been interrupted and the waiters queue should be empty
        assertEquals(pool.queued(), 0);
    }

    /**
     * This test creates lots of threads that hammer on the pool
     */
    @Test
    public void testStressfulSituation() throws Exception {
        int numThreads = 1;
        final int iterations = 50000;
        final int poolableSize = 1024;
        final long totalMemory = poolableSize * 5;
        File storefile = new File(System.getProperty("java.io.tmpdir"), "kafka-producer-data-" + new Random().nextInt(Integer.MAX_VALUE) + ".dat");
        final BufferPool pool = new MmapBufferPool(totalMemory, poolableSize, time, storefile);
        List<BufferPoolTest.StressTestThread> threads = new ArrayList<BufferPoolTest.StressTestThread>();
        for (int i = 0; i < numThreads; i++)
            threads.add(new BufferPoolTest.StressTestThread(pool, iterations));
        for (BufferPoolTest.StressTestThread thread : threads)
            thread.start();
        for (BufferPoolTest.StressTestThread thread : threads)
            thread.join();
        for (BufferPoolTest.StressTestThread thread : threads)
            assertTrue(thread.getName() + " should have completed all iterations successfully.", thread.success.get());
        assertEquals(totalMemory, pool.availableMemory());
    }
}
