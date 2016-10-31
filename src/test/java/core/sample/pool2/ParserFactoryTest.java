package core.sample.pool2;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.fail;



/**
 * Test case to test-
 *  1. object creation by factory
 *  2. object borrow from pool.
 *  3. returning object back to pool.
 * 
 * @author abhishek
 *
 */
public class ParserFactoryTest {
    private ParserPool<String, String> pool;
    private AtomicInteger count = new AtomicInteger(0);
    @Before
    public void setUp() throws Exception {
        GenericObjectPoolConfig config = new GenericObjectPoolConfig();
        config.setMaxIdle(1);
        config.setMaxTotal(3);
        // config.setMaxTotal(1);
        /*---------------------------------------------------------------------+
        |TestOnBorrow=true --> To ensure that we get a valid object from pool  |
        |TestOnReturn=true --> To ensure that valid object is returned to pool |
        +---------------------------------------------------------------------*/
        config.setTestOnBorrow(true);
        config.setTestOnReturn(true);
        pool = new ParserPool<String, String>(new ParserFactory<String, String>(), config);
    }
    @Test
    public void test() {
        try {
            int limit = 10;
            ExecutorService es = new ThreadPoolExecutor(10, 10, 0L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(limit));
            for (int i=0; i<limit; i++) {
                Runnable r = new Runnable() {
                    @Override
                    public void run() {
                        Parser<String, String> parser = null;
                        try {
                            parser = pool.borrowObject();
                            count.getAndIncrement();
                            parser.parse(null, null);
                        } catch (Exception e) {
                            e.printStackTrace(System.err);
                        } finally {
                            if (parser != null) {
                                pool.returnObject(parser);
                            }
                        }
                    }
                };
                es.submit(r);
            }
            es.shutdown();
            try {
                es.awaitTermination(1, TimeUnit.MINUTES);
            } catch (InterruptedException ignored) {}
            System.out.println("Pool Stats:\n Created:[" + pool.getCreatedCount() + "], Borrowed:[" + pool.getBorrowedCount() + "]");
            Assert.assertEquals(limit, count.get());
            Assert.assertEquals(count.get(), pool.getBorrowedCount());
            Assert.assertEquals(3, pool.getCreatedCount());
            //Assert.assertEquals(1, pool.getCreatedCount());
        } catch (Exception ex) {
            fail("Exception:" + ex);
        }
    }
}