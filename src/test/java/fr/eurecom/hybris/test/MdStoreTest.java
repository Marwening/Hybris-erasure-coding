package fr.eurecom.hybris.test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import fr.eurecom.hybris.Config;
import fr.eurecom.hybris.HybrisException;
import fr.eurecom.hybris.Utils;
import fr.eurecom.hybris.kvs.CloudProvider;
import fr.eurecom.hybris.mds.MdStore;
import fr.eurecom.hybris.mds.Metadata;
import fr.eurecom.hybris.mds.Metadata.Timestamp;


public class MdStoreTest extends HybrisAbstractTest {
    
    private static MdStore mds;
    
    private static String MDS_TEST_ROOT = "mdstest-root";
    private static String MDS_ADDRESS = "localhost:2181";
    
    @BeforeClass
    public static void beforeClassSetup() throws IOException {
        Config.getInstance();
        mds = new MdStore(MDS_ADDRESS, MDS_TEST_ROOT);
    }

    // Executed before each test
    @Before
    public void setUp() throws Exception {
        mds.emptyMetadataContainer();
        mds.emptyStaleAndOrphansContainers();
    }

    @After
    public void tearDown() throws Exception {  }

    @Test
    public void testBasicWriteAndRead() throws HybrisException {
        
        String key = TEST_KEY_PREFIX + (new BigInteger(50, random).toString(32));
        Timestamp ts = new Timestamp(new BigInteger(10, random).intValue(), Utils.getClientId());
        byte[] hash = (new BigInteger(50, random).toString(10)).getBytes();
        List<CloudProvider> replicas = new ArrayList<CloudProvider>();
        replicas.add(new CloudProvider("transient", "A-accessKey", "A-secretKey", true, 0));
        replicas.add(new CloudProvider("transient", "B-accessKey", "B-secretKey", true, 0));
        replicas.add(new CloudProvider("transient", "C-accessKey", "C-secretKey", true, 0));
        Metadata md = new Metadata(ts, hash, replicas);
        
        mds.tsWrite(key, md, MdStore.NONODE);
        
        md = mds.tsRead(key, null);
        assertEquals(ts, md.getTs());
        assertTrue(Arrays.equals(hash, md.getHash()));
        assertTrue(Arrays.equals(replicas.toArray(), md.getReplicasLst().toArray()));
        
        mds.delete(key);
        assertNull(mds.tsRead(key, null));
    }
    
    @Test
    public void testOverwrite() throws HybrisException {
        
        String key = TEST_KEY_PREFIX + (new BigInteger(50, random).toString(32));
        List<CloudProvider> replicas = new ArrayList<CloudProvider>();
        replicas.add(new CloudProvider("transient", "A-accessKey", "A-secretKey", true, 0));
        byte[] hash = (new BigInteger(50, random).toString(10)).getBytes();
        Stat stat = new Stat();
        Metadata retrieved;
        String cid1 = "ZZZ";
        String cid2 = "AAA";
        
        mds.tsWrite(key, new Metadata(new Timestamp(0, cid1), hash, replicas), -1);  // znode does not exist, create hver. 0, zkver. 0
        mds.tsWrite(key, new Metadata(new Timestamp(0, cid2), hash, replicas), -1);  // NODEEXISTS retries because AAA > ZZZ, write hver. 0, zkver. 1
        
        retrieved = mds.tsRead(key, stat);
        assertEquals(0, retrieved.getTs().getNum());
        assertEquals(cid2, retrieved.getTs().getCid());
        assertEquals(1, stat.getVersion());
        
        mds.tsWrite(key, new Metadata(new Timestamp(1, cid1), hash, replicas), 1);   // write hver. 1, zkver. 2
        
        mds.tsWrite(key, new Metadata(new Timestamp(2, cid1), hash, replicas), 2);   // write hver. 2, zkver. 3
        try {
            mds.tsWrite(key, new Metadata(new Timestamp(2, cid1), hash, replicas), 2);   // BADVERSION, fails because cids are equals
            fail();
        } catch(HybrisException e) {  }
        mds.tsWrite(key, new Metadata(new Timestamp(2, cid2), hash, replicas), 2);       // BADVERSION, retries because AAA > ZZZ, write hver. 2, zkver. 4
        
        retrieved = mds.tsRead(key, stat);
        assertEquals(2, retrieved.getTs().getNum());
        assertEquals(cid2, retrieved.getTs().getCid());
        assertEquals(4, stat.getVersion());
        
        try{
            mds.tsWrite(key, new Metadata(new Timestamp(0, cid1), hash, replicas), 0);  // BADVERSION, fails because hver is smaller
            fail();
        } catch(HybrisException e) {  }

        mds.tsWrite(key, new Metadata(new Timestamp(3, cid1), hash, replicas), 1);  // BADVERSION, retries because 3 > 2, write hver. 3, zkver. 5
        
        retrieved = mds.tsRead(key, stat);
        assertEquals(3, retrieved.getTs().getNum());
        assertEquals(cid1, retrieved.getTs().getCid());
        assertEquals(5, stat.getVersion());
    }
    
    @Test
    public void testDeleteNotExistingKey() {
        
        String key = TEST_KEY_PREFIX + (new BigInteger(50, random).toString(32));
        try {
            mds.delete(key);
        } catch (HybrisException e) {
            e.printStackTrace();
            fail();
        }
    }
    
    @Test
    public void testReadNotExistingKey() {
        
        String key = TEST_KEY_PREFIX + (new BigInteger(50, random).toString(32));
        Metadata value = null;
        
        try {
            Stat stat = new Stat();
            stat.setVersion(MdStore.NONODE);
            value = mds.tsRead(key, stat);
            assertNull(value);
            assertEquals(MdStore.NONODE, stat.getVersion()); // in case of not existent znode, stat will remain unmodified
        } catch (HybrisException e) {
            e.printStackTrace();
            fail();
        }
    }
    
    @Test
    public void testTimestampOrdering() {
        // Timestamps differing for the clientIds
        Timestamp t1 = new Timestamp(2, "AAAAA");
        Timestamp t2 = new Timestamp(2, "ZZZZZ");
        assertTrue(t1.isGreater(t2));
        assertFalse(t2.isGreater(t1));
        
        // Timestamps differing for the num
        t1.setNum(3);
        t2.setNum(4);
        assertTrue(t2.isGreater(t1));
        assertFalse(t1.isGreater(t2));
        
        // Equal timestamps
        Timestamp t3 = new Timestamp(2, "XXXXXX");
        Timestamp t4 = new Timestamp(2, "XXXXXX");
        assertFalse(t3.isGreater(t4));
        assertFalse(t4.isGreater(t3));
        assertTrue(t3.equals(t4));
    }
    
    @Test
    public void testSerialization() throws HybrisException {
        
        String key = TEST_KEY_PREFIX + (new BigInteger(50, random).toString(32));
        Timestamp ts = new Timestamp(new BigInteger(10, random).intValue(), Utils.getClientId());
        byte[] hash = (new BigInteger(50, random).toString(10)).getBytes();
        List<CloudProvider> replicas = new ArrayList<CloudProvider>();
        replicas.add(new CloudProvider("transient", "A-accessKey", "A-secretKey", true, 10));
        replicas.add(new CloudProvider("transient", "B-accessKey", "B-secretKey", true, 20));
        replicas.add(new CloudProvider("transient", "C-accessKey", "C-secretKey", true, 30));
        
        Metadata md = new Metadata(ts, hash, replicas); 
        mds.tsWrite(key, md, MdStore.NONODE);
        
        md = mds.tsRead(key, null);
        for(CloudProvider provider : md.getReplicasLst()) {
            assertNotNull(provider.getId());
            assertEquals(replicas.get( md.getReplicasLst().indexOf(provider) ), provider);
            
            assertFalse(provider.isAlreadyUsed());
            assertFalse(provider.isEnabled());
            assertEquals(0, provider.getReadLatency());
            assertEquals(0, provider.getWriteLatency());
            assertEquals(0, provider.getCost());
            assertNull(provider.getAccessKey());
            assertNull(provider.getSecretKey());
        }
        
        mds.delete(key);
        assertNull(mds.tsRead(key, null));
    }
    
    @Test
    public void testList() throws HybrisException {
        
        String key1 = TEST_KEY_PREFIX + (new BigInteger(50, random).toString(32));
        String key2 = TEST_KEY_PREFIX + (new BigInteger(50, random).toString(32));
        String key3 = TEST_KEY_PREFIX + (new BigInteger(50, random).toString(32));
        String key4 = TEST_KEY_PREFIX + (new BigInteger(50, random).toString(32));
        String key5 = TEST_KEY_PREFIX + (new BigInteger(50, random).toString(32));
        List<String> keys = new LinkedList<String>();
        keys.add(key1); keys.add(key2); keys.add(key3); keys.add(key4); keys.add(key5);
        Timestamp ts = new Timestamp(new BigInteger(10, random).intValue(), Utils.getClientId());
        byte[] hash = (new BigInteger(50, random).toString(10)).getBytes();
        List<CloudProvider> replicas = new ArrayList<CloudProvider>();
        replicas.add(new CloudProvider("transient", "A-accessKey", "A-secretKey", true, 10));
        replicas.add(new CloudProvider("transient", "B-accessKey", "B-secretKey", true, 20));
        
        Metadata md = new Metadata(ts, hash, replicas);
        
        mds.tsWrite(key1, md, MdStore.NONODE);
        mds.tsWrite(key2, md, MdStore.NONODE);
        mds.tsWrite(key3, md, MdStore.NONODE);
        mds.tsWrite(key4, md, MdStore.NONODE);
        mds.tsWrite(key5, md, MdStore.NONODE);
        
        List<String> listedKeys = mds.list();
        assertEquals(5, listedKeys.size());
        for (String k : keys)
            assertTrue(listedKeys.contains(k));
        
        ts.inc(Utils.getClientId());
        md = new Metadata(ts, (new BigInteger(50, random).toString(10)).getBytes(), replicas);
        mds.tsWrite(key4, md, MdStore.NONODE);  // overwrites a key
        listedKeys = mds.list();
        assertEquals(5, listedKeys.size());
        Stat stat = new Stat();
        Metadata newMd = mds.tsRead(key4, stat);
        assertFalse(Arrays.equals(newMd.getHash(), hash));
        assertEquals(1, stat.getVersion());
        
        mds.delete(key3);               // remove a key
        listedKeys = mds.list();
        assertEquals(4, listedKeys.size());
        for (String k : keys)
            if (!k.equals(key3))
                assertTrue(listedKeys.contains(k));
            else
                assertFalse(listedKeys.contains(k));
        
        for (String k : keys)           // remove all keys
            mds.delete(k);
        listedKeys = mds.list();
        assertEquals(0, listedKeys.size());
        
        ts.inc(Utils.getClientId());    // add a key previously removed
        md = new Metadata(ts, (new BigInteger(50, random).toString(10)).getBytes(), replicas);
        mds.tsWrite(key2, md, -1);
        listedKeys = mds.list();
        assertEquals(1, listedKeys.size());
        assertEquals(key2, listedKeys.get(0));
    }
    
    @Test
    public void testGetAll() throws HybrisException {
        
        String key1 = TEST_KEY_PREFIX + (new BigInteger(50, random).toString(32));
        String key2 = TEST_KEY_PREFIX + (new BigInteger(50, random).toString(32));
        String key3 = TEST_KEY_PREFIX + (new BigInteger(50, random).toString(32));
        String key4 = TEST_KEY_PREFIX + (new BigInteger(50, random).toString(32));
        String key5 = TEST_KEY_PREFIX + (new BigInteger(50, random).toString(32));
        List<String> keys = new LinkedList<String>();
        keys.add(key1); keys.add(key2); keys.add(key3); keys.add(key4); keys.add(key5);
        Timestamp ts = new Timestamp(new BigInteger(10, random).intValue(), Utils.getClientId());
        byte[] hash = (new BigInteger(50, random).toString(10)).getBytes();
        List<CloudProvider> replicas = new ArrayList<CloudProvider>();
        replicas.add(new CloudProvider("transient", "A-accessKey", "A-secretKey", true, 10));
        replicas.add(new CloudProvider("transient", "B-accessKey", "B-secretKey", true, 20));
        
        Metadata md = new Metadata(ts, hash, replicas);
        
        mds.tsWrite(key1, md, MdStore.NONODE);
        mds.tsWrite(key2, md, MdStore.NONODE);
        mds.tsWrite(key3, md, MdStore.NONODE);
        mds.tsWrite(key4, md, MdStore.NONODE);
        mds.tsWrite(key5, md, MdStore.NONODE);
        
        Map<String, Metadata> allMd = mds.getAll();
        assertEquals(5, allMd.size());
        for (String k : keys) {
            assertTrue(allMd.keySet().contains(k));
            assertFalse(allMd.get(k).isTombstone());
            assertTrue(allMd.get(k).equals(md));
        }
        
        Timestamp ts4 = new Timestamp(ts.getNum() +1, Utils.getClientId());
        Metadata md4 = new Metadata(ts4, (new BigInteger(50, random).toString(10)).getBytes(), replicas);
        mds.tsWrite(key4, md4, MdStore.NONODE);  // overwrites a key
        allMd = mds.getAll();
        assertEquals(5, allMd.size());
        Stat stat = new Stat();
        Metadata newMd = mds.tsRead(key4, stat);
        assertFalse(Arrays.equals(newMd.getHash(), hash));
        assertEquals(1, stat.getVersion());
        
        mds.delete(key3);                       // remove a key
        allMd = mds.getAll();
        assertEquals(4, allMd.size());
        for (String k : keys)
            if (!k.equals(key3)) {
                assertTrue(allMd.keySet().contains(k));
                assertFalse(allMd.get(k).isTombstone());
                if (k.equals(key4))
                    assertEquals(md4, allMd.get(k));
                else
                    assertEquals(md, allMd.get(k));
            } else
                assertFalse(allMd.keySet().contains(k));
        
        for (String k : keys)                   // remove all keys
            mds.delete(k);
        allMd = mds.getAll();
        assertEquals(0, allMd.size());
        
        ts.inc(Utils.getClientId());            // add a key previously removed
        md = new Metadata(ts, (new BigInteger(50, random).toString(10)).getBytes(), replicas);
        mds.tsWrite(key2, md, -1);
        allMd = mds.getAll();
        assertEquals(1, allMd.size());
        assertEquals(md, allMd.get(key2));
    }
}