/**
 * Copyright (C) 2013 EURECOM (www.eurecom.fr)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package fr.eurecom.hybris.test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.curator.test.TestingServer;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import fr.eurecom.hybris.Hybris;
import fr.eurecom.hybris.HybrisException;
import fr.eurecom.hybris.Utils;
import fr.eurecom.hybris.kvs.KvsManager;
import fr.eurecom.hybris.kvs.KvsManager.KvsPutWorker;
import fr.eurecom.hybris.kvs.drivers.Kvs;
import fr.eurecom.hybris.mds.MdsManager;
import fr.eurecom.hybris.mds.MdsManager.GcMarker;
import fr.eurecom.hybris.mds.Metadata.Timestamp;

@Ignore
public class HybrisGcTest extends HybrisAbstractTest {

    private final Hybris hybris;
    private final KvsManager kvs;
    private final MdsManager mds;

    private final String MDS_TEST_ROOT = "mdstest-root";

    private final String KVS_ROOT = "kvstest-root";
    private final String KVS_ACCOUNTS_FILE = "accounts.properties";

    public HybrisGcTest() throws Exception {
        zkTestingServer = new TestingServer();
        this.hybris = new Hybris(zkTestingServer.getConnectString(), this.MDS_TEST_ROOT, this.KVS_ACCOUNTS_FILE,
                this.KVS_ROOT, false, "clientId", 0, 600, 600, true, false, false, null, 0, "onwrite");
        this.mds = new MdsManager(zkTestingServer.getConnectString(), this.MDS_TEST_ROOT);
        this.kvs = new KvsManager(this.KVS_ACCOUNTS_FILE, this.KVS_ROOT, false);
    }

    // Executed before each test
    @Before
    public void setUp() throws Exception {
        this.mds.emptyMetadataContainer();
        this.mds.emptyStaleAndOrphansContainers();
        for (Kvs provider : this.kvs.getKvsList())
            try{
                this.kvs.emptyStorageContainer(provider);
            } catch (Exception e) {}
    }

    @Test
    public void testGcKey() throws HybrisException, IOException {

        String key = this.TEST_KEY_PREFIX + new BigInteger(50, this.random).toString(32);
        byte[] value1 = new BigInteger(50, this.random).toString(32).getBytes();
        byte[] value2 = new BigInteger(50, this.random).toString(32).getBytes();
        byte[] value3 = new BigInteger(50, this.random).toString(32).getBytes();

        // write 3 different versions of the same key and then gc
        this.hybris.put(key, value1);
        this.hybris.put(key, value2);
        this.hybris.put(key, value3);

        this.hybris.new GcManager().gc(key);

        // check that the outdated versions are gone
        for (Kvs provider : this.kvs.getKvsList()) {
            assertNull(this.kvs.get(provider, Utils.getKvsKey(key, new Timestamp(0, Utils.generateClientId()))));
            assertNull(this.kvs.get(provider, Utils.getKvsKey(key, new Timestamp(1, Utils.generateClientId()))));
        }

        // check that right versions are still there
        byte[] retrieved = this.hybris.get(key);
        assertNotNull(retrieved);
        assertTrue(Arrays.equals(value3, retrieved));
    }

    @Test
    public void testGc() throws HybrisException, IOException, InterruptedException {

        String key1 = this.TEST_KEY_PREFIX + new BigInteger(50, this.random).toString(32);
        String key2 = this.TEST_KEY_PREFIX + new BigInteger(50, this.random).toString(32);
        String key3 = this.TEST_KEY_PREFIX + new BigInteger(50, this.random).toString(32);
        byte[] value1 = new BigInteger(50, this.random).toString(32).getBytes();
        byte[] value2 = new BigInteger(50, this.random).toString(32).getBytes();
        byte[] value3 = new BigInteger(50, this.random).toString(32).getBytes();
        byte[] value4 = new BigInteger(50, this.random).toString(32).getBytes();
        byte[] value5 = new BigInteger(50, this.random).toString(32).getBytes();
        byte[] value6 = new BigInteger(50, this.random).toString(32).getBytes();

        // populate stales
        this.hybris.put(key1, value1);
        this.hybris.put(key1, value2);
        this.hybris.put(key1, value3);

        this.hybris.put(key2, value4);
        this.hybris.put(key2, value5);

        // populate orphans
        Timestamp ts = new Timestamp(1, Utils.generateClientId());
        List<Kvs> savedReplicas = new ArrayList<Kvs>();
        savedReplicas.add(this.kvs.getKvsList().get(0));
        savedReplicas.add(this.kvs.getKvsList().get(1));
        this.kvs.put(savedReplicas.get(0), Utils.getKvsKey(key3, ts), value6);
        this.kvs.put(savedReplicas.get(1), Utils.getKvsKey(key3, ts), value6);      
        ArrayList<String> keylist=Utils.ercode(value6, key3);
		GcMarker gcm = this.mds.new GcMarker(key3, ts, keylist, savedReplicas);
        gcm.start();
        gcm.join();

        // gc
        this.hybris.new GcManager().gc();

        // check that stale keys and orphans are gone
        for (Kvs provider : this.kvs.getKvsList()) {
            assertNull(this.kvs.get(provider, Utils.getKvsKey(key1, new Timestamp(0, Utils.generateClientId()))));
            assertNull(this.kvs.get(provider, Utils.getKvsKey(key1, new Timestamp(1, Utils.generateClientId()))));

            assertNull(this.kvs.get(provider, Utils.getKvsKey(key2, new Timestamp(0, Utils.generateClientId()))));
        }

        for (Kvs provider : savedReplicas)
            assertNull(this.kvs.get(provider, Utils.getKvsKey(key3, ts)));

        // check that right versions are still there
        byte[] retrieved = this.hybris.get(key1);
        assertNotNull(retrieved);
        assertTrue(Arrays.equals(value3, retrieved));

        retrieved = this.hybris.get(key2);
        assertNotNull(retrieved);
        assertTrue(Arrays.equals(value5, retrieved));

        retrieved = this.hybris.get(key3);
        assertNull(retrieved);
    }

    @Test
    public void testBatchGc() throws HybrisException, IOException, InterruptedException {

        // write random  stuff on kvs
        this.kvs.put(this.kvs.getKvsList().get(0), "malformedkey1", "fakevalue1".getBytes());
        this.kvs.put(this.kvs.getKvsList().get(1), "malformedkey2", "fakevalue2".getBytes());

        // write a couple of stales
        String key1 = this.TEST_KEY_PREFIX + new BigInteger(50, this.random).toString(32);
        String key2 = this.TEST_KEY_PREFIX + new BigInteger(50, this.random).toString(32);
        String key3 = this.TEST_KEY_PREFIX + new BigInteger(50, this.random).toString(32);
        byte[] value1 = new BigInteger(50, this.random).toString(32).getBytes();
        byte[] value2 = new BigInteger(50, this.random).toString(32).getBytes();
        byte[] value3 = new BigInteger(50, this.random).toString(32).getBytes();
        byte[] value4 = new BigInteger(50, this.random).toString(32).getBytes();
        byte[] value5 = new BigInteger(50, this.random).toString(32).getBytes();
        byte[] value6 = new BigInteger(50, this.random).toString(32).getBytes();

        // populate stales
        this.hybris.put(key1, value1);
        this.hybris.put(key1, value2);
        this.hybris.put(key1, value3);

        this.hybris.put(key2, value4);
        this.hybris.put(key2, value5);

        // make up a couple of orphans
        Timestamp ts = new Timestamp(1, Utils.generateClientId());
        List<Kvs> savedReplicas = new ArrayList<Kvs>();
        savedReplicas.add(this.kvs.getKvsList().get(0));
        savedReplicas.add(this.kvs.getKvsList().get(1));
        this.kvs.put(savedReplicas.get(0), Utils.getKvsKey(key3, ts), value6);
        this.kvs.put(savedReplicas.get(1), Utils.getKvsKey(key3, ts), value6);
        ArrayList<String> keylist = Utils.ercode(value6, key3);
		GcMarker gcm = this.mds.new GcMarker(key3, ts, keylist, savedReplicas);
        gcm.start();
        gcm.join();
        
        
        // make a put test
        
        List<Kvs> savedReplicas1 = new ArrayList<Kvs>();
        savedReplicas1.add(this.kvs.getKvsList().get(0));
        savedReplicas1.add(this.kvs.getKvsList().get(1));
        savedReplicas1.add(this.kvs.getKvsList().get(2));

        ArrayList<String> keylist1 = Utils.ercode(value6, key3);
    	for (Kvs kvStore : savedReplicas)
        	for (String Alfa : keylist1)
        		if (keylist1.indexOf(Alfa)==savedReplicas.indexOf(kvStore))
        			this.kvs.put(savedReplicas1.get(savedReplicas.indexOf(kvStore)), Utils.getKvsKey(Alfa, ts), Utils.keytovalue(Alfa));
        
        
        
        
        
        
        
        // batchGc
        this.hybris.new GcManager().batchGc();

        // make sure stales and orphans are gone
        assertNull(this.kvs.get(this.kvs.getKvsList().get(0), "malformedkey1"));
        assertNull(this.kvs.get(this.kvs.getKvsList().get(1), "malformedkey2"));

        for (Kvs provider : this.kvs.getKvsList()) {
            assertNull(this.kvs.get(provider, Utils.getKvsKey(key1, new Timestamp(0, Utils.generateClientId()))));
            assertNull(this.kvs.get(provider, Utils.getKvsKey(key1, new Timestamp(1, Utils.generateClientId()))));

            assertNull(this.kvs.get(provider, Utils.getKvsKey(key2, new Timestamp(0, Utils.generateClientId()))));
        }

        for (Kvs provider : savedReplicas)
            assertNull(this.kvs.get(provider, Utils.getKvsKey(key3, ts)));

        // check that right versions are still there
        byte[] retrieved = this.hybris.get(key1);
        assertNotNull(retrieved);
        assertTrue(Arrays.equals(value3, retrieved));

        retrieved = this.hybris.get(key2);
        assertNotNull(retrieved);
        assertTrue(Arrays.equals(value5, retrieved));

        retrieved = this.hybris.get(key3);
        assertNull(retrieved);
    }
}