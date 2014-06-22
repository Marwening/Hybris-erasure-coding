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
package fr.eurecom.hybris;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.BinaryConnectionFactory;
import net.spy.memcached.MemcachedClient;

import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fr.eurecom.hybris.kvs.KvsManager;
import fr.eurecom.hybris.kvs.drivers.Kvs;
import fr.eurecom.hybris.mds.MdsManager;
import fr.eurecom.hybris.mds.Metadata;
import fr.eurecom.hybris.mds.Metadata.Timestamp;


/**
 * Hybris cloud storage library main class.
 * @author P. Viotti
 */
public class Hybris {

    private static final Logger logger = LoggerFactory.getLogger(Config.LOGGER_NAME);

    private MdsManager mds;
    private KvsManager kvs;

    /* caching */
    private MemcachedClient cache;
    private int cacheExp;   // default cache expiration timeout [s]
    private boolean cacheEnabled;
    private enum CachePolicy { ONREAD, ONWRITE };
    private CachePolicy cachePolicy;

    private final int quorum;

    /* read and write timeouts for cloud communications [s] */
    private final int TIMEOUT_WRITE;
    private final int TIMEOUT_READ;     // TODO

    /* GC */
    private final boolean gcEnabled;

    /* confidentiality */
    private final boolean cryptoEnabled;
    private byte[] IV;

    private final String clientId;


    /**
     * Creates a Hybris client with the parameters specified
     * in the given configuration file.
     * @param path to the Hybris properties file
     * @throws HybrisException
     */
    public Hybris(String propertiesFile) throws HybrisException {

        Config conf = Config.getInstance();
        try {
            conf.loadProperties(propertiesFile);
            this.mds = new MdsManager(conf.getProperty(Config.MDS_ADDR),
                    conf.getProperty(Config.MDS_ROOT));
            this.kvs = new KvsManager(conf.getProperty(Config.KVS_ACCOUNTSFILE),
                    conf.getProperty(Config.KVS_ROOT),
                    Boolean.parseBoolean(conf.getProperty(Config.KVS_TESTSONSTARTUP)));
        } catch (IOException e) {
            logger.error("Could not initialize Zookeeper or the cloud storage KvStores.", e);
            throw new HybrisException("Could not initialize Zookeeper or the cloud storage KvStores.", e);
        }

        this.cacheEnabled = Boolean.parseBoolean(conf.getProperty(Config.CACHE_ENABLED));
        if (this.cacheEnabled)
            try {
                Properties sysProp = System.getProperties();
                sysProp.put("net.spy.log.LoggerImpl", "net.spy.memcached.compat.log.Log4JLogger");
                System.setProperties(sysProp);
                this.cache = new MemcachedClient(new BinaryConnectionFactory(),
                        AddrUtil.getAddresses(conf.getProperty(Config.CACHE_ADDRESS)));
                this.cacheExp = Integer.parseInt(conf.getProperty(Config.CACHE_EXP));
                this.cachePolicy = CachePolicy.valueOf(conf.getProperty(Config.CACHE_POLICY).toUpperCase());
            } catch (Exception e) {
                logger.warn("Could not initialize the caching client. Please check its settings.", e);
                this.cacheEnabled = false;
            }

        int t = Integer.parseInt(conf.getProperty(Config.HS_F));
        this.quorum = t + 1;
        this.TIMEOUT_WRITE = Integer.parseInt(conf.getProperty(Config.HS_TO_WRITE));
        this.TIMEOUT_READ = Integer.parseInt(conf.getProperty(Config.HS_TO_READ));
        this.gcEnabled = Boolean.parseBoolean(conf.getProperty(Config.HS_GC));
        this.cryptoEnabled = Boolean.parseBoolean(conf.getProperty(Config.HS_CRYPTO));
        if (this.cryptoEnabled)
            this.IV = this.mds.getOrCreateIv();

        String cid = conf.getProperty(Config.HS_CLIENTID);
        if (cid != null)    this.clientId = cid;
        else                this.clientId = Utils.generateClientId();
    }

    /**
     * Creates a Hybris client.
     * @param zkAddress - list of comma separated addresses of Zookeeper cluster servers.
     * @param zkRoot - Zookeeper znode to adopt as root by the MdsManager. If not existing it will be created.
     * @param kvsAccountFile - path of the property file containing KVS accounts details.
     * @param kvsRoot - KVS container to adopt as root by the KVSs. If not existing it will be created.
     * @param kvsTestOnStartup - perform latency tests and sort KVSs accordingly.
     * @param clientId - clientId - if null, it will be randomly generated.
     * @param t - number of tolerated faulty KVS replicas.
     * @param writeTimeout - timeout to adopt when writing on KVSs (seconds).
     * @param readTimeout - timeout to adopt when reading from KVSs (seconds).
     * @param gcEnabled - enables KVS garbage collection.
     * @param cryptoEnabled - enables data confidentiality support.
     * @param cachingEnable - enables caching.
     * @param memcachedAddrs - list of comma separated addresses of Memcached servers.
     * @param cacheExp - caching default expiration timeout (seconds).
     * @param cachePolicy - caching policy, either "onread" or "onwrite"
     * @throws HybrisException
     */
    public Hybris(String zkAddress, String zkRoot,
            String kvsAccountFile, String kvsRoot, boolean kvsTestOnStartup,
            String clientId, int t, int writeTimeout, int readTimeout, boolean gcEnabled, boolean cryptoEnabled,
            boolean cachingEnable, String memcachedAddrs, int cacheExp, String cachePolicy) throws HybrisException {
        try {
            this.mds = new MdsManager(zkAddress, zkRoot);
            this.kvs = new KvsManager(kvsAccountFile, kvsRoot, kvsTestOnStartup);
        } catch (IOException e) {
            logger.error("Could not initialize Zookeeper or the cloud storage KvStores.", e);
            throw new HybrisException("Could not initialize Zookeeper or the cloud storage KvStores", e);
        }

        this.cacheEnabled = cachingEnable;
        if (this.cacheEnabled)
            try {
                Properties sysProp = System.getProperties();
                sysProp.put("net.spy.log.LoggerImpl", "net.spy.memcached.compat.log.Log4JLogger");
                System.setProperties(sysProp);
                this.cache = new MemcachedClient(new BinaryConnectionFactory(),
                        AddrUtil.getAddresses(memcachedAddrs));
                this.cacheExp = cacheExp;
                this.cachePolicy = CachePolicy.valueOf(cachePolicy.toUpperCase());
            } catch (Exception e) {
                logger.warn("Could not initialize the caching client. Please check its settings.", e);
                this.cacheEnabled = false;
            }

        this.quorum = Utils.DATACHUNKS + Utils.REDCHUNKS;
        this.TIMEOUT_WRITE = writeTimeout;
        this.TIMEOUT_READ = readTimeout;
        this.gcEnabled = gcEnabled;
        this.cryptoEnabled = cryptoEnabled;
        if (this.cryptoEnabled)
            this.IV = this.mds.getOrCreateIv();

        if (clientId != null)   this.clientId = clientId;
        else                    this.clientId = Utils.generateClientId();
    }


    /* ---------------------------------------------------------------------------------------
                                            Public APIs
       --------------------------------------------------------------------------------------- */

    /**
     * Writes a byte array associated with a key.
     * @param key
     * @param value
     * @return the list of Kvs in which Hybris stored the data
     * @throws HybrisException
     * @throws IOException 
     */
    public List<Kvs> put(String key, byte[] value) throws HybrisException, IOException {
//    	ArrayList<String> keylist = Utils.ercode(value, key);
//    	System.out.println("the keylist is"+keylist);
    	
    	//here we have a metadata initiation then a state definition 
        Timestamp ts;
        Stat stat = new Stat();
        Metadata md = this.mds.tsRead(key, stat);
        if (md == null) {
            ts = new Timestamp(0, this.clientId);
            stat.setVersion(MdsManager.NONODE);
        } else {
            ts = md.getTs();
            ts.inc( this.clientId );
        }

        byte[] cryptoKey = null;
        if (this.cryptoEnabled) {
            if (md == null || md.getCryptoKey() == null) {
                logger.debug("Generating new encryption key for key {}", key);
                cryptoKey = new byte[Utils.CRYPTO_KEY_LENGTH];
                cryptoKey = Utils.generateRandomBytes(cryptoKey);
            } else
                cryptoKey = md.getCryptoKey();

            try {
                logger.debug("Encrypting data for key {}", key);
                value = Utils.encrypt(value, cryptoKey, this.IV);
            } catch(GeneralSecurityException e) {
                logger.error("Could not encrypt data", e);
                cryptoKey = null;
            }
        }

        List<Kvs> savedChunksLst = new ArrayList<Kvs>();
        
        String kvsKey = Utils.getKvsKey(key, ts);
    	ArrayList<String> keylist = Utils.ercode(value, key);
        ExecutorService executor = Executors.newFixedThreadPool(this.quorum);
        CompletionService<Kvs> compServ = new ExecutorCompletionService<Kvs>(executor);
        int idxFrom = 0; int idxTo = this.quorum; long start; Future<Kvs> future;
        do {
            List<Kvs> kvsSublst = this.kvs.getKvsSortedByWriteLatency().subList(idxFrom, idxTo);
            start = System.currentTimeMillis();
            for (Kvs kvStore : kvsSublst)            
						try {
							compServ.submit(this.kvs.new KvsPutWorker(kvStore, keylist.get(kvsSublst.indexOf(kvStore)), Utils.keytovalue(keylist.get(kvsSublst.indexOf(kvStore)))));
						} catch (IOException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
						
                
            Kvs savedChunk = null;
            for (int i=0; i<kvsSublst.size(); i++)
                try {
                    future =  compServ.poll(this.TIMEOUT_WRITE, TimeUnit.SECONDS);
                    if (future != null && (savedChunk = future.get()) != null) {
                        logger.debug("Data stored on {}, {} ms", savedChunk,
                                System.currentTimeMillis() - start);
                        savedChunksLst.add(savedChunk);
                        if (savedChunksLst.size() >= this.quorum)
                            break;
                    }
                } catch (InterruptedException | ExecutionException e) {
                    logger.warn("Exception on write task execution", e);
                }

            idxFrom = idxTo;
            idxTo = this.kvs.getKvsList().size() > idxTo + this.quorum ?
                    idxTo + this.quorum : this.kvs.getKvsList().size();

        } while (savedChunksLst.size() < this.quorum && idxFrom < idxTo);
        executor.shutdown();

        if (savedChunksLst.size() < this.quorum) {
            if (this.gcEnabled) this.mds.new GcMarker(key, ts, keylist, savedChunksLst).start();
            logger.warn("Could not store data in cloud stores for key {}.", key);
            throw new HybrisException("Could not store data in cloud stores");
        }

        if (this.cacheEnabled && CachePolicy.ONWRITE.equals(this.cachePolicy))
            this.cache.set(kvsKey, this.cacheExp, value);

        boolean overwritten = false;
        try {
        	ArrayList<byte[]> chunkhashed = new ArrayList<byte []> ();
        	byte[] valhashed = new byte[20];
        	valhashed = Utils.getHash(value);
        	for (String Sigma : Utils.ercode(value, key)) 
        		chunkhashed.add(Utils.getHash(Utils.keytovalue(Sigma)));        	;
            Metadata newMd = new Metadata(ts, valhashed, chunkhashed, value.length, Utils.ercode(value, key), savedChunksLst, cryptoKey);
            System.out.println("just created Metadata"+ newMd);
           // System.out.println("keylist is " +Utils.ercode(value, key));
            overwritten = this.mds.tsWrite(key, newMd, stat.getVersion());
        } catch (HybrisException e) {
            if (this.gcEnabled) this.mds.new GcMarker(key, ts, keylist, savedChunksLst).start();
            logger.warn("Could not store metadata on Zookeeper for key {}.", key);
            throw new HybrisException("Could not store the metadata on Zookeeper");
        }

        if (this.gcEnabled && overwritten) this.mds.new GcMarker(key).start();

        logger.info("Data stored on: {}", savedChunksLst);
        return savedChunksLst;
    }


    /**
     * parallel GET function.
     * This function gets called to reach all the clouds and get a parallel values to decode.
     * due to Byzantine faults or concurrent GC.
     * @param key
     * @return a byte array containing the value associated with <key>.
     * @throws HybrisException
     * @throws IOException 
     */
    public byte[] get(String key) throws HybrisException, IOException {

        HybrisWatcher hwatcher = this.new HybrisWatcher();
        Metadata md = this.mds.tsRead(key, null, hwatcher);
        if (md == null || md.isTombstone()) {
            logger.warn("Could not find metadata associated with key {}.", key);
            return null;
        }

        String kvsKey = Utils.getKvsKey(key, md.getTs());
        ExecutorService executor = Executors.newFixedThreadPool(this.quorum);
        CompletionService<byte[]> compServ = new ExecutorCompletionService<byte[]>(executor);
        Future<byte[]> futureResult;
        byte[] value = null;
        boolean keepRetrieving = true;
        List<Kvs> kvsSublst = this.kvs.getKvsSortedByReadLatency();
        kvsSublst.retainAll(md.getChunksLst());
        Future<byte[]>[] futuresArray = new Future[kvsSublst.size()];
        ArrayList<byte[]> values= new ArrayList<byte[]>();
    	ArrayList<String> keylist=md.getkeylist();
        do {
        	for (Kvs kvStore : kvsSublst)
                 			futuresArray[kvsSublst.indexOf(kvStore)] = compServ.submit(this.kvs.new KvsGetWorker(kvStore, keylist.get(kvsSublst.indexOf(kvStore))));

            for (int i=0; i<kvsSublst.size(); i++)
                try {
                    if (hwatcher.isChanged()) {
                        for (Future<byte[]> future : futuresArray)
                            future.cancel(true);
                        return this.get(key);
                    }

                    futureResult =  compServ.poll(this.TIMEOUT_READ, TimeUnit.SECONDS);
                    if (futureResult != null && (value = futureResult.get()) != null)

                            if (this.cacheEnabled && CachePolicy.ONREAD.equals(this.cachePolicy))
                                this.cache.set(kvsKey, this.cacheExp, value);

                          /*  if (md.getCryptoKey() != null)
                                try {
                                    logger.debug("Decrypting data for key {}", key);
                                    value = Utils.decrypt(value, md.getCryptoKey(), this.IV);
                                } catch (GeneralSecurityException | UnsupportedEncodingException e) {
                                    logger.error("Could not decrypt data", e);
                                    throw new HybrisException("Could not decrypt data", e);
                                } */
                            if (Arrays.equals(md.getHashlist().get(i),Utils.getHash(value)))
                            	values.add(i,value);
    //                        keepRetrieving = false;
      //                      for (Future<byte[]> future : futuresArray)
        //                        future.cancel(true);
          //                  break;
                        

                } catch (InterruptedException | ExecutionException e) {
                    logger.warn("Exception on write task execution", e);
                }

        } while (keepRetrieving);
        executor.shutdown();
        
        int Sizy = md.getSize();
		value = Utils.dercode(values, keylist, kvsKey, Sizy );
        return value;
    }


    /**
     * Deletes data and metadata associated with <key>.
     * @param key
     * @throws HybrisException
     */
    public void delete(String key) throws HybrisException {

        Stat stat = new Stat();
        ArrayList<String> keylist = new ArrayList<String>();
        Metadata md = this.mds.tsRead(key, stat);
        if (md == null) {
            logger.debug("Could not find the metadata associated with key {}.", key);
            return;
        }
        Timestamp ts = md.getTs();
        keylist= md.getkeylist();
        ts.inc( this.clientId );
        Metadata tombstone = Metadata.getTombstone(ts);

        if (!this.gcEnabled) {
            String kvsKey = Utils.getKvsKey(keylist.get(0), md.getTs());
            for (Kvs kvStore : this.kvs.getKvsList()) {

                if (!md.getChunksLst().contains(kvStore))
                    continue;

                try {
                    this.kvs.delete(kvStore, kvsKey);
                } catch (IOException e) {
                    logger.warn("Could not delete {} from {}", kvsKey, kvStore);
                }
            }
        }

        this.mds.delete(key, tombstone, stat.getVersion());
    }


    /**
     * Lists keys by inquiring the MDS.
     * @return
     * @throws HybrisException
     */
    public List<String> list() throws HybrisException {
        return this.mds.list();
    }


    /**
     * Fetches all metadata currently stored on MDS.
     * XXX not scalable: for debugging purposes
     * @return
     * @throws HybrisException
     */
    public Map<String, Metadata> getAllMetadata() throws HybrisException {
        return this.mds.getAll();
    }


    /**
     * Tests KVSs latencies and sort them accordingly.
     * @param testDataSize [kB]
     */
    public void testLatencyAndSortClouds(int testDataSize) {
        this.kvs.testLatencyAndSortClouds(testDataSize);
    }


    /**
     * Stops Hybris client.
     * Closes connections and shuts off thread pools.
     */
    public void shutdown() {
        for (Kvs kvStore : this.kvs.getKvsList())
            this.kvs.shutdown(kvStore);
        this.mds.shutdown();
        if (this.cacheEnabled)
            this.cache.shutdown();
    }


    /* -------------------------------------- HybrisWatcher -------------------------------------- */

    /**
     * Class in charge of handling ZooKeeper notifications.
     * @author P. Viotti
     */
    public class HybrisWatcher implements CuratorWatcher {

        private boolean changed = false;
        public boolean isChanged() { return this.changed; }

        /**
         * Process a notification sent by ZooKeeper
         * @see org.apache.curator.framework.api.CuratorWatcher#process(org.apache.zookeeper.WatchedEvent)
         */
        public void process(WatchedEvent event) throws Exception {
            this.changed = true;
        }
    }


    /* -------------------------------------- GcManager -------------------------------------- */

    /**
     * Class in charge of performing garbage collection tasks.
     * @author P. Viotti
     */
    public class GcManager {

        /**
         * Deletes from KVSs all orphan or stale keys which are indexed on MDS.
         * @throws HybrisException
         */
        public void gc() throws HybrisException {

            // Orphans
            Map<String, Metadata> orphans = Hybris.this.mds.getOrphans();
            Set<String> orphanKeys = orphans.keySet();
            for (Iterator<String> it = orphanKeys.iterator(); it.hasNext();) {
                String kvsKey = it.next();
                Metadata md = orphans.get(kvsKey);
                boolean error = false;

                for (Kvs kvStore : Hybris.this.kvs.getKvsList()) {

                    if (!md.getChunksLst().contains(kvStore))
                        continue;

                    try {
                        Hybris.this.kvs.delete(kvStore, kvsKey);
                    } catch (IOException e) {
                        error = true;
                        logger.warn("GC: could not delete {} from {}", kvsKey, kvStore);
                    }
                }

                if (error) it.remove();
            }
            Hybris.this.mds.removeOrphanKeys(orphanKeys);

            // Stale
            List<String> staleKeys = Hybris.this.mds.getStaleKeys();
            for (String key : staleKeys)
                try {
                    this.gc(key);
                } catch (HybrisException e) {
                    logger.warn("GC: could not gc key {}", key);
                }
        }


        /**
         * Deletes from KVSs stale data associated with <key>.
         * @param key
         * @throws HybrisException
         */
        public void gc(String key) throws HybrisException {

            Metadata md = Hybris.this.mds.tsRead(key, null);
            if (md == null) {
                logger.debug("GC: could not find the metadata associated with key {}.", key);
                return;
            }

            for (Kvs kvStore : Hybris.this.kvs.getKvsList()) {

                List<String> kvsKeys;
                try {
                    kvsKeys = Hybris.this.kvs.list(kvStore);
                } catch (IOException e) {
                    logger.warn("GC: could not list {} container", kvStore);
                    continue;
                }

                for (String kvsKey : kvsKeys) {
                    String prefixKey = ""; Timestamp kvTs = null;
                    boolean malformedKey = false;
                    try {
                        prefixKey = Utils.getKeyFromKvsKey(kvsKey);
                        kvTs = Utils.getTimestampfromKvsKey(kvsKey);
                    } catch(IndexOutOfBoundsException e) {
                        malformedKey = true;
                    }

                    if ( malformedKey ||
                            key.equals(prefixKey) && md.getTs().isGreater(kvTs) )  {
                        try {
                            Hybris.this.kvs.delete(kvStore, kvsKey);
                        } catch (IOException e) {
                            logger.warn("GC: could not delete {} from {}", kvsKey, kvStore);
                            continue;
                        }
                        logger.debug("GC: deleted {} from {}", kvsKey, kvStore);
                    }
                }
            }

            Hybris.this.mds.removeStaleKey(key);
        }


        /**
         * Deletes from KVSs all the keys which are not present on MDS or obsolete or malformed.
         * Heads up: this function does a complete MDS dump and a complete KVS listing,
         * so it can be very slow and resource consuming.
         * @throws HybrisException
         */
        public void batchGc() throws HybrisException {

            Map<String, Metadata> mdMap = Hybris.this.mds.getAll();     // !! heavy operation

            for (Kvs kvStore : Hybris.this.kvs.getKvsList()) {

                List<String> kvsKeys;
                try {
                    kvsKeys = Hybris.this.kvs.list(kvStore);
                } catch (IOException e) {
                    logger.warn("GC: could not list {} container", kvStore);
                    continue;
                }

                for (String kvsKey : kvsKeys) {

                    String key = ""; Timestamp kvTs = null;
                    boolean malformedKey = false;
                    try {
                        key = Utils.getKeyFromKvsKey(kvsKey);
                        kvTs = Utils.getTimestampfromKvsKey(kvsKey);
                    } catch(IndexOutOfBoundsException e) {
                        malformedKey = true;
                    }

                    if ( malformedKey || !mdMap.keySet().contains(key) ||
                            mdMap.get(key).getTs().isGreater(kvTs) ) {
                        try {
                            Hybris.this.kvs.delete(kvStore, kvsKey);
                        } catch (IOException e) {
                            logger.warn("GC: could not delete {} from {}", kvsKey, kvStore);
                            continue;
                        }
                        logger.debug("GC: deleted {} from {}", kvsKey, kvStore);
                    }
                }
            }
            //mds.emptyStaleAndOrphansContainers();
        }


        /* -------------------------------------- TEMP / DEBUG -------------------------------------- */
        /**
         * XXX TEMP for testing and debugging - cleans up the KVS and MDS containers (!)
         * @throws HybrisException
         */
        public void _emptyContainers() throws HybrisException {
            Hybris.this.mds.emptyMetadataContainer();
            for (Kvs kvStore : Hybris.this.kvs.getKvsList())
                try {
                    Hybris.this.kvs.emptyStorageContainer(kvStore);
                } catch (IOException e) {
                    logger.warn("Could not empty {} container", kvStore);
                }
        }
    }
}
