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
package fr.eurecom.hybris.mds;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import fr.eurecom.hybris.Config;
import fr.eurecom.hybris.Utils;
import fr.eurecom.hybris.kvs.KvsManager.KvsId;
import fr.eurecom.hybris.kvs.drivers.Kvs;

/**
 * Holds timestamped metadata.
 * @author P. Viotti
 */
public class Metadata implements KryoSerializable {

    public static class Timestamp implements KryoSerializable {

        private int num;
        private String cid;

        public Timestamp() { }
        public Timestamp(int n, String c) {
            this.num = n;
            this.cid = c;
        }

        public static Timestamp parseString(String tsStr) {
            String[] tsParts = tsStr.split("_");
            int n = Integer.parseInt(tsParts[0]);
            String cid = tsParts[1];
            return new Timestamp(n, cid);
        }

        public int getNum()             { return this.num; }
        public void setNum(int num)     { this.num = num; }
        public String getCid()          { return this.cid; }
        public void setCid(String cid)  { this.cid = cid; }

        public boolean isGreater(Timestamp ts) {
            if (ts == null || this.num > ts.num ||
                    this.num == ts.num && this.cid.compareTo(ts.cid) < 0)
                return true;
            return false;
        }

        public void inc(String client) {
            this.num = this.num + 1;
            this.cid = client;
        }

        public String toString() {
            return this.num + "_" + this.cid;
        }

        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + (this.cid == null ? 0 : this.cid.hashCode());
            result = prime * result + this.num;
            return result;
        }

        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (this.getClass() != obj.getClass())
                return false;
            Timestamp other = (Timestamp) obj;
            if (this.cid == null) {
                if (other.cid != null)
                    return false;
            } else if (!this.cid.equals(other.cid))
                return false;
            if (this.num != other.num)
                return false;
            return true;
        }

        public void write(Kryo kryo, Output output) {
            output.writeInt(this.num);
            output.writeAscii(this.cid);
        }
        public void read(Kryo kryo, Input input) {
            this.num = input.readInt();
            this.cid = input.readString();
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(Config.LOGGER_NAME);

    private Timestamp ts;
    private ArrayList<byte[]> hashlist;
    private byte[] hash;
    private ArrayList<String> keylist;
    private byte[] cryptoKey;
    private int size;
    private List<Kvs> chunksLst;

    public Metadata() { }
    public Metadata(Timestamp ts, byte[] hash ,ArrayList<byte[]> hashlist, int size, ArrayList<String> keylist,
            List<Kvs> chunks, byte[] cryptoKeyIV) {
        this.ts = ts;
        this.hash = hash;
        this.hashlist = hashlist;
        this.keylist = keylist;
        this.size = size;
        this.chunksLst = chunks;
        this.cryptoKey = cryptoKeyIV;
    }

    public Metadata(byte[] raw) {
        Kryo kryo = new Kryo();
        kryo.register(Metadata.class);
        kryo.register(Timestamp.class);
        Input input = new Input(raw);
        Metadata md = kryo.readObject(input, Metadata.class);
        input.close();
        this.ts = md.getTs();
        this.chunksLst = md.getChunksLst();
        this.hash = md.getHash();
        this.keylist = md.getkeylist();
        this.cryptoKey = md.getCryptoKey();
        this.size = md.getSize();
    }

    public static Metadata getTombstone(Timestamp ts) {
        return new Metadata(ts, null, null, 0,null, null, null);
    }

    public byte[] serialize() {
        Kryo kryo = new Kryo();
        kryo.register(Metadata.class);
        kryo.register(Timestamp.class);
        byte[] buff = new byte[512];
        Output output = new Output(buff);
        kryo.writeObject(output, this);
        output.close();
        return output.toBytes();
    }

    public boolean isTombstone() {
        return this.hash == null &&
        		this.keylist == null &&
                this.chunksLst == null &&
                this.size == 0 &&
                this.cryptoKey == null;
    }

    public Timestamp getTs() { return this.ts; }
    public void setTs(Timestamp ts) { this.ts = ts;    }
    public List<Kvs> getChunksLst() { return this.chunksLst; }
    public void setChunksLst(List<Kvs> chunksLst) { this.chunksLst = chunksLst; }
    public ArrayList<byte[]> getHashlist() { return this.hashlist; }
    public byte[] getHash() { return this.hash; }
    public void setHashlist(ArrayList<byte[]> hashlist) { this.hashlist = hashlist; }
    public void setHashlist(byte[] hash) { this.hash = hash; }
    public ArrayList<String> getkeylist() { return this.keylist; }
    public int getSize() { return this.size; }
    public void setSize(int s) { this.size = s; }
    public byte[] getCryptoKey() { return this.cryptoKey; }

    public String toString() {
        return "Metadata [ts=" + this.ts + ", hash=" + Utils.bytesToHexStr(this.hash)
                + ", size=" + this.size + ", replicasLst=" + this.chunksLst +", keylist ="+ this.keylist
                + ", cryptoKey=" + Utils.bytesToHexStr(this.cryptoKey) + "]";
    }

    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(this.cryptoKey);
        for (byte[] alfa : this.hashlist)
            result = prime * result + Arrays.hashCode(alfa);        	
      //  result = prime * result + Arrays.hashCode(this.hash);
        result = prime * result
                + (this.chunksLst == null ? 0 : this.chunksLst.hashCode());
        result = prime * result + this.size;
        result = prime * result + (this.ts == null ? 0 : this.ts.hashCode());
        return result;
    }

    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (this.getClass() != obj.getClass())
            return false;
        Metadata other = (Metadata) obj;
        if (!Arrays.equals(this.cryptoKey, other.cryptoKey))
            return false;
        for (byte [] alfa : this.hashlist)
         if (!Arrays.equals(alfa, other.hashlist.get(this.hashlist.indexOf(alfa))))
            return false;
        if (this.chunksLst == null) {
            if (other.chunksLst != null)
                return false;
        } else if (!this.chunksLst.equals(other.chunksLst))
            return false;
        if (this.size != other.size)
            return false;
        if (this.ts == null) {
            if (other.ts != null)
                return false;
        } else if (!this.ts.equals(other.ts))
            return false;
        return true;
    }

    public void write(Kryo kryo, Output out) {
        kryo.writeClassAndObject(out, this.ts);

        if (this.hash == null){
            byte[] ba = new byte[Utils.HASH_LENGTH];
            Arrays.fill(ba, (byte) 0x0);
            out.write(ba);
        } else
        	for (byte[] alfa : this.hashlist)
            	out.write(alfa);

        if (this.cryptoKey == null){
            byte[] ba = new byte[Utils.CRYPTO_KEY_LENGTH];
            Arrays.fill(ba, (byte) 0x0);
            out.write(ba);
        } else
            out.write(this.cryptoKey);

        out.writeInt(this.size);

        if (this.chunksLst != null)
            if (this.chunksLst.size() > 0)
                for (int i=0; i<this.chunksLst.size(); i++)
                    try {
                        out.writeShort( KvsId.valueOf( this.chunksLst.get(i).getId().toUpperCase() ).getSerial() );
                    } catch (IllegalArgumentException e) {
                        logger.error("Serialization of {} Kvs failed: Hybris could not find any suitable driver",
                                this.chunksLst.get(i).getId().toUpperCase());
                    }
            else
                out.writeShort(-1);     // empty replicas array
        else
            out.writeShort(-2);         // null replicas array
    }

	public void read(Kryo kryo, Input in) {
        this.ts = (Timestamp) kryo.readClassAndObject(in);
        this.hashlist = new ArrayList<byte[]>(Utils.DATACHUNKS+Utils.REDCHUNKS);
        for (int i=0; i< this.hashlist.size(); i++)
            this.hashlist.add(i, in.readBytes(Utils.HASH_LENGTH));
        byte[] ba = new byte[Utils.HASH_LENGTH];
        Arrays.fill(ba, (byte) 0x0);
        for (byte[] alfa : this.hashlist)
         if (Arrays.equals(ba, alfa))
            alfa = null;

        this.cryptoKey = in.readBytes(Utils.CRYPTO_KEY_LENGTH);
        ba = new byte[Utils.CRYPTO_KEY_LENGTH];
        Arrays.fill(ba, (byte) 0x0);
        if (Arrays.equals(ba, this.cryptoKey))
            this.cryptoKey = null;

        this.size = in.readInt();

        this.chunksLst = new ArrayList<Kvs>();
        while (true) {
            short rep;
            try {
                rep = in.readShort();
            } catch(Exception e) {
                break;
            }
            if (rep == -1)          // empty replicas array
                break;
            else if (rep == -2) {   // null replicas array
                this.chunksLst = null;
                break;
            }

            try {
                switch (KvsId.getIdFromSerial(rep)) {
                    case AMAZON:
                        this.chunksLst.add(new Kvs(KvsId.AMAZON.toString(), null, false, 0));
                        break;
                    case AZURE:
                        this.chunksLst.add(new Kvs(KvsId.AZURE.toString(), null, false, 0));
                        break;
                    case GOOGLE:
                        this.chunksLst.add(new Kvs(KvsId.GOOGLE.toString(), null, false, 0));
                        break;
                    case RACKSPACE:
                        this.chunksLst.add(new Kvs(KvsId.RACKSPACE.toString(), null, false, 0));
                        break;
                    case TRANSIENT:
                        this.chunksLst.add(new Kvs(KvsId.TRANSIENT.toString(), null, false, 0));
                        break;
                    default:
                        break;
                }
            } catch (IllegalArgumentException e) {
                logger.error("Deserialization of {} Kvs failed: Hybris could not find any suitable driver", rep);
            }
        }
    }
}
