/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.vfs;

import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.Exceptions.PoolReadOnlyException;
import de.dimm.vsm.Utilities.CryptTools;
import de.dimm.vsm.net.RemoteFSElem;
import de.dimm.vsm.net.interfaces.RemoteFSApi;
import fr.cryptohash.Digest;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.impl.SimpleLog;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 * @author Administrator
 */
public class VfsBufferedFileTest {
    
    static Log log;
    
    RemoteFSApi remoteFSApi;
        
    
    VfsWriteBlockRunner brunner;
            
    public VfsBufferedFileTest() {
        
    }
    
    @BeforeClass
    public static void setUpClass() {
        System.setProperty("org.apache.commons.logging.simplelog.defaultlog", "debug");
        log = new SimpleLog("Test");
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    
    Map<Long,String> wantHash = new HashMap<>();
    Map<Long,String> gotHash = new HashMap<>();


    

    /**
     * Test of write_block method, of class VfsBufferedFile.
     */
    @Test
    public void testWrite() throws Exception {
        log.debug("write");
        long handleNo;
        remoteFSApi = new MockRemoteFSApi(this);
        brunner = new VfsWriteBlockRunner(remoteFSApi);
        
        VfsBufferedFile instance = createInstance();        
        handleNo = instance.open(true);
        
        int bs = 1024*1024;
        
        int blocks = 10*1000;
        
        byte[] block = new byte[getWriteBlockSize()];
        for (int i = 0; i < block.length; i++) {
            block[i] = (byte)(i & 0xff);
        }
        
        for (long i = 0; i < blocks; i++) {           
            instance.write(handleNo, i * block.length, block.length, block);
            
            if (i % 1000 == 0)
                log.debug(brunner.printStatistics());
        }
        
        instance.close(handleNo);
        
        for (Long l: wantHash.keySet()) {
            String hash = gotHash.get(l);
            if (hash == null)
                throw new RuntimeException("Hash Missing Error");
            if (!hash.equals(wantHash.get(l)))
                throw new RuntimeException("Hash Difference Error");
        }
        
    }
    

    /**
     * Test of write_block method, of class VfsBufferedFile.
     */
    @Ignore
    @Test
    public void testWrite_block() throws Exception {
        log.debug("write_block");
        long handleNo;
        remoteFSApi = new MockBlockRemoteFSApi(this);
        brunner = new VfsWriteBlockRunner(remoteFSApi);
        
        VfsBufferedFile instance = createInstance();        
        handleNo = instance.open(true);
        
        int bs = 1024*1024;
        
        int blocks = 10*1000;
        
        
        for (long i = 0; i < blocks; i++) {
           
            VfsBlock vfsBlock = brunner.getNewFullBlock(i * bs, bs);
            byte[] s = Long.toString(i).getBytes();
            System.arraycopy(s, 0, vfsBlock.getData(), 0, s.length);
            wantHash.put(vfsBlock.getOffset(),  Long.toString(i));
            instance.write_block(handleNo, vfsBlock);
            if (i % 100 == 0)
                log.debug(brunner.printStatistics());
        }
        
        instance.close(handleNo);
        
        for (Long l: wantHash.keySet()) {
            String hash = gotHash.get(l);
            if (hash == null)
                throw new RuntimeException("Hash Missing Error");
            if (!hash.equals(wantHash.get(l)))
                throw new RuntimeException("Hash Difference Error");
        }
        
    }
    String calcHash( byte[] data ) throws UnsupportedEncodingException
    {
        Digest digest = new fr.cryptohash.SHA1();
        digest.reset();
        byte[] hash = digest.digest(data);
        return CryptTools.encodeUrlsafe(hash);          
    }

    private VfsBufferedFile createInstance() {
        try {
            IVfsHandler mockHandler = new VfsHandler(remoteFSApi);
            IVfsDir parent = new VfsDir(null, "/", RemoteFSElem.createDir("/"), remoteFSApi, mockHandler);
            File f = new File ("j:\\mock", "file");
            RemoteFSElem elem = new RemoteFSElem(f);
            VfsBufferedFile file = new VfsBufferedFile(parent, "j:\\mock\\file", elem, remoteFSApi, brunner);            
            file.setNewFile(true);
            return file;
        }
        catch (SQLException sQLException) {
        }
        return null;
    }

    int getWriteBlockSize() {
        return 12345;
    }
    
    
}