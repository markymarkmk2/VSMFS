/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.vfs;

import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.Exceptions.PoolReadOnlyException;
import de.dimm.vsm.Utilities.CryptTools;
import de.dimm.vsm.Utilities.NamedThreadPoolExecutor;
import de.dimm.vsm.Utilities.QueueElem;
import de.dimm.vsm.Utilities.QueueRunner;
import de.dimm.vsm.VSMFSLogger;
import de.dimm.vsm.net.interfaces.RemoteFSApi;
import fr.cryptohash.Digest;
import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author Administrator
 */
public class VfsWriteBlockRunner implements IWriteBlockRunner {

    public static final int MAX_HASH_THREADS = 8;
    public static final int MAX_WRITE_QUEUE_LEN = 48;
    private static long MAX_HASH_CALC_TIME = 60;
    
    static final int blocksize = 1024*1024; // TODO: Should come from server
    
    NamedThreadPoolExecutor service;
    QueueRunner writeQueue = new QueueRunner("WriteBlockQueue", MAX_WRITE_QUEUE_LEN);
    protected RemoteFSApi remoteFSApi;
    Exception hashException;
    Exception writeException;
    BlockingQueue<HashCalculator> calcQueue = new LinkedBlockingQueue<>();
    BlockingQueue<VfsBlock> blockQueue = new LinkedBlockingQueue<>();

    public VfsWriteBlockRunner( RemoteFSApi remoteFSApi ) {
        this.remoteFSApi = remoteFSApi;
        service = new NamedThreadPoolExecutor("HashThread", MAX_HASH_THREADS, MAX_HASH_THREADS, MAX_HASH_THREADS, 60, TimeUnit.SECONDS, false);
    }

    public void debug( String s ) {
        System.err.println(s);
/*        try {
         VSMFSLogger.getLog().debug( toString() + " debug: " + s);
         }
         catch (Throwable thr)
         {
         thr.printStackTrace();
         }*/
    }

    public void error( String s ) {
        VSMFSLogger.getLog().debug(toString() + " error: " + s);
    }

    @Override
    public void write_block( long handleNo, VfsBlock vfsBlock ) throws IOException, SQLException, PoolReadOnlyException, PathResolveException {
//        // TODO: calc Hashes, send to Server, and if necessary put hashes into blockbuffer for server to fetch.
//        byte[] hash = digest.digest(vfsBlock.getData());
//        vfsBlock.setHash( CryptTools.encodeUrlsafe(hash) );
//        
//        debug( "Write Block " + vfsBlock.getOffset() + " len " + vfsBlock.getValidLen());   
//        
//        remoteFSApi.writeBlock(handleNo, vfsBlock.getHash(), vfsBlock.getData(), vfsBlock.getValidLen(), vfsBlock.getOffset());
        long n = vfsBlock.getOffset() / blocksize;
        HashCalculator calc = calcQueue.poll();
        if (calc == null) {
            calc = new HashCalculator();
        }
        HashWriteElem writeElem = new HashWriteElem(handleNo, vfsBlock);
        WriteQueueElem writeQueueEleme = new WriteQueueElem(writeElem, calc);
        calc.setNewBlock(writeElem);
        writeQueue.addElem(writeQueueEleme);

        debug("Adding WQ (" + writeQueue.getQueueLen() + ") " + vfsBlock.toString());

        // Blocking Call (
        service.execute(calc);
    }

    @Override
    public void close() {
        service.shutdown();
        try {
            service.awaitTermination(60, TimeUnit.SECONDS);
        }
        catch (InterruptedException interruptedException) {
            error("Exception in shutdown " + interruptedException.getMessage());
        }
        writeQueue.close();
    }

    @Override
    public boolean flushAndWait() {
        debug("flush queue ");

        writeQueue.flush();

        if (writeQueue.isWriteError()) {
            error("WriteError in writeQueue");
            return false;
        }

        if (hashException != null) {
            hashException.printStackTrace();
            error("Exception in hashQueue " + hashException.getMessage());
            hashException = null;
            return false;
        }

        if (writeException != null) {
            writeException.printStackTrace();
            error("Exception in writeQueue " + writeException.getMessage());
            writeException = null;
            return false;
        }

        debug("done queue ");
        return true;
    }

    class HashWriteElem {

        long handleNo;
        VfsBlock vfsBlock;

        public HashWriteElem( long handleNo, VfsBlock vfsBlock ) {
            this.handleNo = handleNo;
            this.vfsBlock = vfsBlock;
        }

        boolean writeBlock() {
            try {
                debug("write blk " + vfsBlock.toString());
                remoteFSApi.writeBlock(handleNo, vfsBlock.getHash(), vfsBlock.getData(), vfsBlock.getValidLen(), vfsBlock.getOffset());
                debug("done  blk " + vfsBlock.toString());
                returnFullBlock(vfsBlock);
                return true;
            }
            catch (Exception exception) {
                writeException = exception;
                exception.printStackTrace();
                return false;
            }
        }
    }

    class WriteQueueElem extends QueueElem {

        HashWriteElem elem;
        HashCalculator calc;

        public WriteQueueElem( HashWriteElem elem, HashCalculator calc ) {
            this.elem = elem;
            this.calc = calc;
        }

        @Override
        public boolean run() {
            if (!calc.waitForHashReady()) {
                return false;
            }
            calc.releaseHashReady();
            calcQueue.add(calc);

            boolean ret = elem.writeBlock();
            return ret;
        }
    }

    @Override
    public String printStatistics() {
        StringBuilder sb = new StringBuilder();
        sb.append("  WriteQueueLen: ");
        sb.append(writeQueue.getQueueLen());
        sb.append("  FreeCalcQueueLen: ");
        sb.append(calcQueue.size());
        sb.append("  FreeBlockQueueLen: ");
        sb.append(blockQueue.size());
        sb.append("  BusyThreadsLen: ");
        sb.append(service.getQueue().size());

        return sb.toString();
    }

    @Override
    public VfsBlock getNewFullBlock( long offset, int len ) {

        VfsBlock block = blockQueue.poll();
        if (block == null) {
            byte[] data = new byte[blocksize];
            block = new VfsBlock(offset, len, data);
        }
        else {
            block.setOffsetLen(offset, len);
        }

        return block;
    }
    @Override
    public VfsBlock getNewFullBlock( long offset, int len, int valid_len ) {

        VfsBlock block = blockQueue.poll();
        if (block == null) {
            byte[] data = new byte[blocksize];
            block = new VfsBlock(offset, len, valid_len, data);
        }
        else {
            block.setOffsetLenValidLen(offset, len, valid_len);
        }

        return block;
    }

    @Override
    public VfsBlock getNewFullBlock( long offset, int len, byte[] data ) {
         VfsBlock block = blockQueue.poll();
        if (block == null) {            
            block = new VfsBlock(offset, len,  data);
        }
        else {
            block.setOffsetLenData(offset, len, data);
        }

        return block;
    }
       @Override
    public VfsBlock getNewFullBlock( long offset, int len, int valid_len, byte[] data ) {
         VfsBlock block = blockQueue.poll();
        if (block == null) {            
            block = new VfsBlock(offset, len, valid_len, data);
        }
        else {
            block.setOffsetLenValidLenData(offset, len, valid_len, data);            
        }

        return block;
    }

    @Override
    public void returnFullBlock( VfsBlock block ) {
        if (block.getData().length == blocksize) {
            try {
                blockQueue.put(block);
            }
            catch (InterruptedException ex) {
                ex.printStackTrace();
            }
        }
    }

    class HashCalculator implements Runnable {

        Digest digest;
        Semaphore hashReady;
        VfsWriteBlockRunner.HashWriteElem actBlock;

        HashCalculator() {

            digest = new fr.cryptohash.SHA1();
            hashReady = new Semaphore(1);
        }

        void setNewBlock( VfsWriteBlockRunner.HashWriteElem block ) {
            try {
                actBlock = block;
                digest.reset();
                if (hashReady.availablePermits() != 1) {
                    throw new RuntimeException("Invalid State for HashCalculator");
                }
                hashReady.acquire();
            }
            catch (InterruptedException ex) {
                throw new RuntimeException("Error acquire Sema in HashCalculator", ex);
            }
        }

        void calcHash() {
            try {
                debug("Calc Hash " + actBlock.vfsBlock.toString());
                byte[] hash = digest.digest(actBlock.vfsBlock.getData());
                actBlock.vfsBlock.setHash(CryptTools.encodeUrlsafe(hash));
            }
            catch (Exception ex) {
                hashException = ex;
            }
            finally {
                hashReady.release();
            }
        }

        public boolean waitForHashReady() {
            try {
                return hashReady.tryAcquire(MAX_HASH_CALC_TIME, TimeUnit.SECONDS);
            }
            catch (InterruptedException ex) {
                throw new RuntimeException("Error acquire Sema in HashCalculator", ex);
            }
        }

        public void releaseHashReady() {
            hashReady.release();
        }

        @Override
        public void run() {
            calcHash();
        }
    }
}
