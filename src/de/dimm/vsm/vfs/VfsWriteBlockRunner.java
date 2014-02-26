/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.vfs;

import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.Exceptions.PoolReadOnlyException;
import de.dimm.vsm.Utilities.CryptTools;
import de.dimm.vsm.Utilities.QueueElem;
import de.dimm.vsm.Utilities.QueueRunner;
import de.dimm.vsm.VSMFSLogger;
import de.dimm.vsm.fsutils.RemoteStoragePoolHandler;
import fr.cryptohash.Digest;
import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 *
 * @author Administrator
 */
public class VfsWriteBlockRunner
{
    // CREATE URLSAFE ENCODE
    Digest digest = new fr.cryptohash.SHA1();
    
    protected RemoteStoragePoolHandler remoteFSApi;
    Exception hashException;
    Exception writeException;

    public VfsWriteBlockRunner( RemoteStoragePoolHandler remoteFSApi )
    {
        this.remoteFSApi = remoteFSApi;
    }        
    
    public void debug(String s)
    {
        VSMFSLogger.getLog().debug( toString() + " debug: " + s);
    }
    
    public void error(String s)
    {
        VSMFSLogger.getLog().debug( toString() + " error: " + s);
    }
    
    public void write_block( long handleNo, VfsBlock vfsBlock) throws IOException, SQLException, PoolReadOnlyException, PathResolveException
    {
//        // TODO: calc Hashes, send to Server, and if necessary put hashes into blockbuffer for server to fetch.
//        byte[] hash = digest.digest(vfsBlock.getData());
//        vfsBlock.setHash( CryptTools.encodeUrlsafe(hash) );
//        
//        debug( "Write Block " + vfsBlock.getOffset() + " len " + vfsBlock.getValidLen());   
//        
//        remoteFSApi.writeBlock(handleNo, vfsBlock.getHash(), vfsBlock.getData(), vfsBlock.getValidLen(), vfsBlock.getOffset());
        
        debug("Adding HQ " + vfsBlock.toString());
        hashQueue.addElem( new HashQueueElem( new HashWriteElem(handleNo, vfsBlock)));
    }  
    
    public  void close()
    {
        hashQueue.close();        
        writeQueue.close();
    }
    public boolean flushAndWait()
    {
        debug("flush queue ");
        
        hashQueue.flush();        
        writeQueue.flush();
        
        if (hashQueue.isWriteError())
        {
            error("WriteError in hashQueue");
            return false;
        }
        if (writeQueue.isWriteError())
        {
            error("WriteError in writeQueue");
            return false;
        }
        
        if (hashException != null)
        {
            error("Exception in hashQueue " +  hashException.getMessage());
            return false;
        }
        
        if (writeException != null)
        {
            error("Exception in writeQueue " +  writeException.getMessage());
            return false;
        }
            
        debug("done queue ");
        return true;        
    }
    
    class HashWriteElem
    {
         ReentrantLock hashReadyLock = new ReentrantLock();
         long handleNo; 
         VfsBlock vfsBlock;

        public HashWriteElem( long handleNo, VfsBlock vfsBlock )
        {
            this.handleNo = handleNo;
            this.vfsBlock = vfsBlock;
        }

        public ReentrantLock getHashReadyLock()
        {
            return hashReadyLock;
        }
        boolean writeBlock()
        {
            try
            {
                debug("write blk " + vfsBlock.toString());
                remoteFSApi.writeBlock(handleNo, vfsBlock.getHash(), vfsBlock.getData(), vfsBlock.getValidLen(), vfsBlock.getOffset());
                debug("done  blk " + vfsBlock.toString());
                return true;
            }
            catch (Exception exception)
            {
                writeException = exception;
                return false;
            }
        }
        boolean calcHash()
        {
            try
            {
                debug("Calc hash " + vfsBlock.toString());

                byte[] hash = digest.digest(vfsBlock.getData());
                vfsBlock.setHash(CryptTools.encodeUrlsafe(hash));     
                return true;
            }
            catch (Exception exception)
            {
                hashException = exception;
                return false;
            }
            finally
            {
                debug("unlock hs " + vfsBlock.toString());
                
                hashReadyLock.unlock();
            }
        }
    }
    
    ExecutorService service = Executors.newFixedThreadPool(8);
    
    
    QueueRunner hashQueue = new QueueRunner("HashBlockQueue", 8);
    QueueRunner writeQueue = new QueueRunner("WriteBlockQueue", 8);
    
    class WriteQueueElem extends QueueElem
    {
        HashWriteElem elem;

        public WriteQueueElem( HashWriteElem elem )
        {
            this.elem = elem;
        }
        @Override
        public boolean run()
        {
            try
            {
                elem.hashReadyLock.tryLock(30, TimeUnit.SECONDS);
            }
            catch (InterruptedException interruptedException)
            {
                writeException = interruptedException;
                return false;
            }
            debug("write blk " + elem.vfsBlock.toString());

            if (!elem.hashReadyLock.isLocked())                    
            {
                return elem.writeBlock();               
            }
            else
            {
                writeException = new IOException("Timeout while hashReadyLock tryLock");
            }
            return false;
        }        
    }
    
    class HashQueueElem extends QueueElem
    {
        HashWriteElem elem;

        public HashQueueElem( HashWriteElem elem )
        {
            this.elem = elem;
        }
        

        @Override
        public boolean run()
        {
            elem.hashReadyLock.lock();
            writeQueue.addElem( new WriteQueueElem(elem));
            
            service.execute( new Runnable() {

                @Override
                public void run()
                {
                    elem.calcHash();                    
                }
            });       
            
            return true;
        }        
    }
}
