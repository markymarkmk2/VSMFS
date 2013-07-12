/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.fsutils;

import de.dimm.vsm.Utilities.MaxSizeHashMap;
import de.dimm.vsm.VSMFSLogger;
import de.dimm.vsm.net.RemoteFSElem;
import de.dimm.vsm.vfs.IBufferedEventProcessor;
import de.dimm.vsm.vfs.IVfsEventProcessor;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Administrator
 */
public class VirtualFSFile implements IVirtualFSFile
{
    
    int blocksize = 1024*1024; // TODO: Should come from server
    // 
    public static final int MAX_WRITE_BLOCKS = 250;
    public static final int ASYNC_THRESHOLD_PERCENT = 10;
    public static final int MAX_WAIT_MS = 120*1000;
    
    boolean abort = false;
    RemoteFSElem elem;
    IBufferedEventProcessor eventProcessor;
    Map<Long,Block>blockMap = new HashMap<>();
    Semaphore sema = new Semaphore(MAX_WRITE_BLOCKS);
    
    Throwable asyncExc;
    
    long asyncFutureId = 0;
    
    MaxSizeHashMap<Long,Block> readBuffStack = new MaxSizeHashMap<>(30);    

    @Override
    public boolean existsBlock( long offset, int size)
    {
        Block bl = waitForBlockFinished(offset);
        
        return bl != null && bl.getData().length == size;        
    }

    @Override
    public RemoteFSElem getElem()
    {
        return elem;
    }
    
    

    
    @Override
    public void closeWrite() throws IOException
    {
        // On WriteClose we set all Blocks finished, maybe we have short write (Sparse?)
        for (Block bl: blockMap.values())
        {
            bl.setFinished(true);            
        }
        
        int lastSize = blockMap.size();
        // We are async ?
        if (asyncFutureId != 0)
        {
            try
            {
                while(true)
                {
                    if (abort)
                        break;
                    
                    // Wait and check if after 120 s at least one block was read from Server
                    boolean done = eventProcessor.waitProcess( asyncFutureId, MAX_WAIT_MS);
                    
                    // throw on error
                    if (asyncExc != null)
                        throw new IOException(asyncExc.getMessage(), asyncExc);
                    
                    // Ok
                    if (done && blockMap.isEmpty())
                        break;
                    
                    if (blockMap.size() == lastSize)
                    {
                        abort = true;                    
                        throw new IOException("Timeout beim AsyncLesen von "  + this.elem.getPath());
                    }
                    if (!blockMap.isEmpty())
                    {
                        abort = true;                    
                        throw new IOException("Abbruch beim AsyncLesen von "  + this.elem.getPath());
                    }
                    
                    lastSize = blockMap.size();
                    VSMFSLogger.getLog().debug("Warte auf " + lastSize + " Blöcke für " + this.elem.getPath());
                }
            }
            finally
            {
                boolean result = eventProcessor.fetchResult(asyncFutureId);
                if (!result)
                {
                    throw new IOException("Fehler beim AsyncSichern des VfsFileHandles von "  + this.elem.getPath());
                }
            }
        }
        else
        {
            List<RemoteFSElem> list = new ArrayList<>();
            list.add(elem);
            if (!eventProcessor.process(list))
            {
                throw new IOException("Fehler beim Sichern des VfsFileHandles");
            }        
        }
    }

    @Override
    public boolean closeRead()
    {
         VirtualFsFilemanager.getSingleton().removeFile(eventProcessor, elem.getPath());
         VSMFSLogger.getLog().debug("Closeread blocks ");                            
         blockMap.clear();
         return true;
    }
    
    
    class Block
    {
        long offset;
        byte[] data;
        private boolean finished;

        public Block( long offset, byte[] data )
        {
            this.offset = offset;
            this.data = data;
        }

        public byte[] getData()
        {
            return data;
        }  

        @Override
        public String toString()
        {
            return "Offs: " + offset + " Len: " + data.length;
        }        

        private boolean isFinished()
        {
            return finished;
        }

        public void setFinished( boolean finished )
        {
            this.finished = finished;
        }        
    }    

    public VirtualFSFile(IBufferedEventProcessor eventProcessor, RemoteFSElem elem)
    {
        this.eventProcessor = eventProcessor;
        this.elem = elem;
        
    }             

    @Override
    public void force( boolean b )
    {
    }

    @Override
    public int read( byte[] b, int length, long offset )
    {       
        int restLength = length;
        int destPos = 0;
        while(restLength > 0)
        {
            if (abort)
                break;
            long blockNr = offset / blocksize;
            long realOffset = blockNr*blocksize;
            
            Block block = waitForBlockFinished(realOffset);
            if (block == null)
                return -1;

            int offsetInBlock = (int)(offset - realOffset);
            int readLen = restLength;
            if (readLen > block.getData().length - offsetInBlock)
            {
                readLen = block.getData().length - offsetInBlock;
            }
            System.arraycopy(block.getData(), offsetInBlock, b, destPos, readLen);
            destPos += readLen;
            restLength -= readLen;
            offset = realOffset + blocksize;
        }
        return destPos;
    }
    long getSize()
    {
        return elem.getDataSize();
    }
    
    boolean warnedWait;
    
    Block waitForBlockFinished(long offset )
    {
        Block cachedBlock = readBuffStack.remove(offset);
        if (cachedBlock != null)
        {
            readBuffStack.put(offset, cachedBlock);
            return cachedBlock;
        }
        
        long startTime = System.currentTimeMillis();
        Block block = blockMap.get(offset );
        while (block == null || !block.isFinished())
        {
            if (!warnedWait)
            {
                VSMFSLogger.getLog().debug("Warte auf Block " + offset + "/" + getSize() + " für " + this.elem.getPath() + (block == null?" -> missing":" -> not finished"));
                warnedWait = true;
            }
            long now = System.currentTimeMillis();
            if (now - startTime > MAX_WAIT_MS)
            {
                VSMFSLogger.getLog().error("Timeout für Block " + offset + "/" + getSize() + " für " + this.elem.getPath());
                break;
            }
            
            if (abort)
            {
                VSMFSLogger.getLog().error("Abbruch für Block " + offset + "/" + getSize() + " für " + this.elem.getPath());
                break;
            }
            
            try
            {
                Thread.sleep(25);
            }
            catch (InterruptedException ex)
            {               
            }
            block = blockMap.get(offset );   
        }
        if (block != null && block.isFinished())
        {
            readBuffStack.put(offset, block);
            return block;
        }
        
        return null;
    }
    
    public byte[] fetchBlock( long offset, int size) throws IOException
    {
        Block block = waitForBlockFinished(offset);
               
        if (block != null)
        {
            // Fetige Blocks kommen in den readCache (read_andHash liest 2*)
            if (block.isFinished())
            {                
                // Nur wenn wir den Block aus der ReadMap entfernen, wird die Semap incr.
                Block removedBlock = blockMap.remove(offset ); 
                if (removedBlock != null)
                {
                    VSMFSLogger.getLog().debug("Rem block " + block);  
                    sema.release();
                    // Nicht noetig, ist schon drin von waitForBlockFinished()
                    //readBuffStack.put(offset, removedBlock);
                }
            }
            else
            {
                VSMFSLogger.getLog().error("block not finished " + block);                            
            }
                
            if (block.getData().length != size)
                throw new RuntimeException("Falsche Blockgröße bei fetchBlock");
            
            return block.getData();
        }
        throw new IOException("Timeout for Block " + offset);
    }       

    @Override
    public byte[] read( int length, long offset )
    {
        byte[] data = new byte[length];
        
        int rlen = read(data, length, offset);
        if (rlen < data.length)
        {
            byte[] tmp = data;
            data = new byte[rlen];
            System.arraycopy(tmp, 0, data, 0, rlen);
        }
        return data;
    }

    @Override
    public void create()
    {
        elem.setDataSize(0);
    }

    @Override
    public void truncateFile( long size )
    {
        elem.setDataSize(size);
        
        // Abort ?
        if (getSize() == 0)
        {
            abort();            
        }
    }
    
    private boolean isAsyncThresholdReached()
    {
        // START IMMEDIATELY ALWAYS
        return true;
        //return sema.availablePermits() < ( (100 - ASYNC_THRESHOLD_PERCENT)*MAX_WRITE_BLOCKS) / 100;
    }
    
    void tryStartAsyncFetch(long realOffset) throws IOException
    {
        if (asyncFutureId == 0)
        {
            VSMFSLogger.getLog().debug("Starte fsync vfs call bei block " + realOffset + " total: " + blockMap.size());
            List<RemoteFSElem> list = new ArrayList<>();
            list.add(elem);
            asyncFutureId = eventProcessor.startProcess(list);
        }
    }
    
    private void checkAsyncStart(long realOffset) throws IOException
    {                
        try
        {
            if (isAsyncThresholdReached())
            {
                tryStartAsyncFetch(realOffset);
            }
        }
        catch (IOException interruptedException)
        {
            VSMFSLogger.getLog().error("Sema.acquire bei write VirtualFSFile fehlgeschlagen", interruptedException );
            throw interruptedException;
        }
    }
    int getSemaPercentFree()
    {
        return (sema.availablePermits() * 100) / MAX_WRITE_BLOCKS;
    }

    private Block waitForNextBlock(long realOffset, int newLength) throws IOException
    {                
        try
        {
            // Throttle down Read speed:
            // if less than 20%  left then pause up to 1s per block
            int percentFree = getSemaPercentFree();
            if (percentFree < 20)
            {
                int pauseMs = (20 - percentFree) * 50;
                VSMFSLogger.getLog().debug("Slowing down write speed with delay " + Integer.toString(pauseMs) + " ms");
                sleepMs(pauseMs);
            }
                
            if (sema.availablePermits() == 0)
            {
                VSMFSLogger.getLog().debug("Warte auf Server bei block " + realOffset + " total: " + blockMap.size());                            
            }
            if (abort)
                return null;

            sema.acquire();
        }
        catch (InterruptedException ex)
        {
             VSMFSLogger.getLog().error("Sema.acquire bei write VirtualFSFile interrupted", ex );
             throw new IOException("Sema.acquire bei write VirtualFSFile interrupted", ex);
        }
        Block block = new Block(realOffset, new byte[newLength]);  
        return block;
    }

    @Override
    public void writeFile( byte[] b, int length, long offset ) throws IOException
    {
        
        if (getSize() < offset + length)
        {
            elem.setDataSize( offset + length);
        }
 
        int restLength = length;
        int destPos = 0;
        while(restLength > 0)
        {
            if (abort)
                break;
            long blockNr = offset / blocksize;
            long realOffset = blockNr*blocksize;
            int offsetInBlock = (int)(offset - realOffset);
            
            Block block = blockMap.get(Long.valueOf(realOffset) );
            if (block == null)
            {
                int newLength = blocksize;
                // Länge zu groß ?
                if (newLength > getSize() - realOffset)
                {
                    newLength = (int)(getSize() - realOffset);
                }
                
                // CHECK FOR START OF FETCH THREAD
                checkAsyncStart(realOffset);
                
                // CHECK FOR AVAILABLE NEW BLOCK (SEMA)
                block = waitForNextBlock(realOffset, newLength);
                
                blockMap.put(Long.valueOf(realOffset), block );
                VSMFSLogger.getLog().debug("Add block " + block + " total: " + blockMap.size());
            }
            if (abort)
                break;

            int writeLen = restLength;
            if (writeLen > block.getData().length - offsetInBlock)
            {
                writeLen = block.getData().length - offsetInBlock;
            }
            //VSMFSLogger.getLog().debug("ARC: " + destPos + "/" + offsetInBlock + "/" + writeLen + "/" );
            System.arraycopy(b, destPos, block.getData(), offsetInBlock, writeLen);
            
            // Block ist kompolett beschrieben
            if (offsetInBlock + writeLen == block.getData().length)
                block.setFinished(true);
            
            destPos += writeLen;
            restLength -= writeLen;
            offset = realOffset + blocksize;
        }                           
    }

    @Override
    public boolean delete()
    {
        VSMFSLogger.getLog().debug("Clr blocks");                            
        blockMap.clear();
        sema.drainPermits();
        sema.release(MAX_WRITE_BLOCKS);
        return true;
    }
    @Override
    public void abort()
    {
        VSMFSLogger.getLog().debug("Clr blocks");       
        abort = true;
        blockMap.clear();
        readBuffStack.clear();
        sema.drainPermits();
        sema.release(MAX_WRITE_BLOCKS);
        if (this.asyncFutureId != 0)
        {
            eventProcessor.abortProcess(this.asyncFutureId);
        }
    }

    @Override
    public long length()
    {
        return getSize();
    }

    @Override
    public boolean exists()
    {
        return true;
    }
    void sleepMs(int ms)
    {
        try
        {
            Thread.sleep(ms);
        }
        catch (InterruptedException ex)
        {
        }
    }        
}
