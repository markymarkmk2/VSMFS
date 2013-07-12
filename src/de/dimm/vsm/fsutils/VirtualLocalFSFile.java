/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.fsutils;

import de.dimm.vsm.VSMFSLogger;
import de.dimm.vsm.net.RemoteFSElem;
import de.dimm.vsm.vfs.IBufferedEventProcessor;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 *
 * @author Administrator
 */
public class VirtualLocalFSFile implements IVirtualFSFile
{
    long size = 0;
    int blocksize = 1024*1024; // TODO: Should come from server
    // 
    public static final int MAX_WRITE_BLOCKS = 250;
    public static final int ASYNC_THRESHOLD_PERCENT = 10;
    public static final int MAX_WAIT_MS = 120*1000;
    
    File dataFile;
    
    
    boolean abort = false;
    RemoteFSElem elem;
    IBufferedEventProcessor eventProcessor;
    
    
    long asyncFutureId = 0;
    
    
    @Override
    public boolean existsBlock( long offset, int size)
    {
        return dataFile.exists() && dataFile.length() >= offset + size;
    }

    
    @Override
    public void closeWrite() throws IOException
    {
        eventProcessor.close(this);
    }

    @Override
    public boolean closeRead()
    {
         VirtualFsFilemanager.getSingleton().removeFile(eventProcessor, elem.getPath());
         VSMFSLogger.getLog().debug("Closeread LocalFsFile");                                     
         return true;
    }           

    public VirtualLocalFSFile(IBufferedEventProcessor eventProcessor, RemoteFSElem elem, File localFile)
    {
        this.eventProcessor = eventProcessor;
        this.elem = elem;
        this.dataFile = localFile;
    }

    public File getDataFile()
    {
        return dataFile;
    }

    @Override
    public RemoteFSElem getElem()
    {
        return elem;
    }            
    

    @Override
    public void force( boolean b )
    {
    }

    @Override
    public int read( byte[] b, int size, long offset )
    {       
        try (RandomAccessFile raf = new RandomAccessFile(dataFile, "r"))
        {
            raf.seek(offset);            
            int rlen = raf.read(b);            
            
            if (rlen < 0)
                throw new IOException( "Read -1 " + dataFile );
            
            if (rlen < b.length)
            {
                byte[] tmp = b;
                b = new byte[rlen];
                System.arraycopy(tmp, 0, b, 0, rlen);
            }
            return b.length;
        }
        catch(IOException exc)
        {
            VSMFSLogger.getLog().error("cannot read localfs " + dataFile.toString(), exc);
            return -1;
        }
    }
    
    boolean warnedWait;
    
    
    
    @Override
    public byte[] fetchBlock( long offset, int size) throws IOException
    {
        try (RandomAccessFile raf = new RandomAccessFile(dataFile, "r"))
        {
            raf.seek(offset);
            byte[] b = new byte[size];
            int rlen = raf.read(b);
            
            if (rlen < 0)
                throw new IOException( "Read -1 " + dataFile );
            
            if (rlen < b.length)
            {
                byte[] tmp = b;
                b = new byte[rlen];
                System.arraycopy(tmp, 0, b, 0, rlen);
            }
            return b;
        }
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
        size = 0;
    }

    @Override
    public void truncateFile( long size )
    {
        this.size = size;
        try (RandomAccessFile raf = new RandomAccessFile(dataFile, "rw"))
        {
            raf.setLength(size);            
        }        
        catch(IOException exc)
        {
            VSMFSLogger.getLog().error("cannot truncate localfs " + dataFile.toString());
        }
        // Abort ?
        if (size == 0)
        {
            abort();            
        }
    }
 
    

    @Override
    public void writeFile( byte[] b, int length, long offset ) throws IOException
    {
        try (RandomAccessFile raf = new RandomAccessFile(dataFile, "rw"))
        {
            raf.seek(offset);
            raf.write(b, 0, length);
        }
    }

    @Override
    public boolean delete()
    {
        VSMFSLogger.getLog().debug("Clr blocks");                            
        dataFile.delete();
        return true;
    }
    @Override
    public void abort()
    {
        VSMFSLogger.getLog().debug("Clr blocks");       
        abort = true;
        dataFile.delete();
    }

    @Override
    public long length()
    {
        return size;
    }

    @Override
    public boolean exists()
    {
        return dataFile.exists();
    }
    
}
