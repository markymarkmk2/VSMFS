/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.fsutils;

import de.dimm.vsm.net.RemoteFSElem;
import de.dimm.vsm.vfs.IBufferedEventProcessor;
import java.io.IOException;

/**
 *
 * @author Administrator
 */
public class VfsProxyFile implements IVirtualFSFile
{
    IVirtualFSFile delegate;
    RemoteFSElem elem;
    IBufferedEventProcessor processor;

    public VfsProxyFile( RemoteFSElem elem, IBufferedEventProcessor processor )
    {
        this.elem = elem;
        this.processor = processor;
    }

    public void setDelegate( IVirtualFSFile delegate )
    {
        this.delegate = delegate;
    }

    public IVirtualFSFile getDelegate()
    {
        return delegate;
    }
    
    void createDelegate(long size)
    {
        elem.setDataSize(size);
        delegate = processor.createDelegate(elem);
    }
    
    
            
    @Override
    public RemoteFSElem getElem()
    {
        return elem;
    }

    @Override
    public void abort()
    {
         delegate.abort();
    }

    @Override
    public boolean closeRead()
    {
        return delegate.closeRead();                    
    }

    @Override
    public void closeWrite() throws IOException
    {
        delegate.closeWrite();
    }

    @Override
    public void create()
    {
        
    }

    @Override
    public boolean delete()
    {
        return delegate.delete();
    }

    @Override
    public boolean exists()
    {
        return delegate.exists();
    }

    @Override
    public boolean existsBlock( long offset, int size )
    {
        return delegate.existsBlock(offset, size);
    }

    @Override
    public void force( boolean b )
    {
        delegate.force(b);
    }

    @Override
    public long length()
    {
        if (delegate != null)
            return delegate.length();
        return 0;
    }

    @Override
    public int read( byte[] b, int length, long offset )
    {
        return delegate.read(b, length, offset);
    }

    @Override
    public byte[] read( int length, long offset )
    {
        return delegate.read(length, offset);
    }

    @Override
    public byte[] fetchBlock( long offset, int size ) throws IOException
    {
        return delegate.fetchBlock(offset, size);
    }

    @Override
    public void writeFile( byte[] b, int length, long offset ) throws IOException
    {
        delegate.writeFile(b, length, offset);
    }

    @Override
    public void truncateFile( long size )
    {
        createDelegate(size);
    }
    
}
