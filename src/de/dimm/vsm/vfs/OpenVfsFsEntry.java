/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.vfs;

import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.Exceptions.PoolReadOnlyException;
import java.io.IOException;
import java.sql.SQLException;

/**
 *
 * @author root
 */
public class OpenVfsFsEntry implements IOpenVfsFsEntry
{
    private IVfsFsEntry entry;
    private long handleNo = 0;
    private long vfsHandleNo = 0;
    private boolean deleteOnClose;
    private boolean forWrite;
    private static long globalHandleCnt = 0;


    @Override
    public boolean isForWrite()
    {
        return forWrite;
    }

    @Override
    public void setForWrite( boolean forWrite )
    {
        this.forWrite = forWrite;
    }
    @Override
    public IVfsFsEntry getEntry()
    {
        return entry;
    }

    @Override
    public long getHandleNo()
    {
        return handleNo;
    }
    boolean isVfsOpen()
    {
        return vfsHandleNo > 0;
    }

    public OpenVfsFsEntry( IVfsFsEntry entry, boolean forWrite )
    {
        this.entry = entry;
        this.forWrite = forWrite;
        this.handleNo = getNextHandle();
    }
    OpenVfsFsEntry( IVfsFsEntry entry, long handleNo )
    {
        this.entry = entry;
        this.forWrite = false;
        this.handleNo = handleNo;
    }
    long getVfsHandle() throws IOException, SQLException, PoolReadOnlyException, PathResolveException
    {
        if (!isVfsOpen())
        {
            vfsHandleNo = entry.open(forWrite);
        }
        return vfsHandleNo;
    }

    boolean isValidHandle()
    {
        return handleNo >= 0;
    }

    @Override
    public boolean isDeleteOnClose()
    {
        return deleteOnClose;
    }

    @Override
    public void setDeleteOnClose( boolean deleteOnClose )
    {
        this.deleteOnClose = deleteOnClose;
    }

    @Override
    public void flush() throws IOException, SQLException, PoolReadOnlyException, PathResolveException
    {
        if (isVfsOpen())
            entry.flush(vfsHandleNo);
    }

    @Override
    public void write( long offset, int length, byte[] b ) throws IOException, SQLException, PoolReadOnlyException, PathResolveException
    {
        entry.write( getVfsHandle(), offset,  b.length, b );          
    }

    @Override
    public byte[] read( long offset, int capacity ) throws IOException, SQLException, PoolReadOnlyException, PathResolveException
    {
        byte[] b = entry.read( getVfsHandle(), offset, capacity);    
        return b;
    }

    @Override
    public void setMsTimes( long ctime, long atime, long mtime ) throws IOException, SQLException, PoolReadOnlyException, PathResolveException
    {
        entry.setMsTimes(getVfsHandle(), ctime, atime, mtime);
    }

    @Override
    public void truncate( long length ) throws IOException, SQLException, PoolReadOnlyException, PathResolveException
    {
        entry.truncate(getVfsHandle(), length);  
    }

    private long getNextHandle()
    {
        return ++globalHandleCnt;
    }

    @Override
    public void close() throws IOException
    {
        if (isVfsOpen())
        {
            entry.close(vfsHandleNo);
            vfsHandleNo = 0;
        }
    }

    @Override
    public boolean isReadOnly() throws IOException, SQLException, PoolReadOnlyException, PathResolveException
    {
        return entry.isReadOnly( getVfsHandle());
    }

    
}
