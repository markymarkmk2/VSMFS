/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.fsutils;

import de.dimm.vsm.Exceptions.PoolReadOnlyException;
import de.dimm.vsm.net.RemoteFSElem;
import de.dimm.vsm.net.interfaces.FileHandle;
import de.dimm.vsm.vfs.IVfsEventProcessor;
import java.io.IOException;
import java.sql.SQLException;

/**
 *
 * @author Administrator
 */

public class VirtualRemoteFileHandle implements FileHandle
{
    VirtualFSFile file;
    
    RemoteFSElem elem;

    public VirtualRemoteFileHandle( VirtualFSFile file,  RemoteFSElem elem )
    {
        this.file = file;        
        this.elem = elem;
    }
       

    public VirtualFSFile getFile()
    {
        return file;
    }

    
    @Override
    public void force( boolean b ) throws IOException
    {
        file.force(b);
    }

    @Override
    public int read( byte[] b, int length, long offset ) throws IOException
    {
        return file.read(  b,  length,  offset );
    }

    @Override
    public byte[] read( int length, long offset ) throws IOException
    {
        return file.read( length,  offset );
    }

    @Override
    public void close() throws IOException
    {
        file.closeWrite();
        
    }

    @Override
    public void create() throws IOException, PoolReadOnlyException
    {
        file.create();
    }

    @Override
    public void truncateFile( long size ) throws IOException, SQLException, PoolReadOnlyException
    {
        file.truncateFile(size);
        elem.setDataSize(size);
    }

    @Override
    public void writeFile( byte[] b, int length, long offset ) throws IOException, SQLException, PoolReadOnlyException
    {
        file.writeFile(  b,  length,  offset );
    }

    @Override
    public boolean delete() throws PoolReadOnlyException
    {
        return file.delete();
    }

    @Override
    public long length()
    {
        return file.length();
    }

    @Override
    public boolean exists()
    {
        return file.exists();
    }       
    
}
