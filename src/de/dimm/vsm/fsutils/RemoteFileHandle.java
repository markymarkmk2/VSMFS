/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.fsutils;

import de.dimm.vsm.Exceptions.PoolReadOnlyException;
import de.dimm.vsm.VSMFSLogger;
import de.dimm.vsm.net.interfaces.FileHandle;
import java.io.IOException;
import java.sql.SQLException;

/**
 *
 * @author Administrator
 */
public class RemoteFileHandle implements FileHandle
{
    RemoteStoragePoolHandler remoteFSApi;
    
    long serverFhIdx;

    public RemoteFileHandle( RemoteStoragePoolHandler remoteFSApi, long serverFhIdx )
    {
        this.remoteFSApi = remoteFSApi;
        
        this.serverFhIdx = serverFhIdx;
    }

    void l( String s)
    {
        VSMFSLogger.getLog().debug(s);
    }


    @Override
    public void force( boolean b ) throws IOException
    {
        l("force");
        remoteFSApi.force( serverFhIdx, b );
    }

    @Override
    public int read( byte[] b, int length, long offset ) throws IOException
    {
        throw new RuntimeException("Not allowed from client, cannot write into buffer on stack");
    }
    @Override
    public byte[] read( int length, long offset ) throws IOException
    {
        l("unbuffered read len:" + length + " offs:" + offset);
        byte[] b = remoteFSApi.read( serverFhIdx,  length, offset );
        return b;
    }

    @Override
    public void close() throws IOException
    {
        l("close");
        remoteFSApi.close( serverFhIdx );
    }

    @Override
    public void create() throws IOException, PoolReadOnlyException
    {
        l("create");
        remoteFSApi.create( serverFhIdx );
        
    }

    @Override
    public void truncateFile( long size ) throws IOException, SQLException, PoolReadOnlyException
    {
        l("truncateFile");
        remoteFSApi.truncateFile( serverFhIdx, size );
    }

    @Override
    public void writeFile( byte[] b, int length, long offset ) throws IOException, SQLException, PoolReadOnlyException
    {
        l("writeFile");
        remoteFSApi.writeFile( serverFhIdx, b, length, offset );
    }

    @Override
    public boolean delete()
    {
        l("delete");
         throw new RuntimeException("Cannot delete from remote");
    }

    @Override
    public long length()
    {
        return remoteFSApi.length(serverFhIdx);
    }

    @Override
    public boolean exists()
    {
        return remoteFSApi.length(serverFhIdx) >= 0;
    }

}