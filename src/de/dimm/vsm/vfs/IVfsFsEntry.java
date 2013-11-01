/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.vfs;

import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.Exceptions.PoolReadOnlyException;
import de.dimm.vsm.net.RemoteFSElem;
import de.dimm.vsm.net.interfaces.FileHandle;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

/**
 *
 * @author root
 */
public interface IVfsFsEntry
{
    public boolean isDeleteOnClose();
    public void setDeleteOnClose( boolean deleteOnClose );    

    public boolean isStreamPath();
    public void setStreamPath( boolean streamPath );
    public void close() throws IOException;


    public boolean exists() throws IOException;
    public String getPath();
    public boolean isDirectory();
    public int getPosixMode();
    public long getAtimeMs();
    public long getCtimeMs();
    public long getMtimeMs();
    public long getSize();
    public long getStreamSize();
    public String getName();


    public RemoteFSElem getNode();

    public void setPosixMode( int mode ) throws IOException, SQLException, PoolReadOnlyException;
    public void setOwner( int uid ) throws IOException, SQLException, PoolReadOnlyException;

    public void setGroup( int gid ) throws IOException, SQLException, PoolReadOnlyException;
    public int getOwner(  );
    public int getGroup(  );
    public long getUnixAccessDate();

    public long getTimestamp();
    public long getUnixModificationDate();
    public long getGUID();

    public void setAttribute( String string, Integer valueOf ) throws IOException, SQLException, PoolReadOnlyException;

    public String readSymlink();

    public void createSymlink( String to ) throws IOException, PoolReadOnlyException;
    public void truncate( long size ) throws IOException, SQLException, PoolReadOnlyException;
    public void setLastAccessed( long l ) throws IOException, SQLException, PoolReadOnlyException;
    public void setLastModified( long l ) throws IOException, SQLException, PoolReadOnlyException;
    public boolean isSymbolicLink();
    public boolean isFile();

    public String getXattribute( String name ) throws SQLException;
    public List<String> listXattributes();
    
    public void addXattribute( String name, String valStr );
    public void setMsTimes( long toJavaTime, long toJavaTime0, long toJavaTime1 ) throws IOException, SQLException, PoolReadOnlyException;
  
    public void write( long offset, int len, byte[] data) throws IOException, SQLException, PoolReadOnlyException;
    public byte[] read( long offset, int len)  throws IOException, SQLException;

    public void flush() throws IOException;


    public void release();

    public void setHandleNo( long handleNo );
    public long getHandleNo();
    public IVfsFsEntry getParent();

    public void removeFromParent();
    
}
