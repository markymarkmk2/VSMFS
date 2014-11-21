/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.vfs;

import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.Exceptions.PoolReadOnlyException;
import de.dimm.vsm.net.RemoteFSElem;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

/**
 *
 * @author root
 */
public interface IVfsFsEntry
{
  

    public boolean isStreamPath();
    public void setStreamPath( boolean streamPath );
    public void close(long handleNo) throws IOException;


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

    public void setPosixMode( long handleNo, int mode ) throws IOException, SQLException, PoolReadOnlyException;
    public void setOwner( long handleNo, int uid ) throws IOException, SQLException, PoolReadOnlyException;

    public void setGroup( long handleNo, int gid ) throws IOException, SQLException, PoolReadOnlyException;
    public int getOwner(  );
    public int getGroup(  );
    public long getUnixAccessDate();

    public long getTimestamp();
    public long getUnixModificationDate();
    public long getGUID();

    public void setAttribute(  String string, Integer valueOf ) throws IOException, SQLException, PoolReadOnlyException;

    public String readSymlink();

    public void createSymlink( String to ) throws IOException, SQLException, PoolReadOnlyException;
    public void truncate( long handleNo, long size ) throws IOException, SQLException, PoolReadOnlyException;
    public void setLastAccessed( long handleNo, long l ) throws IOException, SQLException, PoolReadOnlyException;
    public void setLastModified( long handleNo, long l ) throws IOException, SQLException, PoolReadOnlyException;
    public boolean isSymbolicLink();
    public boolean isFile();

    public String getXattribute( String name ) throws SQLException;
    public List<String> listXattributes();
    
    public void addXattribute( String name, String valStr );
    public void setMsTimes( long handleNo, long toJavaTime, long toJavaTime0, long toJavaTime1 ) throws IOException, SQLException, PoolReadOnlyException;
  
    public void write( long handleNo, long offset, int len, byte[] data) throws IOException, SQLException, PoolReadOnlyException, PathResolveException;
    public void writeBlock( long handleNo, long offset, int len, String hash, byte[] data ) throws IOException, SQLException, PoolReadOnlyException, PathResolveException;
    
    public byte[] read( long handleNo, long offset, int len) throws IOException, SQLException, PoolReadOnlyException, PathResolveException;

    public void flush(long handleNo ) throws IOException, SQLException, PoolReadOnlyException, PathResolveException;

    public long open(boolean forWrite) throws IOException, SQLException, PoolReadOnlyException, PathResolveException;

    public void release();
    
    public IVfsFsEntry getParent();

    public void removeFromParent();

    public boolean isReadOnly( long vfsHandle ) throws IOException, SQLException, PoolReadOnlyException, PathResolveException;
    
}
