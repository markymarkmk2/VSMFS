/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.vfs;

import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.Exceptions.PoolReadOnlyException;
import de.dimm.vsm.VSMFSLogger;
import de.dimm.vsm.net.RemoteFSElem;
import de.dimm.vsm.net.interfaces.RemoteFSApi;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

/**
 *
 * @author root
 */
public class VfsFile implements IVfsFile
{
    private boolean newFile;
    private boolean streamPath;
    protected String path;
    
    protected RemoteFSElem elem;
    protected RemoteFSApi remoteFSApi;
    protected boolean updatedFileSize;

    IVfsDir parent;
    
    protected VfsFile( IVfsDir parent, String path, RemoteFSElem elem, RemoteFSApi remoteFSApi )
    {
        this.parent = parent;
        this.path = path;
        this.elem = elem;
        this.remoteFSApi = remoteFSApi;
    }
    
    
    public static VfsFile createFile( IVfsDir parent, String path, RemoteFSElem elem, RemoteFSApi remoteFSApi, IWriteBlockRunner blockRunner)
    {
        VfsFile ret = new VfsBufferedFile( parent, path, elem, remoteFSApi, blockRunner );
        ret.setNewFile(true);
        return ret;
//        return new VfsFile( path, elem, remoteFSApi );
    }

    public boolean isNewFile()
    {
        return newFile;
    }

   
    

    public void setNewFile( boolean newFile )
    {
        this.newFile = newFile;
    }

    public void setUpdatedFileSize( boolean updatedFileSize )
    {
        this.updatedFileSize = updatedFileSize;
    }

    public boolean isUpdatedFileSize()
    {
        return updatedFileSize;
    }
    

    @Override
    public boolean isStreamPath()
    {
        return streamPath;
    }

    @Override
    public void setStreamPath( boolean streamPath )
    {
        this.streamPath = streamPath;
    }

    @Override
    public boolean exists() throws IOException
    {
        return elem != null;
    }

    @Override
    public String getPath()
    {
        return path;
    }

    @Override
    public boolean isDirectory()
    {
        return elem.isDirectory();
    }

    @Override
    public int getPosixMode()
    {
        return elem.getPosixMode();
    }

    @Override
    public long getAtimeMs()
    {
        return elem.getAtimeMs();
    }

    @Override
    public long getCtimeMs()
    {
        return elem.getCtimeMs();
    }

    @Override
    public long getMtimeMs()
    {
        return elem.getMtimeMs();
    }

    @Override
    public long getSize()
    {
        return elem.getDataSize();
    }

    @Override
    public String getName()
    {
        return elem.getName();
    }

    @Override
    public RemoteFSElem getNode()
    {
        return elem;
    }

    @Override
    public void setPosixMode( long handleNo, int mode ) throws IOException, SQLException, PoolReadOnlyException
    {
        elem.setPosixData( mode, elem.getUid(), elem.getGid(), elem.getUidName(), elem.getGidName() );        
        updateElem(handleNo);
    }

    @Override
    public void setOwner( long handleNo, int uid ) throws IOException, SQLException, PoolReadOnlyException
    {
        elem.setPosixData( elem.getPosixMode(), uid, elem.getGid(), elem.getUidName(), elem.getGidName() );
        updateElem(handleNo);
    }

    @Override
    public void setGroup( long handleNo, int gid ) throws IOException, SQLException, PoolReadOnlyException
    {
        elem.setPosixData( elem.getPosixMode(), elem.getUid(), gid, elem.getUidName(), elem.getGidName() );
        updateElem(handleNo);
    }

    @Override
    public int getOwner()
    {
        return elem.getUid();
    }

    @Override
    public int getGroup()
    {
        return elem.getGid();
    }

    @Override
    public long getUnixAccessDate()
    {
        return elem.getAtimeMs()/1000;
    }

    @Override
    public long getTimestamp()
    {
        return elem.getBase_ts();
    }

    @Override
    public long getUnixModificationDate()
    {
        return elem.getMtimeMs()/1000;
    }

    @Override
    public long getGUID()
    {
        return elem.getIdx();
    }

    @Override
    public void setAttribute( String string, Integer valueOf ) throws IOException, SQLException, PoolReadOnlyException
    {
        remoteFSApi.setAttribute( elem, string, valueOf);
    }

    @Override
    public String readSymlink()
    {
        return remoteFSApi.readSymlink(elem);
    }

    @Override
    public void createSymlink( String to ) throws IOException, SQLException, PoolReadOnlyException
    {
        remoteFSApi.createSymlink(elem, to);
    }

    @Override
    public void truncate( long handleNo, long size ) throws IOException, SQLException, PoolReadOnlyException
    {
        remoteFSApi.truncateFile( handleNo, size);
        getNode().setDataSize( size );
        touchMTime();
    }

    @Override
    public void setLastAccessed( long handleNo, long l ) throws IOException, SQLException, PoolReadOnlyException
    {
        remoteFSApi.set_ms_times(handleNo, elem.getCtimeMs(), l, elem.getMtimeMs() );
        elem.setAtimeMs(l);
    }

    @Override
    public void setLastModified( long handleNo, long l ) throws IOException, SQLException, PoolReadOnlyException
    {
        remoteFSApi.set_ms_times( handleNo, elem.getCtimeMs(), elem.getAtimeMs(), l );
        elem.setMtimeMs(l);
    }

    @Override
    public boolean isSymbolicLink()
    {
        return elem.isSymbolicLink();
    }

    @Override
    public boolean isFile()
    {
        return elem.isFile();
    }

    @Override
    public String getXattribute( String name ) throws SQLException
    {
        return remoteFSApi.getXattribute(elem, name);
    }

    @Override
    public List<String> listXattributes()
    {
        return remoteFSApi.listXattributes(elem);
    }

    @Override
    public void addXattribute( String name, String valStr )
    {
        remoteFSApi.addXattribute(elem, name, valStr);
    }

    @Override
    public void setMsTimes( long handleNo, long c, long a, long m ) throws IOException, SQLException, PoolReadOnlyException
    {
        remoteFSApi.set_ms_times(handleNo, c, a, m );
        if (c != 0)
            elem.setCtimeMs(c);
        if (a != 0)
            elem.setAtimeMs(a);
        if (m != 0)
            elem.setMtimeMs(m);
    }

    @Override
    public void write( long handleNo, long offset, int len, byte[] data ) throws IOException, SQLException, PoolReadOnlyException, PathResolveException
    {
        
        remoteFSApi.writeFile( handleNo, data, len, offset);
    }
    
    @Override
    public void writeBlock( long handleNo, long offset, int len, String hash, byte[] data ) throws IOException, SQLException, PoolReadOnlyException, PathResolveException
    {
        
        remoteFSApi.writeBlock( handleNo, hash, data, len, offset);
    }

    
    @Override
    public byte[] read( long handleNo, long offset, int len) throws IOException, SQLException, PoolReadOnlyException, PathResolveException
    {
        
        return remoteFSApi.read( handleNo, len, offset);        
    }

    @Override
    public void flush(long handleNo ) throws IOException, SQLException, PoolReadOnlyException, PathResolveException
    {
        
        remoteFSApi.force( handleNo, false );
    }

    @Override
    public long getStreamSize()
    {
        return elem.getStreamSize();
    }

    @Override
    public void release()
    {
        // TODO
    }

    public void updateElem(long handleNo )  throws IOException, SQLException, PoolReadOnlyException
    {
        remoteFSApi.updateElem(elem, handleNo);
    }

 

    @Override
    public void close(long handleNo ) throws IOException
    {
        //checkValidHandle();        
        remoteFSApi.close( handleNo );      
    }

    @Override
    public String toString()
    {
        return "File " + getPath();
    }

   
    public void debug(String s)
    {
        VSMFSLogger.getLog().debug( toString() + " debug: " + s);
    }
    public void error(String s)
    {
        VSMFSLogger.getLog().debug( toString() + " error: " + s);
    }

    @Override
    public IVfsDir getParent()
    {
        return parent;
    }

    @Override
    public void removeFromParent()
    {
        if (parent != null)
        {
            parent.removeChild(this);
        }
    }    
    
    protected void touchMTime()
    {
        getNode().setMtimeMs( System.currentTimeMillis());
    }

    @Override
    public long open(  boolean forWrite ) throws IOException, SQLException, PoolReadOnlyException, PathResolveException
    {
        
        long handleNo = remoteFSApi.open_file_handle_no( getNode(), forWrite);        
        return handleNo;
    }
    
    void checkValidOpen( long handleNo, boolean forWrite) throws IOException, SQLException, PoolReadOnlyException, PathResolveException
    {
        if (!isValidHandle(handleNo))
        {
            throw new IOException("Nicht offen Fehler fuer " + getNode().getPath());            
        }              
    }

    private boolean isValidHandle( long handleNo )
    {
        return handleNo >= 0;
    }

    @Override
    public boolean isReadOnly( long vfsHandle ) throws IOException, SQLException, PoolReadOnlyException, PathResolveException
    {
        return remoteFSApi.isReadOnly(vfsHandle);
    }
       
    
}
