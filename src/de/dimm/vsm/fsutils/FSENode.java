/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.fsutils;

import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.Exceptions.PoolReadOnlyException;
import de.dimm.vsm.net.RemoteFSElem;
import de.dimm.vsm.net.interfaces.FileHandle;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

/**
 *
 * @author Administrator
 */
public class FSENode
{
    boolean streamPath;
    RemoteFSElem fseNode;
    RemoteStoragePoolHandler remoteFSApi;
    boolean deleteOnClose;


    public FSENode( RemoteFSElem fseNode, RemoteStoragePoolHandler remoteFSApi )
    {
        this.fseNode = fseNode;
        this.remoteFSApi = remoteFSApi;
    }

    @Override
    public String toString()
    {
        return fseNode.toString(); 
    }

    @Override
    public boolean equals( Object obj )
    {
        if (obj instanceof FSENode)
        {
            FSENode n = (FSENode)obj;
            return (n.fseNode.getIdx() != 0 && n.fseNode.getIdx() == fseNode.getIdx());                
        }
        return super.equals( obj );
    }

    public boolean isDeleteOnClose()
    {
        return deleteOnClose;
    }

    public void setDeleteOnClose( boolean deleteOnClose )
    {
        this.deleteOnClose = deleteOnClose;
    }
    
    
    

    public boolean isStreamPath()
    {
        return streamPath;
    }

    public void setStreamPath( boolean streamPath )
    {
        this.streamPath = streamPath;
    }


    void l( String s)
    {
         //System.out.println("Log FSEN: " + s);
    }
    void ll( String s)
    {
        // NO CALL TO SERVER
         //System.out.println("Log FSEN: " + s);
    }

    public boolean exists() throws IOException
    {
        ll("exists");
        return true;
        //return remoteFSApi.exists(fseNode);

    }

    public String get_path()
    {
        ll("get_path");
        return fseNode.getPath();
    }

    public boolean isDirectory()
    {
        ll("isDirectory");
        return fseNode.isDirectory();
    }

    public int get_mode()
    {
        ll("get_mode");
        return fseNode.getPosixMode();
    }

    public long get_last_accessed()
    {
        ll("get_last_accessed");
        return fseNode.getAtimeMs();
    }

    public long get_creation_date()
    {
        ll("get_creation_date");
        return fseNode.getCtimeMs();
    }

    public long get_last_modified()
    {
        ll("get_last_modified");
        return fseNode.getMtimeMs();
    }

    public long get_size()
    {
        ll("get_size");
        return fseNode.getDataSize();
    }

    public String get_name()
    {
        ll("get_name");
        return fseNode.getName();
    }

//    public void set_ms_times( long toJavaTime, long toJavaTime0, long toJavaTime1 ) throws IOException, SQLException, PoolReadOnlyException
//    {
//        l("set_ms_times");
//        remoteFSApi.set_ms_times(fseNode., toJavaTime, toJavaTime0, toJavaTime1);
//    }

    public FileHandle open_file_handle( boolean create ) throws IOException, PoolReadOnlyException, SQLException, PathResolveException
    {
        l("open_file_handle");
        if (isStreamPath())
            return remoteFSApi.open_stream_handle(fseNode, create);

        return remoteFSApi.open_file_handle(fseNode, create);
    }

    public RemoteFSElem getNode()
    {
        ll("getNode");
        return fseNode;
    }

    public void set_mode( int mode ) throws IOException, SQLException, PoolReadOnlyException
    {
        l("set_mode");
        remoteFSApi.set_mode( fseNode, mode);
    }

    public void set_owner_id( int uid ) throws IOException, SQLException, PoolReadOnlyException
    {
        l("set_owner_id");
        remoteFSApi.set_owner_id(  fseNode, uid );
    }

    public void set_group_id( int gid ) throws IOException, SQLException, PoolReadOnlyException
    {
        l("set_group_id");
        remoteFSApi.set_group_id(  fseNode, gid );
    }
    public int get_owner_id(  )
    {
        ll("get_owner_id");
        return fseNode.getUid();
    }

    public int get_group_id(  )
    {
        ll("get_group_id");
        return fseNode.getGid();
    }

    public long get_unix_access_date()
    {
        ll("get_unix_access_date");
        return fseNode.getAtimeMs() / 1000;
    }

    public long get_timestamp()
    {
        ll("get_timestamp");
        return fseNode.getBase_ts();
    }

    public long get_unix_modification_date()
    {
        ll("get_unix_modification_date");
        return fseNode.getMtimeMs() / 1000;
    }

    public long get_GUID()
    {
        ll("get_GUID");
        return fseNode.getIdx();
    }

    public void set_attribute( String string, Integer valueOf ) throws IOException, SQLException, PoolReadOnlyException
    {
        l("set_attribute");
        remoteFSApi.set_attribute( fseNode, string,  valueOf );
    }

    public String read_symlink()
    {
        l("read_symlink");
        return remoteFSApi.read_symlink(fseNode);
    }

    public void create_symlink( String to ) throws IOException, PoolReadOnlyException
    {
        l("create_symlink");
        remoteFSApi.create_symlink( fseNode,to );
    }

    public void truncate( long size ) throws IOException, SQLException, PoolReadOnlyException
    {
        l("truncate");
        remoteFSApi.truncate( fseNode, size );
    }

    public void set_last_accessed( long l ) throws IOException, SQLException, PoolReadOnlyException
    {
        l("set_last_accessed");
        remoteFSApi.set_last_accessed( fseNode, l );
    }

    public void set_last_modified( long l ) throws IOException, SQLException, PoolReadOnlyException
    {
        l("set_last_modified");
        remoteFSApi.set_last_modified( fseNode, l );
    }

    public boolean isSymbolicLink()
    {
        ll("isSymbolicLink");
        return fseNode.isSymbolicLink();
    }

    public boolean isFile()
    {
        ll("isFile");
        return fseNode.isFile();
    }

    public String get_xattribute( String name ) throws SQLException
    {
        l("get_xattribute");
        return remoteFSApi.get_xattribute( fseNode, name );
    }

    public List<String> list_xattributes()
    {
        l("list_xattributes");
        return remoteFSApi.list_xattributes(fseNode);
    }

    public void add_xattribute( String name, String valStr )
    {
        l("add_xattribute");
        remoteFSApi.add_xattribute( fseNode, name,  valStr );
    }

    public void set_ms_times( long toJavaTime, long toJavaTime0, long toJavaTime1 ) throws IOException, SQLException, PoolReadOnlyException
    {
        remoteFSApi.set_ms_times(fseNode.getFileHandle(), toJavaTime, toJavaTime0, toJavaTime1);
    }


}
