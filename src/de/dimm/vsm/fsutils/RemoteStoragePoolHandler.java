/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.fsutils;

import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.Exceptions.PoolReadOnlyException;
import de.dimm.vsm.FSServerConnector;
import de.dimm.vsm.net.RemoteFSElem;
import de.dimm.vsm.net.StoragePoolWrapper;
import de.dimm.vsm.net.interfaces.FileHandle;
import de.dimm.vsm.net.interfaces.RemoteFSApi;
import de.dimm.vsm.net.interfaces.StoragePoolHandlerInterface;
import de.dimm.vsm.records.FileSystemElemNode;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.List;


class RemoteFileHandle implements FileHandle
{
    RemoteStoragePoolHandler remoteFSApi;
    
    long serverFhIdx;

    public RemoteFileHandle( RemoteStoragePoolHandler remoteFSApi, long idx )
    {
        this.remoteFSApi = remoteFSApi;
        
        serverFhIdx = idx;
    }

    void l( String s)
    {
       // System.out.println("Log RFHa: " + s);
    }


    public void force( boolean b ) throws IOException
    {
        l("force");
        remoteFSApi.force( serverFhIdx, b );
    }

    public int read( byte[] b, int length, long offset ) throws IOException
    {
        throw new RuntimeException("Not allowed from client, cannot write into buffer on stack");
    }
    public byte[] read( int length, long offset ) throws IOException
    {
        l("read");
        byte[] b = remoteFSApi.read( serverFhIdx,  length, offset );
        return b;
    }

    public void close() throws IOException
    {
        l("close");
        remoteFSApi.close( serverFhIdx );
    }

    public void create() throws IOException, PoolReadOnlyException
    {
        l("create");
        remoteFSApi.create( serverFhIdx );
        
    }

    public void truncateFile( long size ) throws IOException, SQLException, PoolReadOnlyException
    {
        l("truncateFile");
        remoteFSApi.truncateFile( serverFhIdx, size );
    }

    public void writeFile( byte[] b, int length, long offset ) throws IOException, SQLException, PoolReadOnlyException
    {
        l("writeFile");
        remoteFSApi.writeFile( serverFhIdx, b, length, offset );
    }

    public boolean delete()
    {
        l("delete");
         throw new RuntimeException("Cannot delete from remote");
    }

    public long length()
    {
        return remoteFSApi.length(serverFhIdx);
    }
}

/**
 *
 * @author Administrator
 */
public class RemoteStoragePoolHandler implements RemoteFSApi
{
    FSServerConnector fs_server_conn;
    StoragePoolHandlerInterface api;
    StoragePoolWrapper poolWrapper;
    InetAddress adr;
    int port;
    /*Date timestamp;
    String subPath;
    User user;
*/
    public RemoteStoragePoolHandler( StoragePoolWrapper pool/*, Date timestamp, String subPath, User user*/ )
    {
        this.poolWrapper = pool;
//        this.timestamp = timestamp;
//        this.subPath = subPath;
//        this.user = user;
        this.api = null;
        this.fs_server_conn = null;

        init();
    }

    void l( String s)
    {
        //System.out.println("Log RSPH: " + s);
    }

    final void init()
    {
        fs_server_conn = new FSServerConnector();
    }

    public StoragePoolHandlerInterface getApi()
    {
        return api;
    }


    public void disconnect()
    {
        l("disconnect");
        try
        {
            fs_server_conn.disconnect(adr, port);
        }
        catch (Exception e)
        {
        }
    }

    
    public void connect(String host, int port) throws UnknownHostException
    {
        l("connect");
        adr = InetAddress.getByName(host);
        this.port = port;
        api = fs_server_conn.getStoragePoolHandlerApi(adr, port, false, /*tcp*/ false);
    }
    public void connect(InetAddress adr, int port) throws UnknownHostException
    {
        l("connect");
        this.adr = adr;
        this.port = port;
        api = fs_server_conn.getStoragePoolHandlerApi(adr, port, false, /*tcp*/ false);
    }

    public RemoteFSElem create_fse_node( String fileName, String type ) throws IOException, PoolReadOnlyException, PathResolveException
    {
        l("create_fse_node");
        return api.create_fse_node(poolWrapper, fileName, type);
    }

    public RemoteFSElem resolve_node( String path ) throws SQLException
    {
        l("resolve_node");
        return api.resolve_node(poolWrapper, path);
    }

    public long getTotalBlocks()
    {
        l("getTotalBlocks");
        return api.getTotalBlocks(poolWrapper);
    }

    public long getUsedBlocks()
    {
        l("getUsedBlocks");
        return api.getUsedBlocks(poolWrapper);
    }

    public int getBlockSize()
    {
        l("getBlockSize");
        return api.getBlockSize(poolWrapper);
    }

    public void mkdir( String pathName ) throws IOException, PoolReadOnlyException, PathResolveException
    {
        l("mkdir");
        api.mkdir(poolWrapper, pathName);
    }

//    public long open_file_handle_no( String path, boolean create ) throws IOException
//    {
//        l("open_file_handle_no");
//        return api.open_file_handle_no(pool, path, create);
//    }



    public String getName()
    {
        l("getName");
        return api.getName(poolWrapper);
    }

    public boolean remove_fse_node( String path ) throws PoolReadOnlyException, SQLException
    {
        l("remove_fse_node");
        return api.delete_fse_node(poolWrapper, path);
    }

    public List<RemoteFSElem> get_child_nodes( RemoteFSElem node ) throws SQLException
    {
        l("get_child_nodes");
        return api.get_child_nodes(poolWrapper, node);
    }

    public void move_fse_node( String from, String to ) throws IOException, SQLException, PoolReadOnlyException, PathResolveException
    {
        l("move_fse_node");
        api.move_fse_node(poolWrapper, from, to);
    }

//    public void set_ms_times( RemoteFSElem fseNode, long toJavaTime, long toJavaTime0, long toJavaTime1 ) throws IOException, SQLException, PoolReadOnlyException
//    {
//        l("set_ms_times");
//        api.set_ms_times(pool, fseNode.getIdx(), toJavaTime, toJavaTime0, toJavaTime1 );
//    }
    public void set_ms_times( long idx, long toJavaTime, long toJavaTime0, long toJavaTime1 ) throws IOException, SQLException, PoolReadOnlyException
    {
        l("set_ms_times");
        api.set_ms_times(poolWrapper, idx, toJavaTime, toJavaTime0, toJavaTime1 );
    }
/*
    public FileHandle open_file_handle( FileSystemElemNode fseNode, boolean create )
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }*/

    public boolean exists( RemoteFSElem fseNode ) throws IOException
    {
        l("exists");
        return api.exists(poolWrapper, fseNode);
    }

    public void force( long idx, boolean b ) throws IOException
    {
        l("force");
        api.force( poolWrapper, idx, b );
    }

    public byte[] read( long idx, int length, long offset ) throws IOException
    {
        l("read");
        byte[] b = api.read( poolWrapper, idx, length, offset );
        return b;
    }
    public long length(long idx)
    {
        long l = api.length( poolWrapper, idx );
        return l;
    }

    public void close( long idx ) throws IOException
    {
        l("close");
        api.close_fh( poolWrapper, idx );
    }

    public void create( long idx ) throws IOException, PoolReadOnlyException
    {
        l("create");
        api.create( poolWrapper, idx );
    }

    public void truncateFile( long idx, long size ) throws IOException, SQLException, PoolReadOnlyException
    {
        l("truncateFile");
        api.truncateFile( poolWrapper, idx, size );
    }

    public void writeFile( long idx, byte[] b, int length, long offset ) throws IOException, SQLException, PoolReadOnlyException
    {
        l("writeFile");
        api.writeFile( poolWrapper, idx, b, length, offset );
    }

    public void delete( FileSystemElemNode fseNode ) throws IOException, PoolReadOnlyException
    {
        l("delete");
        throw new UnsupportedOperationException("Delete not yet supoorted");
    }

//    public FileHandle open_file_handle( String path, boolean b ) throws IOException
//    {
//        l("open_file_handle path");
//        RemoteFSElem node = resolve_node( path );
//        long idx;
//        if (node != null && node.getIdx() > 0)
//        {
//            idx = api.open_file_handle_no(pool, node.getIdx(), b);
//        }
//        else
//        {
//            idx = api.open_file_handle_no(pool, path, b);
//        }
//
//        RemoteFileHandle handle = new RemoteFileHandle(this,  idx);
//        return handle;
//    }
    public FileHandle open_file_handle( RemoteFSElem fseNode, boolean b ) throws IOException, PoolReadOnlyException, SQLException, PathResolveException
    {
        l("open_file_handle hno " + fseNode.getIdx());
        long handleNo = api.open_fh(poolWrapper, fseNode, b);
        // STOR IN FSENODE
        fseNode.setFileHandle(handleNo);

        RemoteFileHandle handle = new RemoteFileHandle(this,  handleNo);
        return handle;
    }

    public FileHandle open_stream_handle( RemoteFSElem fseNode, boolean b ) throws IOException, PoolReadOnlyException, SQLException, PathResolveException
    {
        l("open_file_handle hno " + fseNode.getIdx());
        long handleNo = api.open_stream(poolWrapper, fseNode, b);
        // STOR IN FSENODE
        fseNode.setFileHandle(handleNo);

        RemoteFileHandle handle = new RemoteFileHandle(this,  handleNo);
        return handle;
    }

    void set_attribute( RemoteFSElem fseNode, String string, Integer valueOf ) throws IOException, SQLException, PoolReadOnlyException
    {
        api.set_attribute(poolWrapper, fseNode, string, valueOf);
    }

    String read_symlink( RemoteFSElem fseNode )
    {
        return api.read_symlink(poolWrapper, fseNode);
    }

    void create_symlink( RemoteFSElem fseNode, String to ) throws IOException, PoolReadOnlyException
    {
        api.create_symlink(poolWrapper, fseNode, to);
    }

    void truncate( RemoteFSElem fseNode, long size ) throws IOException, PoolReadOnlyException
    {
        api.truncate(poolWrapper, fseNode, size);
    }

    void set_last_modified( RemoteFSElem fseNode, long l ) throws IOException, SQLException, PoolReadOnlyException
    {
        api.set_last_modified(poolWrapper, fseNode, l);
    }

    String get_xattribute( RemoteFSElem fseNode, String name ) throws SQLException
    {
        return api.get_xattribute(poolWrapper, fseNode, name);
    }

    void set_last_accessed( RemoteFSElem fseNode, long l ) throws IOException, SQLException, PoolReadOnlyException
    {
        api.set_last_accessed(poolWrapper, fseNode, l);
    }

    List<String> list_xattributes( RemoteFSElem fseNode )
    {
        return api.list_xattributes(poolWrapper, fseNode);
    }

    void add_xattribute( RemoteFSElem fseNode, String name, String valStr )
    {
        api.add_xattribute(poolWrapper, fseNode, name, valStr);
    }

    void set_mode( RemoteFSElem fseNode, int mode ) throws IOException, SQLException, PoolReadOnlyException
    {
        api.set_mode(poolWrapper, fseNode, mode);
    }

    void set_owner_id( RemoteFSElem fseNode, int uid ) throws IOException, SQLException, PoolReadOnlyException
    {
        api.set_owner_id(poolWrapper, fseNode, uid);
    }

    void set_group_id( RemoteFSElem fseNode, int gid ) throws IOException, SQLException, PoolReadOnlyException
    {
        api.set_group_id(poolWrapper, fseNode, gid);
    }

    public long open_file_handle_no( RemoteFSElem node, boolean create ) throws IOException, PoolReadOnlyException, SQLException, PathResolveException
    {
        l("open_file_handle_no");
        return api.open_fh(poolWrapper, node, create);
    }
}