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
    boolean buffered;
    int writeBufferBlockSize;
    int readBufferBlockSize;

    public RemoteStoragePoolHandler( StoragePoolWrapper pool/*, Date timestamp, String subPath, User user*/ )
    {
        this.poolWrapper = pool;
        this.api = null;
        this.fs_server_conn = null;

        init();
    }

    void l( String s)
    {
        System.err.println("Log RSPH: " + s);
    }

    final void init()
    {
        fs_server_conn = new FSServerConnector();
    }

    public StoragePoolHandlerInterface getApi()
    {
        return api;
    }

    public void setBuffered( boolean buffered )
    {
        this.buffered = buffered;
    }

    public boolean isBuffered()
    {
        return buffered;
    }

    public void setReadBufferBlockSize( int readBufferBlockSize )
    {
        this.readBufferBlockSize = readBufferBlockSize;
    }

    public void setWriteBufferBlockSize( int writeBufferBlockSize )
    {
        this.writeBufferBlockSize = writeBufferBlockSize;
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

    @Override
    public RemoteFSElem create_fse_node( String fileName, String type ) throws IOException, PoolReadOnlyException, PathResolveException
    {
        l("create_fse_node");
        return api.create_fse_node(poolWrapper, fileName, type);
    }

    @Override
    public RemoteFSElem resolve_node( String path ) throws SQLException
    {
        l("resolve_node");
        return api.resolve_node(poolWrapper, path);
    }
    long lastTotalBlocks;
    long lastUsedBlocks;
    int  lastBlockSize;
    long lastStafsTS = 0;
    
    void checkStatfs()
    {
        long now = System.currentTimeMillis();
        if (now - lastStafsTS > 120*1000) 
        {
            lastTotalBlocks = api.getTotalBlocks(poolWrapper);
            lastUsedBlocks = api.getUsedBlocks(poolWrapper);
            lastBlockSize = api.getBlockSize(poolWrapper);
            lastStafsTS = now;
        }
    }

    @Override
    public long getTotalBlocks()
    {
        l("getTotalBlocks");
        checkStatfs();
        return lastTotalBlocks;
    }

    @Override
    public long getUsedBlocks()
    {
        l("getUsedBlocks");
        checkStatfs();
        return lastUsedBlocks;
    }

    @Override
    public int getBlockSize()
    {
        l("getBlockSize");
        checkStatfs();
        return lastBlockSize;
    }

    @Override
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



    @Override
    public String getName()
    {
        l("getName");
        return api.getName(poolWrapper);
    }

    @Override
    public boolean remove_fse_node( String path ) throws PoolReadOnlyException, SQLException
    {
        l("remove_fse_node");
        return api.delete_fse_node(poolWrapper, path);
    }

    @Override
    public List<RemoteFSElem> get_child_nodes( RemoteFSElem node ) throws SQLException
    {
        l("get_child_nodes");
        return api.get_child_nodes(poolWrapper, node);
    }

    @Override
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
    @Override
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

    @Override
    public boolean exists( RemoteFSElem fseNode ) throws IOException
    {
        l("exists");
        return api.exists(poolWrapper, fseNode);
    }

    @Override
    public boolean isReadOnly( long idx ) throws IOException, SQLException
    {
        l("exists");
        return api.isReadOnly(poolWrapper, idx);
    }

    @Override
    public void force( long idx, boolean b ) throws IOException
    {
        l("force");
        api.force( poolWrapper, idx, b );
    }

    @Override
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

    @Override
    public void close( long idx ) throws IOException
    {
        l("close");
        api.close_fh( poolWrapper, idx );
    }

    @Override
    public void create( long idx ) throws IOException, PoolReadOnlyException
    {
        l("create");
        api.create( poolWrapper, idx );
    }

    @Override
    public void truncateFile( long idx, long size ) throws IOException, SQLException, PoolReadOnlyException
    {
        l("truncateFile");
        api.truncateFile( poolWrapper, idx, size );
    }

    @Override
    public void writeFile( long idx, byte[] b, int length, long offset ) throws IOException, SQLException, PoolReadOnlyException
    {
        l("RealwriteFile at " + offset + " len " + length);
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
    public FileHandle open_file_handle( RemoteFSElem fseNode, boolean forWrite ) throws IOException, PoolReadOnlyException, SQLException, PathResolveException
    {
        long handleNo;
        l("open_file_handle hno " + fseNode.getIdx());
        handleNo = api.open_fh(poolWrapper, fseNode.getIdx(), forWrite);
        
        if (handleNo == -1)
            throw new IOException("Cannot open file " + fseNode.getPath() );

        // STOR IN FSENODE
        fseNode.setFileHandle(handleNo);

        FileHandle handle = isBuffered() ? 
            new BufferedRemoteFileHandle(this, handleNo, readBufferBlockSize, writeBufferBlockSize) :
            new RemoteFileHandle(this,  handleNo);
        return handle;
    }

    public FileHandle open_stream_handle( RemoteFSElem fseNode, boolean forWrite ) throws IOException, PoolReadOnlyException, SQLException, PathResolveException
    {
        l("open_file_handle hno " + fseNode.getIdx());
        long handleNo = api.open_stream(poolWrapper, fseNode.getIdx(), forWrite);
        if (handleNo == -1)
            throw new IOException("Cannot open file " + fseNode.getPath() );
        
        // STOR IN FSENODE
        fseNode.setFileHandle(handleNo);

        RemoteFileHandle handle = isBuffered() ?
            new BufferedRemoteFileHandle(this, handleNo, readBufferBlockSize, writeBufferBlockSize) :
            new RemoteFileHandle(this,  handleNo);
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

    @Override
    public long open_file_handle_no( RemoteFSElem node, boolean create ) throws IOException, PoolReadOnlyException, SQLException, PathResolveException
    {
        l("open_file_handle_no");
        return api.open_fh(poolWrapper, node.getIdx(), create);
    }
}
