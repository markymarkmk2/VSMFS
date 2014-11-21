/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.fsutils;

import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.Exceptions.PoolReadOnlyException;
import de.dimm.vsm.FSServerConnector;
import de.dimm.vsm.VSMFSLogger;
import de.dimm.vsm.net.RemoteFSElem;
import de.dimm.vsm.net.StoragePoolWrapper;
import de.dimm.vsm.net.interfaces.FileHandle;
import de.dimm.vsm.net.interfaces.RemoteFSApi;
import de.dimm.vsm.net.interfaces.StoragePoolHandlerInterface;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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
    private final Lock insideApi = new ReentrantLock(true);
    private long startTime;
    private long endTime;

    public RemoteStoragePoolHandler( StoragePoolWrapper pool/*, Date timestamp, String subPath, User user*/ )
    {
        this.poolWrapper = pool;
        this.api = null;
        this.fs_server_conn = null;

        init();
    }

    void l( String s )
    {
        VSMFSLogger.getLog().debug("RSPH: " + s);
    }

    final void init()
    {
        fs_server_conn = new FSServerConnector();
    }
    
    public boolean waitForApi( int ms)
    {
        try
        {
            boolean ret = insideApi.tryLock(ms, TimeUnit.MILLISECONDS);
            if (ret)
            {
                unlock();
            }
            return ret;
        }
        catch (InterruptedException interruptedException)
        {
        }
        return false;
    }
    public long timeSinceLastCall()
    {
        return System.currentTimeMillis() - endTime;
    }
    public long durationLastCall()
    {
        return endTime - startTime;
    }
    public boolean isInsideApi()
    {
        return durationLastCall() < 0;
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

    public void connect( String host, int port ) throws UnknownHostException
    {
        l("connect");
        adr = InetAddress.getByName(host);
        this.port = port;
        api = fs_server_conn.getStoragePoolHandlerApi(adr, port, false, /*tcp*/ false);
    }

    public void connect( InetAddress adr, int port ) throws UnknownHostException
    {
        l("connect");
        this.adr = adr;
        this.port = port;
        api = fs_server_conn.getStoragePoolHandlerApi(adr, port, false, /*tcp*/ false);
    }

    @Override
    public RemoteFSElem create_fse_node( String fileName, String type ) throws IOException, PoolReadOnlyException, PathResolveException
    {
        l("create_fse_node " + fileName + " " + type);
        try
        {
            lock();
            return api.create_fse_node(poolWrapper, fileName, type);
        }
        finally
        {
            unlock();
        }
    }

    @Override
    public RemoteFSElem resolve_node( String path ) throws SQLException
    {
        l("resolve_node " + path);
        try
        {
            lock();
            return api.resolve_node(poolWrapper, path);
        }
        finally
        {
            unlock();
        }

    }
    long lastTotalBlocks;
    long lastUsedBlocks;
    int lastBlockSize;
    long lastStafsTS = 0;

    void checkStatfs()
    {
        long now = System.currentTimeMillis();
        if (now - lastStafsTS > 120 * 1000)
        {
            try
            {
                lock();
                lastTotalBlocks = api.getTotalBlocks(poolWrapper);
                lastUsedBlocks = api.getUsedBlocks(poolWrapper);
                lastBlockSize = api.getBlockSize(poolWrapper);
                lastStafsTS = now;
            }
            finally
            {
                unlock();
            }

        }
    }

    @Override
    public long getTotalBlocks()
    {
        //l("getTotalBlocks");
        checkStatfs();
        return lastTotalBlocks;
    }

    @Override
    public long getUsedBlocks()
    {
        //l("getUsedBlocks");
        checkStatfs();
        return lastUsedBlocks;
    }

    @Override
    public int getBlockSize()
    {
        //l("getBlockSize");
        checkStatfs();
        return lastBlockSize;
    }

    @Override
    public void mkdir( String pathName ) throws IOException, PoolReadOnlyException, PathResolveException
    {
        l("mkdir " + pathName);
        try
        {
            lock();
            api.mkdir(poolWrapper, pathName);
        }
        finally
        {
            unlock();
        }

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
        try
        {
            lock();
            return api.getName(poolWrapper);
        }
        finally
        {
            unlock();
        }

    }

    @Override
    public boolean remove_fse_node( String path ) throws PoolReadOnlyException, SQLException, IOException
    {
        l("remove_fse_node " + path);
        try
        {
            lock();
            return api.delete_fse_node_path(poolWrapper, path);
        }
        finally
        {
            unlock();
        }

    }

    @Override
    public boolean remove_fse_node_idx( long idx ) throws PoolReadOnlyException, SQLException, IOException
    {
        l("remove_fse_node " + idx);
        try
        {
            lock();
            return api.delete_fse_node_idx(poolWrapper, idx);
        }
        finally
        {
            unlock();
        }

    }

    @Override
    public List<RemoteFSElem> get_child_nodes( RemoteFSElem node ) throws SQLException
    {
        l("get_child_nodes");
        try
        {
            lock();
            return api.get_child_nodes(poolWrapper, node);
        }
        finally
        {
            unlock();
        }

    }

    @Override
    public void move_fse_node( String from, String to ) throws IOException, SQLException, PoolReadOnlyException, PathResolveException
    {
        l("move_fse_node " + from  + " to " + to);
        try
        {
            lock();
            api.move_fse_node(poolWrapper, from, to);
        }
        finally
        {
            unlock();
        }

    }
    @Override
    public void move_fse_node_idx( long from, String to ) throws IOException, SQLException, PoolReadOnlyException, PathResolveException
    {
        l("move_fse_node_idx " + from  + " to " + to);
        try
        {
            lock();
            api.move_fse_node_idx(poolWrapper, from, to);
        }
        finally
        {
            unlock();
        }

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
        try
        {
            lock();
            api.set_ms_times(poolWrapper, idx, toJavaTime, toJavaTime0, toJavaTime1);
        }
        finally
        {
            unlock();
        }

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
        try
        {
            lock();
            return api.exists(poolWrapper, fseNode);
        }
        finally
        {
            unlock();
        }

    }

    @Override
    public boolean isReadOnly( long idx ) throws IOException, SQLException
    {
        l("exists");
        try
        {
            lock();
            return api.isReadOnly(poolWrapper, idx);
        }
        finally
        {
            unlock();
        }

    }

    @Override
    public void force( long idx, boolean b ) throws IOException
    {
        l("force");
        try
        {
            lock();
            api.force(poolWrapper, idx, b);
        }
        finally
        {
            unlock();
        }

    }

    @Override
    public byte[] read( long idx, int length, long offset ) throws IOException
    {
        l("read");
        try
        {
            lock();
            byte[] b = api.read(poolWrapper, idx, length, offset);
            return b;
        }
        finally
        {
            unlock();
        }

    }

    public long length( long idx )
    {
        try
        {
            lock();
            long l = api.length(poolWrapper, idx);
            return l;
        }
        finally
        {
            unlock();
        }

    }

    @Override
    public void close( long idx ) throws IOException
    {
        l("close");
        try
        {
            lock();
            api.close_fh(poolWrapper, idx);
        }
        finally
        {
            unlock();
        }

    }

    @Override
    public void create( long idx ) throws IOException, PoolReadOnlyException
    {
        l("create");
        try
        {
            lock();
            api.create(poolWrapper, idx);
        }
        finally
        {
            unlock();
        }

    }

    @Override
    public void truncateFile( long idx, long size ) throws IOException, SQLException, PoolReadOnlyException
    {
        l("truncateFile");
        try
        {
            lock();
            api.truncateFile(poolWrapper, idx, size);
        }
        finally
        {
            unlock();
        }

    }

    @Override
    public void writeFile( long idx, byte[] b, int length, long offset ) throws IOException, SQLException, PoolReadOnlyException
    {
        l("RealwriteFile at " + offset + " len " + length);
        try
        {
            lock();
            api.writeFile(poolWrapper, idx, b, length, offset);
        }
        finally
        {
            unlock();
        }

    }
    @Override
    public void writeBlock( long idx, String hash, byte[] b, int length, long offset ) throws IOException, SQLException, PoolReadOnlyException, PathResolveException
    {
        l("RealwriteBlock at " + offset + " len " + length);
        try
        {
            lock();
            if (!api.checkBlock(poolWrapper,  hash))
            {
                l( "Write Block with data " + offset + " len " + length);   
                api.writeBlock(poolWrapper, idx, hash, b, length, offset);
            }
            else
            {
                try
                {
                    l( "Write Block with hash without data " + offset + " len " + length);   
                    api.writeBlock(poolWrapper, idx, hash, null, length, offset);
                }
                catch (IOException iOException)
                {
                    l("Retry schreiben von Block: " + iOException.getMessage());
                    api.writeBlock(poolWrapper, idx, hash, b, length, offset);                    
                }
            }
        }
        finally
        {
            unlock();
        }

    }

    public FileHandle open_file_handle( RemoteFSElem fseNode, boolean forWrite ) throws IOException, PoolReadOnlyException, SQLException, PathResolveException
    {
        long handleNo;
        l("open_file_handle hno " + fseNode.getIdx());
        try
        {
            lock();
            handleNo = api.open_fh(poolWrapper, fseNode.getIdx(), forWrite);

            if (handleNo == -1)
            {
                throw new IOException("Cannot open file " + fseNode.getPath());
            }

            // STOR IN FSENODE
            fseNode.setFileHandle(handleNo);

            FileHandle handle = isBuffered()
                    ? new BufferedRemoteFileHandle(this, handleNo, readBufferBlockSize, writeBufferBlockSize)
                    : new RemoteFileHandle(this, handleNo);
            return handle;
        }
        finally
        {
            unlock();
        }

    }

    public FileHandle open_stream_handle( RemoteFSElem fseNode, boolean forWrite ) throws IOException, PoolReadOnlyException, SQLException, PathResolveException
    {
        l("open_file_handle hno " + fseNode.getIdx());
        try
        {
            lock();
            long handleNo = api.open_stream(poolWrapper, fseNode.getIdx(), forWrite);
            if (handleNo == -1)
            {
                throw new IOException("Cannot open file " + fseNode.getPath());
            }

            // STOR IN FSENODE
            fseNode.setFileHandle(handleNo);

            RemoteFileHandle handle = isBuffered()
                    ? new BufferedRemoteFileHandle(this, handleNo, readBufferBlockSize, writeBufferBlockSize)
                    : new RemoteFileHandle(this, handleNo);
            return handle;
        }
        finally
        {
            unlock();
        }

    }

    void set_attribute( RemoteFSElem fseNode, String string, Integer valueOf ) throws IOException, SQLException, PoolReadOnlyException
    {
        try
        {
            lock();
            api.set_attribute(poolWrapper, fseNode, string, valueOf);
        }
        finally
        {
            unlock();
        }

    }

    String read_symlink( RemoteFSElem fseNode )
    {
        try
        {
            lock();
            return api.read_symlink(poolWrapper, fseNode);
        }
        finally
        {
            unlock();
        }

    }

    void create_symlink( RemoteFSElem fseNode, String to ) throws IOException, PoolReadOnlyException
    {
        try
        {
            lock();
            api.create_symlink(poolWrapper, fseNode, to);
        }
        finally
        {
            unlock();
        }

    }

    void truncate( RemoteFSElem fseNode, long size ) throws IOException, PoolReadOnlyException
    {
        try
        {
            lock();
            api.truncate(poolWrapper, fseNode, size);
        }
        finally
        {
            unlock();
        }

    }

    void set_last_modified( RemoteFSElem fseNode, long l ) throws IOException, SQLException, PoolReadOnlyException
    {
        try
        {
            lock();
            api.set_last_modified(poolWrapper, fseNode, l);
        }
        finally
        {
            unlock();
        }

    }

    String get_xattribute( RemoteFSElem fseNode, String name ) throws SQLException
    {
        try
        {
            lock();
            return api.get_xattribute(poolWrapper, fseNode, name);
        }
        finally
        {
            unlock();
        }

    }

    void set_last_accessed( RemoteFSElem fseNode, long l ) throws IOException, SQLException, PoolReadOnlyException
    {
        try
        {
            lock();
            api.set_last_accessed(poolWrapper, fseNode, l);
        }
        finally
        {
            unlock();
        }

    }

    List<String> list_xattributes( RemoteFSElem fseNode )
    {
        try
        {
            lock();
            return api.list_xattributes(poolWrapper, fseNode);
        }
        finally
        {
            unlock();
        }

    }

    void add_xattribute( RemoteFSElem fseNode, String name, String valStr )
    {
        try
        {
            lock();
            api.add_xattribute(poolWrapper, fseNode, name, valStr);
        }
        finally
        {
            unlock();
        }

    }

    void set_mode( RemoteFSElem fseNode, int mode ) throws IOException, SQLException, PoolReadOnlyException
    {
        try
        {
            lock();
            api.set_mode(poolWrapper, fseNode, mode);
        }
        finally
        {
            unlock();
        }

    }

    void set_owner_id( RemoteFSElem fseNode, int uid ) throws IOException, SQLException, PoolReadOnlyException
    {
        try
        {
            lock();
            api.set_owner_id(poolWrapper, fseNode, uid);
        }
        finally
        {
            unlock();
        }

    }

    void set_group_id( RemoteFSElem fseNode, int gid ) throws IOException, SQLException, PoolReadOnlyException
    {
        try
        {
            lock();
            api.set_group_id(poolWrapper, fseNode, gid);
        }
        finally
        {
            unlock();
        }

    }

    @Override
    public long open_file_handle_no( RemoteFSElem node, boolean create ) throws IOException, PoolReadOnlyException, SQLException, PathResolveException
    {
        l("open_file_handle_no");
        try
        {
            lock();
            return api.open_fh(poolWrapper, node.getIdx(), create);
        }
        finally
        {
            unlock();
        }
    }

    private void lock()
    {
        startTime = System.currentTimeMillis();
        insideApi.lock();
    }

    private void unlock()
    {
        insideApi.unlock();
        endTime = System.currentTimeMillis();
    }

    public StoragePoolWrapper getWrapper()
    {
        return poolWrapper;
    }

    @Override
    public void setAttribute( RemoteFSElem elem, String string, Integer valueOf ) throws IOException, SQLException, PoolReadOnlyException {
        getApi().set_attribute( getWrapper(), elem, string, valueOf);
    }

    @Override
    public String readSymlink( RemoteFSElem elem ) {
        return getApi().read_symlink( getWrapper(), elem);
    }

    @Override
    public void createSymlink( RemoteFSElem elem, String to ) throws IOException, PoolReadOnlyException {
         getApi().create_symlink( getWrapper(), elem, to );
    }
     @Override
    public String getXattribute( RemoteFSElem elem, String name ) throws SQLException
    {
        return getApi().get_xattribute( getWrapper(), elem, name);
    }

    @Override
    public List<String> listXattributes(RemoteFSElem elem )
    {
        return getApi().list_xattributes( getWrapper(), elem);
    }

    @Override
    public void addXattribute( RemoteFSElem elem, String name, String valStr )
    {
        getApi().add_xattribute( getWrapper(), elem, name, valStr );
    }

    @Override
    public void updateElem(RemoteFSElem elem, long handleNo )  throws IOException, SQLException, PoolReadOnlyException
    {
        getApi().updateAttributes(getWrapper(), handleNo, elem);
    }

}
