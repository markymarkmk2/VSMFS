package de.dimm.vsm.fuse;

import de.dimm.vsm.Exceptions.FuseException;
import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.Exceptions.PoolReadOnlyException;
import de.dimm.vsm.fsutils.FSENode;
import de.dimm.vsm.fsutils.RemoteStoragePoolHandler;
import de.dimm.vsm.fsutils.ShutdownHook;
import de.dimm.vsm.net.interfaces.FileHandle;
import de.dimm.vsm.fsutils.VSMFS;
import de.dimm.vsm.net.RemoteFSElem;
import java.io.File;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.sql.SQLException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Logger;



public class FuseVSMFS implements Filesystem3, XattrSupport, VSMFS
{
    ShutdownHook hook;

    //public String mountedVolume;
    public String mountPoint;
    private static final Log log = LogFactory.getLog(FuseVSMFS.class);
    private static final int NAME_LENGTH = 2048;
    static long tbc = 1024 * 1024 * 1024 * 1024;
    static int gbc = 1024 * 1024 * 1024;
    static int mbc = 1024 * 1024;
    static int kbc = 1024;
    //private SDFSCmds sdfsCmds;


    RemoteStoragePoolHandler remoteFSApi;

    String[] fuse_args;
    Logger logger;

    public FuseVSMFS( RemoteStoragePoolHandler handler, String mountPoint, Logger logger, String[] fuse_args   )
    {

        remoteFSApi = handler;
        this.mountPoint = mountPoint;
        this.fuse_args = fuse_args;
        this.logger = logger;

        log.info("mounting " + handler.getName() + " to " + mountPoint);
        //this.mountedVolume = mountedVolume;
        //sdfsCmds = new SDFSCmds(this.mountedVolume, this.mountPoint);

        File f = new File(mountPoint);
        if (!f.exists())
        {
            f.mkdirs();
        }

        hook = new ShutdownHook( mountPoint, true );
        Runtime.getRuntime().addShutdownHook(hook);

    }
    @Override
    public boolean mount()
    {
        try
        {
            FuseMount.mount(fuse_args, this, logger );
            return true;
        }
        catch (Exception e)
        {
                 log.error("unable to mount " + mountPoint, e);
        }
        return false;
    }

    public static boolean unmount( String mp )
    {
        try
        {
            Process p = Runtime.getRuntime().exec("umount " + mp);
            p.waitFor();
            return true;
        }
        catch (Exception e)
        {
                 log.error("unable to unmount " + mp, e);
        }
        return false;
    }
    @Override
    public boolean unmount( )
    {
//        handleResolver.closeAll();

        remoteFSApi.disconnect();

        boolean ret = unmount(mountPoint);
        if (ret)
            Runtime.getRuntime().removeShutdownHook(hook);

        return ret;

    }

    @Override
    public int chmod( String path, int mode ) throws FuseException
    {
        // log.info("setting file permissions " + mode);
        FSENode mf = resolvePath(path);

            try
            {
                mf.set_mode(mode);
            }
            catch (Exception e)
            {                
                log.error("unable to chmod " + path, e);
                throw new FuseException("access denied for " + path).initErrno(FuseException.EACCES);
            }
            finally
            {
            }
        return 0;
    }

    @Override
    public int chown( String path, int uid, int gid ) throws FuseException
    {
        FSENode mf = resolvePath(path);
        
            try
            {
                mf.set_owner_id(uid);
                mf.set_group_id(gid);
            }
            catch (Exception e)
            {
                log.error("unable to chown " + path, e);
                throw new FuseException("access denied for " + path).initErrno(FuseException.EACCES);
            }

        return 0;
    }

    @Override
    public int flush( String path, Object fh ) throws FuseException
    {
        FileHandle ch = (FileHandle) fh;
        try
        {
            ch.force(true);
        }
        catch (IOException e)
        {
            log.error("unable to sync file", e);
            throw new FuseException("symlink not supported").initErrno(FuseException.ENOSYS);
        }
        return 0;
    }

    @Override
    public int fsync( String path, Object fh, boolean isDatasync )
            throws FuseException
    {
        FileHandle ch = (FileHandle) fh;
        try
        {
            ch.force(true);
        }
        catch (IOException e)
        {
            log.error("unable to sync file", e);
            throw new FuseException("unable to sync").initErrno(FuseException.ENOSYS);
        }
        return 0;
    }

    @Override
    public int getattr( String path, FuseGetattrSetter getattrSetter )
            throws FuseException
    {       
        FSENode mf = resolvePath(path);
        
        try
        {
          
            int uid = mf.get_owner_id();
            int gid = mf.get_group_id();
            int mode = mf.get_mode();
            long atime = mf.get_unix_access_date();
            long ctime = mf.get_timestamp();
            long mtime = mf.get_unix_modification_date();
            long fileLength = mf.get_size();
            long blocks = 0;
            //long actualBytes = mf.getActualBytesWritten();

            getattrSetter.set(mf.get_GUID(), mode, 1, uid,
                    gid, 0, fileLength, blocks, atime, mtime, ctime);

        }
        catch (Exception e)
        {
            log.error("unable to parse attributes " + path, e);
            throw new FuseException().initErrno(FuseException.EACCES);
        }
        
        return 0;
    }

    @Override
    public int getdir( String path, FuseDirFiller dirFiller )
            throws FuseException
    {
        FSENode f = null;
        try
        {
            f = resolvePath(path);

            List<RemoteFSElem> list = remoteFSApi.get_child_nodes(f.getNode());
            
            dirFiller.add(".", ".".hashCode(), FuseFtype.TYPE_DIR);
            dirFiller.add("..", "..".hashCode(), FuseFtype.TYPE_DIR);
            for (int i = 0; i < list.size(); i++)
            {
                RemoteFSElem cf = list.get(i);
                dirFiller.add(cf.getName(), cf.hashCode(), getFtype(cf));
            }
        }
        catch (Exception e)
        {
            log.error("unable to getdir " + path, e);
            throw new FuseException().initErrno(FuseException.EACCES);
        }
        finally
        {
            f = null;
        }
        return 0;
    }

    @Override
    public int link( String from, String to ) throws FuseException
    {
        throw new FuseException("hard linking is not supported").initErrno(FuseException.ENOSYS);

    }

    @Override
    public int mkdir( String path, int mode ) throws FuseException
    {
        FSENode mf = resolvePath(path);
        
        try
        {
            if (mf != null && mf.exists())
            {
                throw new FuseException("folder exists").initErrno(FuseException.EPERM);
            }
            remoteFSApi.mkdir(path);
        }
        catch (IOException iOException)
        {
            throw new FuseException("cannot create dir " + path + ": " + iOException.getLocalizedMessage() ).initErrno(FuseException.EINVAL);
        }
        catch (PathResolveException iOException)
        {
            throw new FuseException("cannot create dir " + path + ": " + iOException.getLocalizedMessage() ).initErrno(FuseException.EINVAL);
        }
        catch (PoolReadOnlyException pox)
        {
            throw new FuseException("cannot create dir " + path + ": " + pox.getLocalizedMessage() ).initErrno(FuseException.EACCES);
        }
        mf = resolvePath(path);
        if (mf == null)
        {
            throw new FuseException("access denied for " + path).initErrno(FuseException.EACCES);
        }

        try
        {
            mf.set_attribute("unix:mode", Integer.valueOf(mode));
        }
        catch (Exception exception)
        {
            throw new FuseException("cannot set attribute dir " + path + ": " + exception.getLocalizedMessage() ).initErrno(FuseException.EINVAL);
        }
        return 0;
    }

    @Override
    public int mknod( String path, int mode, int rdev ) throws FuseException
    {
        // log.info("mknod(): " + path + " " + mode + " " + rdev + "\n");
        FSENode mf = resolvePath(path);
       

        try
        {
            if (mf != null && mf.exists())
            {
                throw new FuseException("file exists").initErrno(FuseException.EPERM);
            }
            else
            {
                throw new FuseException("not implemented for " + path).initErrno(FuseException.EACCES);
            }
        }
        catch (IOException e)
        {
            log.error("unable to mknod " + path, e);
            throw new FuseException("access denied for " + path).initErrno(FuseException.EACCES);
        }

        
        //return 0;
    }

    @Override
    public int open( String path, int flags, FuseOpenSetter openSetter )
            throws FuseException
    {
        try
        {
            openSetter.setFh( open_FileHandle(path) );
        }
        catch (FuseException e)
        {
            log.error("unable to open " + path, e);
            throw e;
        }
        return 0;
    }

    @Override
    public int read( String path, Object fh, ByteBuffer buf, long offset )
            throws FuseException
    {
        // log.info("Reading " + path + " at " + offset + " with buffer " +
        // buf.capacity());
        byte[] b = new byte[buf.capacity()];
        try
        {
            FileHandle ch = (FileHandle) fh;
            int read = ch.read(b,  b.length, offset);
            if (read == -1)
            {
                read = 0;
            }
            buf.put(b, 0, read);
        }
        catch (IOException e)
        {
            log.error("unable to read file " + path, e);
            throw new FuseException("error opening " + path).initErrno(FuseException.EACCES);
        }
        return 0;
    }

    @Override
    public int readlink( String path, CharBuffer link ) throws FuseException
    {
        FSENode mf = resolvePath(path);
        try
        {
            String lpath = mf.read_symlink();
            link.put(lpath);
        }
        catch (Exception e)
        {

            log.warn("error getting linking " + path, e);
            throw new FuseException("error getting linking " + path).initErrno(FuseException.EACCES);
        }
       
        return 0;
    }

    @Override
    public int release( String path, Object fh, int flags ) throws FuseException
    {
        // log.info("closing " + path + " with flags " + flags);
        FileHandle ch = (FileHandle) fh;
        try
        {
            ch.close();
        }
        catch (IOException e)
        {
            log.warn("unable to close " + path, e);
        }
        return 0;
    }

    @Override
    public int rename( String from, String to ) throws FuseException
    {        
        try
        {
            remoteFSApi.move_fse_node(from, to);
        }
        catch (Exception e)
        {
            log.warn("unable to rename " + from  + " " + to, e);
            throw new FuseException().initErrno(FuseException.EACCES);
        }
        return 0;
    }

    @Override
    public int rmdir( String path ) throws FuseException
    {

        FSENode f = resolvePath(path);
        if (f.get_name().equals(".") || f.get_name().equals(".."))
        {
            return 0;
        }
        else
        {
            try
            {
                if (remoteFSApi.remove_fse_node(path))
                {
                    return 0;
                }
                else
                {
                    log.debug("unable to delete folder " + f.get_path());
                    throw new FuseException().initErrno(FuseException.ENOTEMPTY);
                }
            }
            catch (Exception poolReadOnlyException)
            {
                log.debug("unable to delete folder " + f.get_path() + ", read only fs");
                throw new FuseException().initErrno(FuseException.EACCES);
            }
        }
    }

    @Override
    public int statfs( FuseStatfsSetter statfsSetter ) throws FuseException
    {
        // statfsSetter.set(blockSize, blocks, blocksFree, blocksAvail, files,
        // filesFree, namelen)
        long blocks = remoteFSApi.getTotalBlocks();

        long used = remoteFSApi.getUsedBlocks();
        if (used > blocks)
        {
            used = blocks;
        }
        statfsSetter.set(remoteFSApi.getBlockSize(), (int) blocks, (int) (blocks - used),
                (int) (blocks - used), (int) 0, 0, NAME_LENGTH);
        return 0;
    }

    @Override
    public int symlink( String from, String to ) throws FuseException
    {
        FSENode ffrom = resolvePath(from);
        FSENode fto = resolvePath(to);


        try
        {
            if (fto != null && fto.exists())
            {
                throw new FuseException().initErrno(FuseException.EPERM);
            }
            ffrom.create_symlink( to );
        }
        catch (Exception e)
        {

            log.error("error linking " + from + " to " + to, e);
            throw new FuseException().initErrno(FuseException.EACCES);
        }
        return 0;
    }

    @Override
    public int truncate( String path, long size ) throws FuseException
    {
        try
        {
            FSENode ffrom = resolvePath(path);

            ffrom.truncate(size);            
        }
        catch (Exception e)
        {
            log.error("unable to truncate file " + path, e);
            throw new FuseException().initErrno(FuseException.EACCES);
        }
        return 0;
    }

    @Override
    public int unlink( String path ) throws FuseException
    {
        FSENode f = this.resolvePath(path);
        try
        {
            if (remoteFSApi.remove_fse_node(path))
            {
                return 0;
            }
            else
            {
                log.warn("unable to delete file " + f.get_path());
                throw new FuseException().initErrno(FuseException.ENOSYS);
            }
        }
        catch (Exception e)
        {
            log.error("unable to file file " + path, e);
            throw new FuseException().initErrno(FuseException.EACCES);
        }
    }

    @Override
    public int utime( String path, int atime, int mtime ) throws FuseException
    {
        FSENode mf = this.resolvePath(path);
        try
        {
            mf.set_last_accessed(atime * 1000L);
            mf.set_last_modified(mtime * 1000L);
        }
        catch (Exception e)
        {
            log.error("unable to utime file " + path, e);
            throw new FuseException().initErrno(FuseException.EACCES);
        }
        return 0;
    }

    @Override
    public int write( String path, Object fh, boolean isWritepage,
            ByteBuffer buf, long offset ) throws FuseException
    {
        /*
         * log.info("writing data to  " +path + " at " + offset +
         * " and length of " + buf.capacity());
         */
        FileHandle ch = (FileHandle) fh;
        byte[] b = new byte[buf.capacity()];
        buf.get(b);
        try
        {
            ch.writeFile(b,  b.length, offset);
        }
        catch (Exception e)
        {
            log.error("unable to write to file" + path, e);
            throw new FuseException().initErrno(FuseException.EACCES);
        }
        return 0;
    }

    private FSENode resolvePath( String path )
    {
        log.debug("Resolved path: " + path);

        RemoteFSElem fse = null;
        try
        {
            fse = remoteFSApi.resolve_node(path);
        }
        catch (SQLException sQLException)
        {
            log.debug("No such node: " + path + ": " + sQLException.getMessage());
            return null;
        }

        if (fse == null)
        {
            log.debug("No such node: " + path);
            return null;
        }
        FSENode node = new FSENode(fse, remoteFSApi);
        return node;
    }

    /*/private FSENode resolvePath( String path ) throws FuseException
    {
        FSENode _f = remoteFSApi.resolve_node(path);
        if (_f == null)
        {
            log.debug("No such node");
            throw new FuseException().initErrno(FuseException.ENOENT);
        }
        return _f;
    }*/

    private int getFtype( RemoteFSElem _f ) throws FuseException
    {
        if (_f.isSymbolicLink())
        {
            return FuseFtype.TYPE_SYMLINK;
        }
        else if (_f.isDirectory())
        {
            return FuseFtype.TYPE_DIR;
        }
        else if (_f.isFile())
        {
            return FuseFtype.TYPE_FILE;
        }
        throw new FuseException().initErrno(FuseException.ENOENT);

    }

    private int getFtype( String path ) throws FuseException
    {
        FSENode mf = this.resolvePath(path);
        if (mf == null)
        {
            throw new FuseException().initErrno(FuseException.ENOENT);
        }
        if (mf.isSymbolicLink())
        {
            return FuseFtype.TYPE_SYMLINK;
        }
        else if (mf.isDirectory())
        {
            return FuseFtype.TYPE_DIR;
        }
        else if (mf.isFile())
        {
            return FuseFtype.TYPE_FILE;
        }
        
        log.error("could not determine type for " + path);
        throw new FuseException().initErrno(FuseException.ENOENT);

    }

    private FileHandle open_FileHandle( String path ) throws FuseException
    {
        FSENode mf = this.resolvePath(path);
        try
        {
            
            return mf.open_file_handle( /*create*/true);
        }
        catch (Exception e)
        {
            log.error("unable to open file" + mf.get_path(), e);
            throw new FuseException("error opening " + path).initErrno(FuseException.EACCES);
        }
    }

    @Override
    public int getxattr( String path, String name, ByteBuffer dst )
            throws FuseException, BufferOverflowException
    {
        int ftype = this.getFtype(path);
        if (ftype != FuseFtype.TYPE_SYMLINK)
        {
            FSENode mf = this.resolvePath(path);

            
            String val = null;
            try {
                val = mf.get_xattribute(name);
            } catch (SQLException sQLException) {
                throw new FuseException().initErrno(FuseException.ENODATA);
            }
            if (val != null)
            {
                log.debug("val=" + val);
                dst.put(val.getBytes());
            }
            else
            {
                throw new FuseException().initErrno(FuseException.ENODATA);
            }
        }
        return 0;
    }

    @Override
    public int getxattrsize( String path, String name, FuseSizeSetter sizeSetter )
            throws FuseException
    {
        if (name.startsWith("security.capability"))
        {
            return 0;
        }
        FSENode mf = this.resolvePath(path);
        String val = null;
        try {
            val = mf.get_xattribute(name);
        } catch (SQLException sQLException) {
            throw new FuseException().initErrno(FuseException.ENODATA);
            
        }
        if (val != null)
        {
            sizeSetter.setSize(val.getBytes().length);
        }
        return 0;
    }

    @Override
    public int listxattr( String path, XattrLister lister ) throws FuseException
    {
        FSENode mf = this.resolvePath(path);
        List<String> vals = mf.list_xattributes();

        for (int i = 0; i < vals.size(); i++)
        {
            lister.add(vals.get(i));
        }

        return 0;
    }

    @Override
    public int removexattr( String path, String name ) throws FuseException
    {
        return 0;
    }

    @Override
    public int setxattr( String path, String name, ByteBuffer value, int flags )
            throws FuseException
    {
        byte valB[] = new byte[value.capacity()];
        value.get(valB);
        String valStr = new String(valB);

        FSENode mf = this.resolvePath(path);
        mf.add_xattribute(name, valStr);
        return 0;
    }

    public void setShutdownHook( ShutdownHook hook )
    {
        this.hook = hook;
    }

    public ShutdownHook getShutdownHook()
    {
        return hook;
    }

}
