package de.dimm.vsm.fuse;

import de.dimm.vsm.Exceptions.FuseException;
import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.Exceptions.PoolReadOnlyException;
import de.dimm.vsm.fsengine.HandleResolver;
import de.dimm.vsm.fsutils.FSENode;
import de.dimm.vsm.fsutils.IVSMFS;
import de.dimm.vsm.fsutils.MacShutdownHook;
import de.dimm.vsm.fsutils.RemoteStoragePoolHandler;
import de.dimm.vsm.fsutils.ShutdownHook;
import de.dimm.vsm.net.RemoteFSElem;
import de.dimm.vsm.net.interfaces.FileHandle;
import de.dimm.vsm.records.FileSystemElemNode;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.List;
import org.apache.log4j.Logger;
import org.catacombae.jfuse.FUSE;
import org.catacombae.jfuse.FUSEErrorValues;
import org.catacombae.jfuse.FUSEOptions;
import org.catacombae.jfuse.MacFUSEFileSystemAdapter;
import org.catacombae.jfuse.types.fuse26.FUSEDirFil;
import org.catacombae.jfuse.types.fuse26.FUSEFileInfo;
import org.catacombae.jfuse.types.fuse26.FUSEFillDir;
import org.catacombae.jfuse.types.system.FileStatusFlags;
import org.catacombae.jfuse.types.system.Stat;
import org.catacombae.jfuse.types.system.StatVFS;
import org.catacombae.jfuse.types.system.Timespec;
import org.catacombae.jfuse.types.system.Utimbuf;
import org.catacombae.jfuse.util.FUSEUtil;




public class MacFuseVSMFS extends MacFUSEFileSystemAdapter/*//FUSEFileSystemAdapter*/ implements IVSMFS
{
    ShutdownHook hook;

    //public String mountedVolume;
    public String mountPoint;
    //private static final Log log = LogFactory.getLog(MacFuseVSMFS.class);
    private static final int NAME_LENGTH = 2048;
    static long tbc = 1024 * 1024 * 1024 * 1024;
    static int gbc = 1024 * 1024 * 1024;
    static int mbc = 1024 * 1024;
    static int kbc = 1024;
    //private SDFSCmds sdfsCmds;
    

    HandleResolver handleResolver;

    RemoteStoragePoolHandler remoteFSApi;

    FUSEOptions fuse_args;
    private Logger log;
    boolean rdwr;

    public MacFuseVSMFS( RemoteStoragePoolHandler handler, String mountPoint, Logger logger, String[] fuse_args, boolean rdwr )
    {

        remoteFSApi = handler;
        this.mountPoint = mountPoint;
        this.fuse_args = new FUSEOptions();
        this.log = logger;
        this.rdwr = rdwr;
        this.fuse_args.setFsname("VSMFS");
        
        handleResolver = new HandleResolver(log);
        
        StringBuilder opts = new StringBuilder();
        for (int i = 0; i < fuse_args.length; i++)
        {
            opts.append( fuse_args[i] );
            opts.append( " ");            
        }
        
        for (int i = 0; i < fuse_args.length; i++)
        {
            String string = fuse_args[i];
            if (string.equals("-d"))
                this.fuse_args.setDebug(Boolean.TRUE);
            if (string.equals("-f"))
                this.fuse_args.setForeground(Boolean.TRUE);
            if (string.equals("-s"))
                this.fuse_args.setSingleThreaded(Boolean.TRUE);
            if (string.equals("-n") && i+1 < fuse_args.length)
            {
                i++;
                this.fuse_args.setFsname( fuse_args[i]);
            }
            if (string.equals("-o") && i+1 < fuse_args.length)
            {
                i++;
                this.fuse_args.addOption(fuse_args[i]);
            }
        }

        log.info("mounting " + handler.getName() + (rdwr?" rdwr" : " rdonly") + " to " + mountPoint + " with opts " + opts.toString());
        //this.mountedVolume = mountedVolume;
        //sdfsCmds = new SDFSCmds(this.mountedVolume, this.mountPoint);

        File f = new File(mountPoint);
        if (!f.exists())
        {
            f.mkdirs();
        }

        hook = new MacShutdownHook( mountPoint );
        Runtime.getRuntime().addShutdownHook(hook);

    }
    
    static void traceEnter( Object ...s)
    {
        StringBuilder sb = new StringBuilder("TraceEnter: ");
        for (Object string : s) {
            if (string instanceof ByteBuffer)
                sb.append(resolveBuffer(((ByteBuffer)string).duplicate()));
            else
                sb.append(string.toString());
            sb.append(" ");
        }
        System.err.println(sb.toString());
    }
    
    static void traceLog( Object ...s)
    {
        StringBuilder sb = new StringBuilder("TraceLog  : ");
        for (Object string : s) {
            if (string instanceof ByteBuffer)
                sb.append(resolveBuffer(((ByteBuffer)string).duplicate()));
            else
                sb.append(string.toString());
            sb.append(" ");
        }
        System.err.println(sb.toString());
    }
    
    static void traceLeave( Object ...s)
    {
        StringBuilder sb = new StringBuilder("TraceLeave: ");
        for (Object string : s) {
            sb.append(string);
            sb.append(" ");
        }
        System.err.println(sb.toString());
    }
    
    @Override
    public boolean mount()
    {
        traceEnter("mount");

        try
        {
            FUSE.mount(this, mountPoint, fuse_args );
            return true;
        }
        catch (Exception e)
        {
                 log.error("unable to mount " + mountPoint, e);
        }
        
        traceLeave("mount");
        return false;
    }

    public static boolean unmount( String mp ) throws IOException, InterruptedException
    {
        traceEnter("unmount", mp);

            Process p = Runtime.getRuntime().exec("umount " + mp);
            p.waitFor();

            traceLeave("unmount");
            return true;
        }
    
    @Override
    public boolean unmount( )
    {
        traceEnter("unmount");

        handleResolver.closeAll();
        remoteFSApi.disconnect();

        boolean ret = false;
            try
            {
            ret = unmount(mountPoint);
            if (ret)
            {
                Runtime.getRuntime().removeShutdownHook(hook);
            }
        }
        catch (Exception iOException)
            {
            log.error("unable to unmount " + mountPoint, iOException);
            }
        traceLeave("unmount");
        return ret;
    }
    
    @Override
    public int create(ByteBuffer path, short mode, FUSEFileInfo fi) 
    {
        traceEnter("create", path);

        if (!rdwr)
            return -FUSEErrorValues.ENOTSUP;
            
        String pathStr = resolveBuffer(path);          
        FSENode mf = resolvePath(pathStr);

        try
        {
            if (mf != null && mf.exists())
            {
                return -FUSEErrorValues.EEXIST;
            }
            RemoteFSElem elem = remoteFSApi.create_fse_node(pathStr, FileSystemElemNode.FT_FILE);
            mf = resolvePath(pathStr);
            if (mf == null)
            {
                return -FUSEErrorValues.EACCES;
            }
            fi.fh = open_FileHandle(mf, true);
        }
        catch (PoolReadOnlyException iOException)
        {
            return -FUSEErrorValues.EACCES;
        }
        catch (PathResolveException edxc)
        {
            return -FUSEErrorValues.EINVAL;
        }
        catch (Exception edxc)
        {
            return -FUSEErrorValues.EINVAL;
        }

        
        mf = resolvePath(pathStr);
        

        try
        {
            mf.set_attribute("unix:mode", Integer.valueOf(mode));
        }
        catch (Exception exception)
        {
            return -FUSEErrorValues.EINVAL;
        }

        traceLeave("create");
        return 0;
    }

    public int chmod( ByteBuffer path, int mode ) throws FuseException
    {
        traceEnter("chmod");
        // log.info("setting file permissions " + mode);
        String pathStr = resolveBuffer(path);          
        FSENode mf = resolvePath(pathStr);

            try
            {
                mf.set_mode(mode);
            }
            catch (Exception e)
            {                
                log.error("unable to chmod " + path, e);
                throw new FuseException("access denied for " + pathStr).initErrno(FuseException.EACCES);
            }
            finally
            {
            }
            traceLeave("chmod");
        return 0;
    }

    @Override
    public int chown( ByteBuffer path, long uid, long gid )
    {
        
        traceEnter("chown");
        FSENode mf = resolvePath(path);
        
            try
            {
                mf.set_owner_id((int)uid);
                mf.set_group_id((int)gid);
            }
            catch (Exception e)
            {
                        String pathStr = resolveBuffer(path);          

                log.error("unable to chown " + pathStr, e);
                return -1;
            }

            traceLeave("chown");
        return 0;
    }

    @Override
    public int flush( ByteBuffer path, FUSEFileInfo fh )
    {
        traceEnter("flush", path, fh.toString());
        
       
        try
        {
            FileHandle ch = handleResolver.get_handle_by_info( fh );
            if (ch != null)
                ch.force(true);
        }
        catch (Exception e)
        {
            log.error("unable to sync file", e);
            return -1;
        }
        traceLeave("flush");
        return 0;
    }

    @Override
    public int fsync( ByteBuffer path,  boolean isDatasync, FUSEFileInfo fh )            
    {
        traceEnter("fsync", path, fh.toString());
        
        
        try
        {
            FileHandle ch = handleResolver.get_handle_by_info( fh );
            if (ch != null)
                ch.force(true);
            
            handleResolver.clearNodeCache( resolveBuffer( path) );
        }
        catch (Exception e)
        {
            log.error("unable to sync file", e);
            return -1;
        }
        traceLeave("fsync");
        return 0;
    }

    private static void mfillStat( Stat stat, FSENode mf )
    {
        int uid = mf.get_owner_id();
        int gid = mf.get_group_id();
        int mode = mf.get_mode();
        long fileLength = mf.get_size();
        //long actualBytes = mf.getActualBytesWritten();
        stat.st_atimespec.setToMillis(mf.get_last_accessed());
        stat.st_mtimespec.setToMillis(mf.get_last_modified());
        stat.st_ctimespec.setToMillis(mf.get_creation_date());
        stat.st_blocks = fileLength / 4096;
        stat.st_blocksize = 4096;
        stat.st_gid = gid;
        stat.st_uid = uid;
        stat.st_size = fileLength;
        if (mode == 0 || (mode & (Stat.S_IFDIR | Stat.S_IFREG )) == 0)
        {
            if (mf.isDirectory())
            {
                stat.st_mode = Stat.S_IFDIR | 0755;
                stat.st_nlink = 2;
            }
            else
            {
                stat.st_mode = Stat.S_IFREG | 0444;
                stat.st_nlink = 1;
            }
        }
        else
        {
            stat.st_mode = mode;    
        }
        stat.st_mode |= 0666;
    }
    private static void rfillStat( Stat stat, RemoteFSElem mf )
    {
        int uid = mf.getUid();
        int gid = mf.getGid();
        int mode = mf.getPosixMode();
        long fileLength = mf.getDataSize();
        //long actualBytes = mf.getActualBytesWritten();
        stat.st_atimespec.setToMillis(mf.getAtimeMs() );
        stat.st_mtimespec.setToMillis(mf.getMtimeMs() );
        stat.st_ctimespec.setToMillis(mf.getCtimeMs());
        stat.st_blocks = 0;
        stat.st_blocksize = 4096;
        stat.st_gid = gid;
        stat.st_uid = uid;
        stat.st_size = fileLength;

        if (mode == 0)
        {
            if (mf.isDirectory())
            {
                stat.st_mode = Stat.S_IFDIR | 0755;
                stat.st_nlink = 2;
            }
            else
            {
                stat.st_mode = Stat.S_IFREG | 0444;
                stat.st_nlink = 1;
            }
        }
        else
        {
            stat.st_mode = mode;    
        }
    }

    @Override
    public int getattr( ByteBuffer path, Stat stat )
    {   
        traceEnter("getattr", path);
        String pathStr = resolveBuffer(path); 
        String[] skipNames = {};
//        String[] skipNames = {".DSStore", "Contents", ".LSOverride", "Support Files", "Icon", "MacOS", "Mac OS 8", "Mac OS X", "MacOSClassic"};

        for (int i = 0; i < skipNames.length; i++)
        {
            String string = skipNames[i];
            if (pathStr.endsWith(string))
            {   traceLeave("getattr skip");
                return -FUSEErrorValues.ENOENT;
            }
        }
        
            
        FSENode mf = resolvePath(pathStr);
  
        if (mf == null)
        {            
            traceLeave("getattr noent");
            return -FUSEErrorValues.ENOENT;
        }
        
        try
        {
            mfillStat( stat, mf );
            stat.printFields( "", System.err);
                    

        }
        catch (Exception e)
        {
            traceLeave("getattr exc");
            log.error("unable to parse attributes " + path, e);
            return -1;
        }
        traceLeave("getattr ok");
        return 0;
    }

    @Override
    public int getdir( ByteBuffer path, FUSEDirFil dirFiller )
    {
        traceEnter("getdir", path);
        
        FSENode f;
        try
        {
        String pathStr = resolveBuffer(path);          
        f = resolvePath(pathStr);

            List<RemoteFSElem> list = remoteFSApi.get_child_nodes(f.getNode());
            
            dirFiller.fill(FUSEUtil.encodeUTF8("."), ".".hashCode(), FuseFtype.TYPE_DIR);
            dirFiller.fill(FUSEUtil.encodeUTF8(".."), "..".hashCode(), FuseFtype.TYPE_DIR);
            for (int i = 0; i < list.size(); i++)
            {
                RemoteFSElem cf = list.get(i);
                
                traceLog("getdir",  cf.getName());

                dirFiller.fill( FUSEUtil.encodeUTF8(cf.getName()), cf.hashCode(), getFtype(cf));
                if (cf.getStreamSize() > 0)
                {
                    dirFiller.fill(FUSEUtil.encodeUTF8("._" + cf.getName()), cf.hashCode(), getFtype(cf));
                }
            }
        }
        catch (Exception e)
        {
            log.error("unable to getdir " + path, e);
            return -1;
        }
        traceLeave("getdir");
        return 0;
    }

    @Override
    public int readdir(ByteBuffer path,
			FUSEFillDir dirFiller,
			long offset,
			FUSEFileInfo fi)
    {
        traceEnter("readdir", path);
        FSENode f;
        try
        {
            f = resolvePath(path);

            List<RemoteFSElem> list = remoteFSApi.get_child_nodes(f.getNode());

            dirFiller.fill(FUSEUtil.encodeUTF8("."), null, 0);
            dirFiller.fill(FUSEUtil.encodeUTF8(".."), null, 0);
            for (int i = 0; i < list.size(); i++)
            {
                RemoteFSElem cf = list.get(i);

                dirFiller.fill(FUSEUtil.encodeUTF8(cf.getName()), null, 0);
                
                FSENode node = new FSENode(cf, remoteFSApi );
                
                String cfPath = resolveBuffer(path);
                cfPath += "/" + cf.getName();
                handleResolver.addNodeCache(node, cfPath);
                traceLog("readdir",  cf.getName());
            }
        }
        catch (Exception e)
        {
            log.error("unable to readdir " + path, e);
            return -1;
        }
        traceLeave("readdir");
        return 0;
    }

    @Override
    public int link( ByteBuffer from, ByteBuffer to )
    {
        log.error("hard linking is not supported");
        return -FUSEErrorValues.EINVAL;

    }

    @Override
    public int mkdir( ByteBuffer path, short mode )
    {
        traceEnter("mkdir", path);
        
        String pathStr = resolveBuffer(path);          
        FSENode mf = resolvePath(pathStr);
        
        try
        {
            if (mf != null && mf.exists())
            {
                return -FUSEErrorValues.EEXIST;
            }
            remoteFSApi.mkdir(pathStr);
        }
        catch (PoolReadOnlyException ex)
        {
             return -FUSEErrorValues.EINVAL;
        }
        catch (PathResolveException ex)
        {
             return -FUSEErrorValues.EINVAL;
        }
        catch (IOException  iOException)
        {
            return -FUSEErrorValues.EINVAL;
        }
        mf = resolvePath(pathStr);
        if (mf == null)
        {
            return -FUSEErrorValues.EACCES;
        }

        try
        {
            mf.set_attribute("unix:mode", Integer.valueOf(mode));
        }
        catch (Exception exception)
        {
            return -FUSEErrorValues.EINVAL;
        }
        traceLeave("mkdir");
        return 0;
    }

    @Override
    public int mknod( ByteBuffer path, short mode, long rdev )
    {
        traceEnter("mknod", path);
        
        // log.info("mknod(): " + path + " " + mode + " " + rdev + "\n");
        String pathStr = resolveBuffer(path);          
        FSENode mf = resolvePath(pathStr);
        

        try
        {
            if (mf != null && mf.exists())
            {
                return -FUSEErrorValues.EEXIST;
            }
            else
            {
                return -FUSEErrorValues.EINVAL;
            }
        }
        catch (IOException e)
        {
            log.error("unable to mknod " + path, e);
            return -FUSEErrorValues.EINVAL;
        }
        finally
        {
            traceLeave("mknod");
        }
        //return 0;
    }

    @Override
    public int open( ByteBuffer path, FUSEFileInfo fi )            
    {
        boolean create = fi.getFlagCreate() || fi.getFlagReadWrite() || fi.getFlagAppend() || fi.getFlagWriteOnly() ;
        traceEnter("open" + (create ? " RW" : "") + " " + fi.flags, path);
        if (fi.flags != 0)
        {
            try
            {
                FileStatusFlags.Print.main( (String[]) null );
            }
            catch (Exception exception)
            {
            }
        }
        
        try
        {
            FSENode mf = resolvePath(path);
            
            
            
            fi.fh = open_FileHandle(mf, create);
            fi.direct_io = true;
        }
        catch (Exception e)
        {
            log.error("unable to open " + path, e);
            return -FUSEErrorValues.EACCES;
        }
        traceLeave("open");
        return 0;
    }

    @Override
    public int read( ByteBuffer path,  ByteBuffer buf, long offset, FUSEFileInfo fi )           
    {
        traceEnter("read", path);
        
        // log.info("Reading " + path + " at " + offset + " with buffer " +
        // buf.capacity());
        
        
        String f = resolveBuffer( path );
        log.debug("Reading " + buf.capacity() + " byte from handle " + fi.fh + " File " + f );
        try
        {
            FileHandle ch = handleResolver.get_handle_by_info( fi );
            
            byte[] b = ch.read( buf.capacity(), offset);
            if (b == null)
                throw new IOException("no data found");
            
            buf.put(b);
            return b.length;
        }
        catch (Exception e)
        {
            log.error("unable to read " + buf.capacity() + " byte offset " + offset + " from file " + f + ": " + e.getMessage());
            return -FUSEErrorValues.EACCES;
        }
        finally
        {
            traceLeave("read");
        }

    }

    @Override
    public int readlink( ByteBuffer path, ByteBuffer link )
    {
        traceEnter("readlink", path);
        
        FSENode mf = resolvePath(path);
        try
        {
            String lpath = mf.read_symlink();
            link.put(lpath.getBytes());
        }
        catch (Exception e)
        {

            log.warn("error getting linking " + path, e);
            return -FUSEErrorValues.EACCES;
        }
        finally
        {
            traceLeave("readlink");
        }
               
        return 0;
    }

    @Override
    public int release( ByteBuffer path, FUSEFileInfo fh )
    {
        traceEnter("release", path);
        
        // log.info("closing " + path + " with flags " + flags);
        FileHandle ch = handleResolver.get_handle_by_info(fh);
        try
        {
            closeFileChannel(fh.fh);
            //ch.close();
        }
        catch (Exception e)
        {
            log.warn("unable to close " + path, e);
        }
        traceLeave("release");
        return 0;
    }

    @Override
    public int rename( ByteBuffer from, ByteBuffer to )
    {            
        traceEnter("rename", from.toString(), to.toString());
    
        
        try
        {
            String f = resolveBuffer( from );
            String t = resolveBuffer( to );
            remoteFSApi.move_fse_node( f, t);
        }
        catch (Exception e)
        {
            log.warn("unable to rename " + from  + " " + to, e);
            return -FUSEErrorValues.EACCES;
        }
        finally
        {
            traceLeave("rename");
        }
        return 0;
    }

    @Override
    public int rmdir( ByteBuffer path )
    {
        traceEnter("rmdir", path);

        FSENode f = resolvePath(path);
        if (f.get_name().equals(".") || f.get_name().equals(".."))
        {
            return 0;
        }
        else
        {
            try
            {
                if (remoteFSApi.remove_fse_node(f.get_path()))
                {
                    return 0;
                }
                else
                {
                    log.debug("unable to delete folder " + f.get_path());
                    return -FUSEErrorValues.EACCES;
                }
            }
            catch (Exception poolReadOnlyException)
            {
                log.debug("unable to delete folder " + f.get_path() + ", read only fs");
                return -FUSEErrorValues.EACCES;
            }
            finally
            {
                traceLeave("rmdir");
            }
        }
    }

    @Override
    public int statfs( ByteBuffer path, StatVFS stat )
    {
        traceEnter("statfs", path);
        // statfsSetter.set(blockSize, blocks, blocksFree, blocksAvail, files,
        // filesFree, namelen)
        long blocks = remoteFSApi.getTotalBlocks();

        long used = remoteFSApi.getUsedBlocks();
        if (used > blocks)
        {
            used = blocks;
        }
        
        stat.f_blocks = blocks;
        stat.f_bavail = blocks - used;
        stat.f_bfree = blocks - used;
        stat.f_bsize = remoteFSApi.getBlockSize();
        stat.f_flag = 0;

        // We do not set the inode number limits... not sure about that yet.
        stat.f_files = Integer.MAX_VALUE;
        stat.f_ffree = Integer.MAX_VALUE;
        stat.f_favail = stat.f_ffree;
       
        stat.f_namemax = NAME_LENGTH;
        
        //stat.printFields("de", System.out);

        traceLeave("statfs");
        return 0;
    }

    @Override
    public int access(ByteBuffer path, int mode) 
    {
        traceEnter("access", path);
        FSENode ffrom = resolvePath(path);
        if (ffrom == null)
        {
            traceLeave("access nok");
            return -1;
        }
        
        traceLeave("access ok");
        return 0;
    }
    

    @Override
    public int symlink( ByteBuffer from, ByteBuffer to )
    {
        traceEnter("symlink", from.toString());
        FSENode ffrom = resolvePath(from);
        FSENode fto = resolvePath(to);


        try
        {
            if (fto != null && fto.exists())
            {
                throw new FuseException().initErrno(FuseException.EPERM);
            }
            ffrom.create_symlink( fto.get_path() );
        }
        catch (Exception e)
        {

            log.error("error linking " + from + " to " + to, e);
            return -FUSEErrorValues.EACCES;
        }
        finally
        {
            traceLeave("symlink");
        }
        return 0;
    }

    @Override
    public int truncate( ByteBuffer path, long size )
    {
        traceEnter("truncate", path);
        try
        {
            FSENode ffrom = resolvePath(path);

            ffrom.truncate(size);    
            handleResolver.clearNodeCache( resolveBuffer( path ) );
        }
        catch (Exception e)
        {
            log.error("unable to truncate file " + path, e);
            return -FUSEErrorValues.EACCES;
        }
        finally
        {
            traceLeave("truncate");
        }
        return 0;
    }

    @Override
    public int unlink( ByteBuffer path )
    {
        traceEnter("unlink", path);
        
        String pathStr = resolveBuffer( path );
        try
        {
            handleResolver.clearNodeCache( pathStr );

            if (remoteFSApi.remove_fse_node(pathStr))
            {
                return 0;
            }
            else
            {
                log.warn("unable to delete file " + pathStr);
                return -FUSEErrorValues.EACCES;
            }
        }
        catch (Exception e)
        {
            log.error("unable to file file " + pathStr, e);
            return -FUSEErrorValues.EACCES;
        }
        finally
        {
            traceLeave("unlink");
        }
    }

    @Override
    public int utime( ByteBuffer path, Utimbuf time )
    {
        traceEnter("utime", path);
        FSENode mf = this.resolvePath(path);
        try
        {
            mf.set_last_accessed(time.actime * 1000L);
            mf.set_last_modified(time.modtime * 1000L);
        }
        catch (Exception e)
        {
            log.error("unable to utime file " + path, e);
            return -FUSEErrorValues.EACCES;
        }
        finally
        {
            traceLeave("utime");
        }
        return 0;
    }
    @Override
    public int utimens(ByteBuffer path,
			Timespec accessTime,
                        Timespec modificationTime)
    {
        traceEnter("utimens", path);
        FSENode mf = this.resolvePath(path);
        try
        {
            mf.set_last_accessed(accessTime.toMillis());
            mf.set_last_modified(modificationTime.toMillis());
        }
        catch (Exception e)
        {
            log.error("unable to utime file " + path, e);
            return -FUSEErrorValues.EACCES;
        }
        finally
        {
            traceLeave("utimens");
        }
        return 0;
    }

    @Override
    public int write( ByteBuffer path, ByteBuffer buf,
		      long offset,
		      FUSEFileInfo fi )
    {
        traceEnter("write o:" + offset + " l:" + buf.capacity(), path);
    
            
        /*
         * log.info("writing data to  " +path + " at " + offset +
         * " and length of " + buf.capacity());
         */
        FileHandle ch = handleResolver.get_handle_by_info(fi);
        byte[] b = new byte[buf.capacity()];
        buf.get(b);
        try
        {
            log.debug("Writing " + buf.capacity() + " byte "  + " at pos " + offset + " to handle " + fi.fh  );

            ch.writeFile(b,  b.length, offset);
            return b.length;
        }
        catch (Exception e)
        {
            log.error("unable to write to file" + path, e);
            return -FUSEErrorValues.EACCES;
        }
        finally
        {
            traceLeave("write");
        }
    
    }

        
    
    private static String resolveBuffer( ByteBuffer bpath )
    {
        bpath.rewind();
        String pathString = FUSEUtil.decodeUTF8(bpath);
        
        return pathString;
        
    }
    private FSENode resolvePath( ByteBuffer bpath )
    {
        String path = resolveBuffer(bpath);
        return resolvePath(path);
    }
    
    private FSENode resolvePath( String path )
    {
        boolean isStreamPath = false;
        int idx = path.lastIndexOf('/');
//        if (idx >= 0 && path.length() - idx > 2 && path.charAt(idx + 1) == '.' && path.charAt(idx + 2) == '_')
//        {
//            path = path.substring(0, idx + 1) + path.substring(idx + 3);
//            isStreamPath = true;
//        }
        
        FSENode n = handleResolver.getNodeCache(path);
        if (n != null)
        {
            //log.debug("Cached node " + path + " (" + nodeMap.size() + " entries)");
            n.setStreamPath(isStreamPath);
            return n;
        }
        
        //log.debug("Resolved path " + path + " to " + path);



        RemoteFSElem fse;
        try
        {
            fse = remoteFSApi.resolve_node(path);
        }
        catch (Exception sQLException)
        {
            log.debug("No such node: " + path + ": " + sQLException.getMessage());
            return null;
        }
        if (fse == null)
        {
           // log.debug("No such node: " + path);            
            return null;
        }
        FSENode node = new FSENode(fse, remoteFSApi );
        node.setStreamPath(isStreamPath);

        handleResolver.addNodeCache(node, path);
        
        log.debug("Resolved path: " + path);

        
        return node;
    }



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

    private int getFtype( ByteBuffer path ) throws FuseException
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

           
    private long open_FileHandle( FSENode mf,  boolean forWrite  ) throws FuseException
    {
        try
        {
            FileHandle handle = mf.open_file_handle( /*create*/forWrite);

            long handleNo = handleResolver.put_FileHandle( mf, handle );

            log.debug("->open_FileHandle " + mf.get_name() + ": " + handleNo);
            
            return handleNo;

        }
        catch (Exception e)
        {
            log.error("Exception in open_FileHandle: " + mf.get_path(), e);

            if (forWrite)
                throw new FuseException(e);
            else
                throw new FuseException(e);
        }
        }

    private void closeFileChannel( long handleNo )
    {
        log.debug("[closeFileChannel] " + handleNo);

            try
            {
            handleResolver.close_FileHandle(handleNo);
            }
            catch (IOException e)
            {
                log.error("unable to close channel" + handleNo, e);
            }
     }


    
    

    @Override
    public int getxattr( ByteBuffer path, ByteBuffer bname, ByteBuffer dst, long position )
    {
        traceEnter("getxattr", path);
//        dst.put("".getBytes());
//        try
//        {
//            int ftype = this.getFtype(path);
//            if (ftype != FuseFtype.TYPE_SYMLINK)
//            {
//                FSENode mf = this.resolvePath(path);
//
//                String name = resolveBuffer( bname );
//                String val = mf.get_xattribute(name);
//                val = null;
//                if (val != null)
//                {
//                    log.debug("val=" + val);
//                    dst.put(val.getBytes());
//                }
//                else
//                {
//                    throw new FuseException().initErrno(FuseException.ENODATA);
//                }
//            }
//        } catch (SQLException ex) {
//            return -FUSEErrorValues.EACCES;
//        } catch (FuseException ex) {
//            return 0;//-FUSEErrorValues.ENOENT;
//        }
//        finally
//        {
//            traceLeave("getxattr");
//        }
        return -FUSEErrorValues.ENOENT;
        //return 0;
    }

    @Override
    public int listxattr(ByteBuffer path,
			  ByteBuffer namebuf)
    {
        traceEnter("listxattr", path);
        StringBuilder sb = new StringBuilder();
        try
        {
            // Not supported
//            FSENode mf = this.resolvePath(path);
//            List<String> list = mf.list_xattributes();
//            for (int i = 0; i < list.size(); i++)
//            {
//                String string = list.get(i);
//                if (sb.length() > 0)
//                {
//                    sb.append(',');
//
//                }
//                sb.append(string);
//            }
            namebuf.put(sb.toString().getBytes());
        }
        catch (Exception e)
        {
            return -FUSEErrorValues.EACCES;
        }
        finally
        {
            traceLeave("listxattr");
        }
        return 0;
    }


    @Override
    public int removexattr( ByteBuffer path, ByteBuffer name )
    {
        traceEnter("removexattr", path);
         return -FUSEErrorValues.EACCES;
    }

    @Override
    public int setxattr( ByteBuffer path,
            ByteBuffer bname,
            ByteBuffer value,
            int flags,
            long position )
            
    {
        traceEnter("setxattr", path);
        try
        {
            byte valB[] = new byte[value.capacity()];
            value.get(valB);
            String valStr = new String(valB);

            FSENode mf = this.resolvePath(path);
            String name = resolveBuffer(bname);
            mf.add_xattribute(name, valStr);
        }
        catch (Exception e)
        {
            return -FUSEErrorValues.EACCES;
        }
        finally
        {
            traceLeave("setxattr");
        }
        return 0;
    }

    @Override
    public void setShutdownHook( ShutdownHook hook )
    {
        this.hook = hook;
    }

    @Override
    public ShutdownHook getShutdownHook()
    {
        return hook;
    }


}
