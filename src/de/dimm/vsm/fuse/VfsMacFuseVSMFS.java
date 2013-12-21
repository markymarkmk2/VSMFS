package de.dimm.vsm.fuse;

import de.dimm.vsm.Exceptions.FuseException;
import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.Exceptions.PoolReadOnlyException;
import de.dimm.vsm.Utilities.WinFileUtilities;
import de.dimm.vsm.fsutils.IVSMFS;
import de.dimm.vsm.fsutils.MacShutdownHook;
import de.dimm.vsm.fsutils.RemoteStoragePoolHandler;
import de.dimm.vsm.fsutils.ShutdownHook;
import de.dimm.vsm.net.RemoteFSElem;
import de.dimm.vsm.vfs.IVfsDir;
import de.dimm.vsm.vfs.IVfsFsEntry;
import de.dimm.vsm.vfs.IVfsHandler;
import de.dimm.vsm.vfs.OpenVfsFsEntry;
import de.dimm.vsm.vfs.VfsHandler;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.List;
import net.decasdev.dokan.DokanFileInfo;
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




public class VfsMacFuseVSMFS extends MacFUSEFileSystemAdapter/*//FUSEFileSystemAdapter*/ implements IVSMFS
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
    

    

    RemoteStoragePoolHandler remoteFSApi;

    FUSEOptions fuse_args;
    private Logger log;
    boolean rdwr;
    IVfsHandler vfsHandler;
       
    public VfsMacFuseVSMFS( RemoteStoragePoolHandler handler, String mountPoint, Logger logger, String[] fuse_args, boolean rdwr ) throws SQLException
    {

        remoteFSApi = handler;
        this.mountPoint = mountPoint;
        this.fuse_args = new FUSEOptions();
        this.log = logger;
        this.rdwr = rdwr;
        this.fuse_args.setFsname("VSMFS");
        
        
        
        vfsHandler = new VfsHandler(remoteFSApi);
        
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
            else if (string instanceof FUSEFileInfo)
            {
                sb.append("FH: ");
                sb.append(((FUSEFileInfo)string).fh);
            }
            else
                sb.append(string.toString());
            sb.append(" ");
        }
        System.out.println(sb.toString());
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
        System.out.println(sb.toString());
    }
    
    static void traceLeave( Object ...s)
    {
        StringBuilder sb = new StringBuilder("TraceLeave: ");
        for (Object string : s) {
            sb.append(string);
            sb.append(" ");
        }
        System.out.println(sb.toString());
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

        vfsHandler.closeAll();
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
        catch (IOException | InterruptedException iOException)
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
        IVfsFsEntry entry;
        
        try
        {
            // Ask for existing node only it can exist
            entry = resolveNode(pathStr);
            
            if (entry != null && entry.exists())
            {
                return -FUSEErrorValues.EEXIST;
            }
            entry = vfsHandler.createFileEntry(pathStr, mode);
                        
            if (entry == null)
            {
                return -FUSEErrorValues.EACCES;
            }
            fi.fh = vfsHandler.openEntryForWrite(entry);
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
        traceLeave("create");
        return 0;
    }

    public int chmod( ByteBuffer path, int mode ) throws FuseException
    {
        traceEnter("chmod");
        String pathStr = resolveBuffer(path);          

        try
        {
            IVfsFsEntry entry = resolveNode(pathStr);
            entry.setPosixMode( -1, mode);
        }
        catch (IOException | SQLException | PoolReadOnlyException e)
        {                
            log.error("unable to chmod " + path, e);
            throw new FuseException("access denied for " + pathStr).initErrno(FuseException.EACCES);
        }
        
        traceLeave("chmod");
        return 0;
    }

    @Override
    public int chown( ByteBuffer path, long uid, long gid )
    {
        
        traceEnter("chown");
        String pathStr = resolveBuffer(path);          
        
        try
        {
            IVfsFsEntry entry = resolveNode(pathStr);
            entry.setOwner(-1, (int)uid);
            entry.setGroup( -1, (int)gid);
        }
        catch (IOException | SQLException | PoolReadOnlyException e)
        {
            log.error("unable to chown " + pathStr, e);
            return -1;
        }

        traceLeave("chown");
        return 0;
    }

    @Override
    public int flush( ByteBuffer path, FUSEFileInfo fh )
    {
        traceEnter("flush", path, fh);
        //String pathStr = resolveBuffer(path);    
        
        try
        {
            OpenVfsFsEntry entry = vfsHandler.getEntryByHandle( fh.fh);
            
            entry.flush();
        }
        catch (IOException | SQLException | PoolReadOnlyException | PathResolveException e)
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
        String pathStr = resolveBuffer(path);    
        
        
        try
        {
            OpenVfsFsEntry entry = vfsHandler.getEntryByHandle( fh.fh);
            
            entry.flush();           
        }
        catch (IOException | SQLException | PoolReadOnlyException | PathResolveException e)
        {
            log.error("unable to sync file", e);
            return -1;
        }
        traceLeave("fsync");
        return 0;
    }

    private static void mfillStat( Stat stat, IVfsFsEntry mf )
    {
        int uid = mf.getOwner();
        int gid = mf.getGroup();
        int mode = mf.getPosixMode();
        long fileLength = mf.getSize();
        //long actualBytes = mf.getActualBytesWritten();
        stat.st_atimespec.setToMillis(mf.getAtimeMs());
        stat.st_mtimespec.setToMillis(mf.getMtimeMs());
        stat.st_ctimespec.setToMillis(mf.getCtimeMs());
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
        
        
        try
        {
            IVfsFsEntry entry = resolveNode(pathStr);
            if (entry == null)
            {            
                traceLeave("getattr noent");
                return -FUSEErrorValues.ENOENT;
            }
            mfillStat( stat, entry );
            stat.printFields( "", System.out);
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
        
        
        try
        {
            String pathStr = resolveBuffer(path);          
            IVfsFsEntry entry = resolveNode(pathStr); 

            if (entry.isDirectory())
            {
                IVfsDir dir = (IVfsDir)entry;
                List<IVfsFsEntry> list = dir.listChildren();

                dirFiller.fill(FUSEUtil.encodeUTF8("."), ".".hashCode(), FuseFtype.TYPE_DIR);
                dirFiller.fill(FUSEUtil.encodeUTF8(".."), "..".hashCode(), FuseFtype.TYPE_DIR);
                for (int i = 0; i < list.size(); i++)
                {
                    IVfsFsEntry cf = list.get(i);

                    traceLog("getdir",  cf.getName());

                    dirFiller.fill( FUSEUtil.encodeUTF8(cf.getName()), getFtype(cf), cf.getGUID() );

                    if (cf.getStreamSize() > 0)
                    {
                        dirFiller.fill(FUSEUtil.encodeUTF8("._" + cf.getName()), getFtype(cf), cf.getGUID());
                    }
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
        
        try
        {
            String pathStr = resolveBuffer(path);          
            IVfsFsEntry entry = resolveNode(pathStr); 

            if (entry.isDirectory())
            {
                IVfsDir dir = (IVfsDir)entry;
                List<IVfsFsEntry> list = dir.listChildren();

                dirFiller.fill(FUSEUtil.encodeUTF8("."), null, 0);
                dirFiller.fill(FUSEUtil.encodeUTF8(".."), null, 0);
                for (int i = 0; i < list.size(); i++)
                {
                    Stat stat = new Stat();
                    IVfsFsEntry cf = list.get(i);

                    mfillStat( stat, entry );
                    dirFiller.fill(FUSEUtil.encodeUTF8(cf.getName()), null, 0);

                    traceLog("readdir",  cf.getName());
                }
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
        
        try
        {
            IVfsFsEntry entry = resolveNode(pathStr); 
            if (entry != null && entry.exists())
            {
                return -FUSEErrorValues.EEXIST;
            }
            vfsHandler.mkdir(pathStr, mode);
            
            entry = vfsHandler.getEntry( pathStr);  
            if (entry == null)
            {
                return -FUSEErrorValues.EACCES;
            }
        }
        catch (PoolReadOnlyException | PathResolveException | IOException | SQLException ex)
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
        

        try
        {
            IVfsFsEntry entry = resolveNode(pathStr); 
            if (entry != null && entry.exists())
            {
                return -FUSEErrorValues.EEXIST;
            }
            else
            {
                return -FUSEErrorValues.EINVAL;
            }
        }
        catch ( IOException e)
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
            String pathStr = resolveBuffer(path);          
            IVfsFsEntry entry = resolveNode(pathStr); 
            if (entry == null)
            {
                log.error("unable to open " + path);
                return -FUSEErrorValues.EACCES;
            }

            if (create)
                fi.fh = vfsHandler.openEntryForWrite( entry );
            else
                fi.fh = vfsHandler.openEntry( entry );
            
        }
        catch (IOException | SQLException | PoolReadOnlyException | PathResolveException e)
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
        String pathStr = resolveBuffer(path);          
        
        
        log.debug("Reading " + buf.capacity() + " byte from handle " + fi.fh + " File " + pathStr );
        try
        {
            OpenVfsFsEntry entry = vfsHandler.getEntryByHandle( fi.fh);            
            byte[] b = entry.read( offset, buf.capacity());                    
            buf.put(b);                
            return b.length;
        }
        catch (IOException | SQLException | PoolReadOnlyException | PathResolveException e)
        {
            log.error("unable to read " + buf.capacity() + " byte offset " + offset + " from file " + pathStr + ": " + e.getMessage());
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
        String pathStr = resolveBuffer(path);                  
            
        try
        {
            IVfsFsEntry entry = resolveNode(pathStr);
            String lpath = entry.readSymlink();
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
    public int release( ByteBuffer path, FUSEFileInfo fi )
    {
        traceEnter("release", path);
        
        // log.info("closing " + path + " with flags " + flags);
        OpenVfsFsEntry entry = vfsHandler.getEntryByHandle( fi.fh);                        
        try
        {
            vfsHandler.closeEntry( entry );
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
            vfsHandler.moveNode( f, t);
        }
        catch (IOException | SQLException | PoolReadOnlyException | PathResolveException e)
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

        String pathStr = resolveBuffer(path);   
        if (pathStr.equals(".") || pathStr.equals(".."))
        {
            return 0;
        }
        else
        {
            try
            {                       
                IVfsFsEntry entry = resolveNode(pathStr);
                if (!entry.isDirectory())
                    return -FUSEErrorValues.EACCES;
                IVfsDir dir = (IVfsDir)entry;
        
                if (vfsHandler.removeDir(dir))
                {
                    return 0;
                }
                else
                {
                    log.debug("unable to delete folder " + pathStr);
                    return -FUSEErrorValues.EACCES;
                }
            }
            catch (IOException | SQLException | PoolReadOnlyException poolReadOnlyException)
            {
                log.debug("unable to delete folder " + pathStr + ", read only fs");
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
        //traceEnter("statfs", path);
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

        //traceLeave("statfs");
        return 0;
    }

    @Override
    public int access(ByteBuffer path, int mode) 
    {
        //traceEnter("access", path);
        String pathStr = resolveBuffer(path);   
        IVfsFsEntry entry = null;
        try
        {
            entry = resolveNode(pathStr);
        }
        catch (IOException iOException)
        {
            traceLeave("access nok: " + pathStr);
            return -1;
        }
        
        if (entry == null)
        {
            traceLeave("access nok: " + pathStr);
            return -1;
        }
        
        //traceLeave("access ok");
        return 0;
    }
    

    @Override
    public int symlink( ByteBuffer from, ByteBuffer to )
    {
        traceEnter("symlink", from.toString());
        String f = resolveBuffer( from );
        String t = resolveBuffer( to );

        try
        {
            IVfsFsEntry fentry = vfsHandler.getEntry( f);  
            IVfsFsEntry tentry = vfsHandler.getEntry( t);  
            if (tentry != null && tentry.exists())
            {
                throw new FuseException().initErrno(FuseException.EPERM);
            }
            fentry.createSymlink(t);
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
            String pathStr = resolveBuffer(path);   
            IVfsFsEntry entry = resolveNode(pathStr);
            entry.truncate(-1, size);                
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
             
            IVfsFsEntry entry = resolveNode(pathStr);

            if (vfsHandler.unlink(entry))
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
        String pathStr = resolveBuffer( path );
        try
        {
            IVfsFsEntry entry = resolveNode(pathStr); 
            entry.setLastAccessed(-1, time.actime * 1000L);
            entry.setLastModified(-1, time.modtime * 1000L);
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
        String pathStr = resolveBuffer( path );
        try
        {
            IVfsFsEntry entry = resolveNode(pathStr);
            entry.setLastAccessed(-1, accessTime.toMillis());
            entry.setLastModified(-1, modificationTime.toMillis());
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
        OpenVfsFsEntry entry = vfsHandler.getEntryByHandle( fi.fh);                        
        byte[] b = new byte[buf.capacity()];
        buf.get(b);
        try
        {
            log.debug("Writing " + buf.capacity() + " byte "  + " at pos " + offset + " to handle " + fi.fh  );
            entry.write( offset,  b.length, b );  
            return b.length;
        }
        catch (IOException | SQLException | PoolReadOnlyException | PathResolveException e)
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

    private int getFtype( IVfsFsEntry _f ) throws FuseException
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
        traceLeave("getxattr");
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
            

            //FSENode mf = this.resolvePath(path);
            String name = resolveBuffer(bname);
            log.debug("Name=" + name + " Val=" + valStr);
            //mf.add_xattribute(name, valStr);
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


    private void log( String string )
    {
        //logs( string);
         log.debug(string);
    }
    private void log( String string, Exception e )
    {
        //logs( string);
         log.error(string, e);
    }
    private IVfsFsEntry resolveNode( String winpath ) throws IOException
    {
        String path = WinFileUtilities.win_to_sys_path(winpath);
        //log.debug("Resolved path " + winpath + " to " + path);
        
        IVfsFsEntry entry;
        try
        {
            entry = vfsHandler.getEntry( path );
        }
        catch (IOException | SQLException iOException)
        {
            return null;
        }
        return entry;
    }
    private OpenVfsFsEntry resolveNodeOpenedFile( String winpath, DokanFileInfo arg1) throws IOException
    {
        String path = WinFileUtilities.win_to_sys_path(winpath);
         try
        {
            OpenVfsFsEntry entry = vfsHandler.getEntryByHandle(arg1.handle);
            if (entry == null) {
                throw new IOException("Node not open " + path);
//                entry = vfsHandler.getEntry( path );
//                if (entry == null) {
//                    throw new IOException("Node not found " + path);
//                }
//                entry.open(true);
            }
            return entry;
        }
        catch (IOException /*| SQLException | PoolReadOnlyException | PathResolveException*/ e)
        {
            log("[resolveNodeFile] " + winpath + " failed with not found ", e);
            throw new IOException("FileNotFound");
        }      
    }  
    private IVfsFsEntry resolveNodeDirMustExist( String winpath ) throws IOException
    {
        IVfsFsEntry entry = resolveNode( winpath );
        if (entry == null)
        {
            log("[resolveNodeDir] " + winpath + " failed with not found");
            throw new IOException("DirNotFound");
        }
        return entry;
    }  
    
}
