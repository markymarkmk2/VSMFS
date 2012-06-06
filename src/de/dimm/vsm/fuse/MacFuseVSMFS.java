package de.dimm.vsm.fuse;

import de.dimm.vsm.Exceptions.FuseException;
import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.Exceptions.PoolReadOnlyException;
import de.dimm.vsm.fsengine.HandleResolver;
import de.dimm.vsm.fsutils.FSENode;
import de.dimm.vsm.fsutils.MacShutdownHook;
import de.dimm.vsm.fsutils.RemoteStoragePoolHandler;
import de.dimm.vsm.fsutils.ShutdownHook;
import de.dimm.vsm.net.interfaces.FileHandle;
import de.dimm.vsm.fsutils.VSMFS;
import de.dimm.vsm.net.RemoteFSElem;
import java.io.File;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.List;

import org.apache.log4j.Logger;
import org.catacombae.jfuse.FUSE;
import org.catacombae.jfuse.FUSEErrorValues;
import org.catacombae.jfuse.FUSEFileSystemAdapter;
import org.catacombae.jfuse.FUSEOptions;
import org.catacombae.jfuse.types.fuse26.FUSEDirFil;
import org.catacombae.jfuse.types.fuse26.FUSEFileInfo;
import org.catacombae.jfuse.types.fuse26.FUSEFillDir;
import org.catacombae.jfuse.types.system.Stat;
import org.catacombae.jfuse.types.system.StatVFS;
import org.catacombae.jfuse.types.system.Timespec;
import org.catacombae.jfuse.types.system.Utimbuf;
import org.catacombae.jfuse.util.FUSEUtil;




public class MacFuseVSMFS extends FUSEFileSystemAdapter implements VSMFS
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

    public MacFuseVSMFS( RemoteStoragePoolHandler handler, String mountPoint, Logger logger, String[] fuse_args   )
    {

        remoteFSApi = handler;
        this.mountPoint = mountPoint;
        this.fuse_args = new FUSEOptions();
        this.log = logger;

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

        log.info("mounting " + handler.getName() + " to " + mountPoint + " with opts " + opts.toString());
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
    @Override
    public boolean mount()
    {
        try
        {
            FUSE.mount(this, mountPoint, fuse_args );
            return true;
        }
        catch (Exception e)
        {
                 log.error("unable to mount " + mountPoint, e);
        }
        return false;
    }

    public static boolean unmount( String mp ) throws IOException, InterruptedException
    {

            Process p = Runtime.getRuntime().exec("umount " + mp);
            p.waitFor();

            return true;
        }
    
    @Override
    public boolean unmount( )
    {
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
        return ret;
    }


    public int chmod( ByteBuffer path, int mode ) throws FuseException
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
    public int chown( ByteBuffer path, long uid, long gid )
    {
        FSENode mf = resolvePath(path);
        
            try
            {
                mf.set_owner_id((int)uid);
                mf.set_group_id((int)gid);
            }
            catch (Exception e)
            {
                log.error("unable to chown " + path, e);
                return -1;
            }

        return 0;
    }

    @Override
    public int flush( ByteBuffer path, FUSEFileInfo fh )
    {
        FileHandle ch = handleResolver.get_handle_by_info( fh );
        try
        {
            ch.force(true);
        }
        catch (IOException e)
        {
            log.error("unable to sync file", e);
            return -1;
        }
        return 0;
    }

    @Override
    public int fsync( ByteBuffer path,  boolean isDatasync, FUSEFileInfo fh )
            
    {
        FileHandle ch = handleResolver.get_handle_by_info( fh );
        try
        {
            ch.force(true);
        }
        catch (IOException e)
        {
            log.error("unable to sync file", e);
            return -1;
        }
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
        FSENode mf = resolvePath(path);
  
        if (mf == null)
            return -1;
        
        try
        {
            mfillStat( stat, mf );

        }
        catch (Exception e)
        {
            log.error("unable to parse attributes " + path, e);
            return -1;
        }
        
        return 0;
    }

    @Override
    public int getdir( ByteBuffer path, FUSEDirFil dirFiller )
           
    {
        FSENode f = null;
        try
        {
            f = resolvePath(path);

            List<RemoteFSElem> list = remoteFSApi.get_child_nodes(f.getNode());
            
            dirFiller.fill(FUSEUtil.encodeUTF8("."), ".".hashCode(), FuseFtype.TYPE_DIR);
            dirFiller.fill(FUSEUtil.encodeUTF8(".."), "..".hashCode(), FuseFtype.TYPE_DIR);
            for (int i = 0; i < list.size(); i++)
            {
                RemoteFSElem cf = list.get(i);
                
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
        finally
        {
            f = null;
        }
        return 0;
    }

    @Override
    public int readdir(ByteBuffer path,
			FUSEFillDir dirFiller,
			long offset,
			FUSEFileInfo fi)
    {
        FSENode f = null;
        try
        {
            f = resolvePath(path);

            List<RemoteFSElem> list = remoteFSApi.get_child_nodes(f.getNode());

            dirFiller.fill(FUSEUtil.encodeUTF8("."), null, 0);
            dirFiller.fill(FUSEUtil.encodeUTF8(".."), null, 0);
            for (int i = 0; i < list.size(); i++)
            {
                RemoteFSElem cf = list.get(i);

//                Stat stat = new Stat();
//                fillStat( stat, cf );
                dirFiller.fill(FUSEUtil.encodeUTF8(cf.getName()), null, 0);
            }
        }
        catch (Exception e)
        {
            log.error("unable to getdir " + path, e);
            return -1;
        }
        finally
        {
            f = null;
        }
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
        FSENode mf = resolvePath(path);
        
        try
        {
            if (mf != null && mf.exists())
            {
                return -FUSEErrorValues.EEXIST;
            }
            remoteFSApi.mkdir(mf.get_path());
        }
        catch (IOException iOException)
        {
            return -FUSEErrorValues.EINVAL;
        }
        catch (PathResolveException iOException)
        {
            return -FUSEErrorValues.EINVAL;
        }
        catch (PoolReadOnlyException pox)
        {
            return -FUSEErrorValues.EINVAL;
        }
        mf = resolvePath(path);
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
        return 0;
    }

    @Override
    public int mknod( ByteBuffer path, short mode, long rdev )
    {
        // log.info("mknod(): " + path + " " + mode + " " + rdev + "\n");
        FSENode mf = resolvePath(path);
       

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

        
        //return 0;
    }

    @Override
    public int open( ByteBuffer path, FUSEFileInfo fi )
            
    {
        try
        {
            FSENode mf = resolvePath(path);
            
            boolean create = fi.getFlagCreate();
            
            fi.fh = open_FileHandle(mf, create);
        }
        catch (Exception e)
        {
            log.error("unable to open " + path, e);
            return -FUSEErrorValues.EACCES;
        }
        return 0;
    }

    @Override
    public int read( ByteBuffer path,  ByteBuffer buf, long offset, FUSEFileInfo fi )
           
    {
        // log.info("Reading " + path + " at " + offset + " with buffer " +
        // buf.capacity());
        
        
        String f = resolve_buffer( path );
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

    }

    @Override
    public int readlink( ByteBuffer path, ByteBuffer link )
    {
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
       
        return 0;
    }

    @Override
    public int release( ByteBuffer path, FUSEFileInfo fh )
    {
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
        return 0;
    }

    @Override
    public int rename( ByteBuffer from, ByteBuffer to )
    {        
        try
        {
            String f = resolve_buffer( from );
            String t = resolve_buffer( to );
            remoteFSApi.move_fse_node( f, t);
        }
        catch (Exception e)
        {
            log.warn("unable to rename " + from  + " " + to, e);
            return -FUSEErrorValues.EACCES;
        }
        return 0;
    }

    @Override
    public int rmdir( ByteBuffer path )
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
        }
    }

    @Override
    public int statfs( ByteBuffer path, StatVFS stat )
    {
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
        stat.f_ffree = 0;
        stat.f_favail = 0;
        stat.f_files = 0;

        stat.f_namemax = NAME_LENGTH;
        
        //stat.printFields("de", System.out);

        return 0;
    }

    @Override
    public int access(ByteBuffer path, int mode) 
    {
        FSENode ffrom = resolvePath(path);
        if (ffrom == null)
            return -1;
        
        return 0;
    }
    

    @Override
    public int symlink( ByteBuffer from, ByteBuffer to )
    {
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
        return 0;
    }

    @Override
    public int truncate( ByteBuffer path, long size )
    {
        try
        {
            FSENode ffrom = resolvePath(path);

            ffrom.truncate(size);            
        }
        catch (Exception e)
        {
            log.error("unable to truncate file " + path, e);
            return -FUSEErrorValues.EACCES;
        }
        return 0;
    }

    @Override
    public int unlink( ByteBuffer path )
    {
        FSENode f = this.resolvePath(path);
        try
        {
            if (remoteFSApi.remove_fse_node(f.get_path()))
            {
                return 0;
            }
            else
            {
                log.warn("unable to delete file " + f.get_path());
                return -FUSEErrorValues.EACCES;
            }
        }
        catch (Exception e)
        {
            log.error("unable to file file " + path, e);
            return -FUSEErrorValues.EACCES;
        }
    }

    @Override
    public int utime( ByteBuffer path, Utimbuf time )
    {
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
        return 0;
    }
    @Override
    public int utimens(ByteBuffer path,
			Timespec accessTime,
                        Timespec modificationTime)
    {
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
        return 0;
    }

    @Override
    public int write( ByteBuffer path, ByteBuffer buf,
		      long offset,
		      FUSEFileInfo fi )
    {
        /*
         * log.info("writing data to  " +path + " at " + offset +
         * " and length of " + buf.capacity());
         */
        FileHandle ch = handleResolver.get_handle_by_info(fi);
        byte[] b = new byte[buf.capacity()];
        buf.get(b);
        try
        {
            ch.writeFile(b,  b.length, offset);
            return b.length;
        }
        catch (Exception e)
        {
            log.error("unable to write to file" + path, e);
            return -FUSEErrorValues.EACCES;
        }
    
    }

    private String resolve_buffer( ByteBuffer bpath )
    {
        String pathString = FUSEUtil.decodeUTF8(bpath);
        
        return pathString;
        
    }
    private FSENode resolvePath( ByteBuffer bpath )
    {
        boolean isStreamPath = false;
        String path = resolve_buffer(bpath);
        int idx = path.lastIndexOf('/');
        if (idx >= 0 && path.length() - idx > 2 && path.charAt(idx + 1) == '.' && path.charAt(idx + 2) == '_')
        {
            path = path.substring(0, idx + 1) + path.substring(idx + 3);
            isStreamPath = true;
        }
        
        FSENode n = handleResolver.getNodeCache(path);
        if (n != null)
        {
            //log.debug("Cached node " + path + " (" + nodeMap.size() + " entries)");
            n.setStreamPath(isStreamPath);
            return n;
        }
        
        //log.debug("Resolved path " + path + " to " + path);



        RemoteFSElem fse = null;
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


    
    
//
//    @Override
//    public int getxattr( ByteBuffer path, ByteBuffer bname, ByteBuffer dst, long position )
//    {
//        try
//        {
//            int ftype = this.getFtype(path);
//            if (ftype != FuseFtype.TYPE_SYMLINK)
//            {
//                FSENode mf = this.resolvePath(path);
//
//                String name = resolve_buffer( bname );
//                String val = mf.get_xattribute(name);
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
//        }
//        catch (FuseException fuseException)
//        {
//            return -FUSEErrorValues.EACCES;
//        }
//        return 0;
//    }
//    @Override
//    public int listxattr(ByteBuffer path,
//			  ByteBuffer namebuf)
//    {
//        StringBuilder sb = new StringBuilder();
//        try
//        {
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
//            namebuf.put(sb.toString().getBytes());
//        }
//        catch (Exception e)
//        {
//            return -FUSEErrorValues.EACCES;
//        }
//        return 0;
//    }
//
//
//
//
//
//    @Override
//    public int removexattr( ByteBuffer path, ByteBuffer name )
//    {
//         return -FUSEErrorValues.EACCES;
//    }
//
//    @Override
//    public int setxattr( ByteBuffer path,
//            ByteBuffer bname,
//            ByteBuffer value,
//            int flags,
//            long position )
//            
//    {
//        try
//        {
//            byte valB[] = new byte[value.capacity()];
//            value.get(valB);
//            String valStr = new String(valB);
//
//            FSENode mf = this.resolvePath(path);
//            String name = resolve_buffer(bname);
//            mf.add_xattribute(name, valStr);
//        }
//        catch (Exception e)
//        {
//            return -FUSEErrorValues.EACCES;
//        }
//        return 0;
//    }

    public void setShutdownHook( ShutdownHook hook )
    {
        this.hook = hook;
    }

    public ShutdownHook getShutdownHook()
    {
        return hook;
    }


}
