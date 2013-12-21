
package de.dimm.vsm.dokan;

import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.Exceptions.PoolReadOnlyException;
import de.dimm.vsm.Utilities.WinFileUtilities;
import de.dimm.vsm.VSMFSLogger;
import de.dimm.vsm.fsutils.RemoteStoragePoolHandler;
import de.dimm.vsm.fsutils.ShutdownHook;
import de.dimm.vsm.fsutils.Utils;
import de.dimm.vsm.fsutils.IVSMFS;
import de.dimm.vsm.net.RemoteFSElem;
import de.dimm.vsm.vfs.IVfsDir;
import de.dimm.vsm.vfs.IVfsFsEntry;
import de.dimm.vsm.vfs.IVfsHandler;
import de.dimm.vsm.vfs.OpenVfsFsEntry;
import de.dimm.vsm.vfs.VfsHandler;
import java.sql.SQLException;
import net.decasdev.dokan.DokanOptions;
import java.util.ArrayList;
import static net.decasdev.dokan.CreationDisposition.CREATE_ALWAYS;
import static net.decasdev.dokan.CreationDisposition.CREATE_NEW;
import static net.decasdev.dokan.CreationDisposition.OPEN_ALWAYS;
import static net.decasdev.dokan.CreationDisposition.OPEN_EXISTING;
import static net.decasdev.dokan.CreationDisposition.TRUNCATE_EXISTING;
import static net.decasdev.dokan.FileAttribute.FILE_ATTRIBUTE_DIRECTORY;
import static net.decasdev.dokan.FileAttribute.FILE_ATTRIBUTE_NORMAL;
import static net.decasdev.dokan.WinError.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import net.decasdev.dokan.ByHandleFileInformation;
import net.decasdev.dokan.Dokan;
import net.decasdev.dokan.DokanDiskFreeSpace;
import net.decasdev.dokan.DokanFileInfo;
import net.decasdev.dokan.DokanOperationException;
import net.decasdev.dokan.DokanOperations;
import net.decasdev.dokan.DokanVolumeInformation;
import net.decasdev.dokan.FileTimeUtils;
import net.decasdev.dokan.Win32FindData;
import net.decasdev.dokan.WinError;
import org.apache.log4j.Logger;




public class VfsDokanVSMFS implements DokanOperations, IVSMFS
{

    ShutdownHook hook;

    public static final int maxOpenFiles=1000;
    public static final int writeThreads=1;

    // TODO FIX THIS
    public static final int FILE_CASE_PRESERVED_NAMES = 0x00000002;
    public static final int FILE_FILE_COMPRESSION = 0x00000010;
    public static final int FILE_SUPPORTS_SPARSE_FILES = 0x00000040;
    public static final int FILE_UNICODE_ON_DISK = 0x00000004;
    public static final int SUPPORTED_FLAGS = FILE_CASE_PRESERVED_NAMES | FILE_UNICODE_ON_DISK | FILE_SUPPORTS_SPARSE_FILES;
    //final static int volumeSerialNumber = 42424242;
    final static int volumeSerialNumber = 11223344;



    final long rootCreateTime = FileTimeUtils.toFileTime(new Date());
    long rootLastWrite = rootCreateTime;
    String drive;
    RemoteStoragePoolHandler remoteFSApi;

    IVfsHandler vfsHandler;
    

    private Logger log = VSMFSLogger.getLog();


    private boolean useVirtualFS = false;
    


    public VfsDokanVSMFS( RemoteStoragePoolHandler api,  String drive, Logger log ) throws SQLException
    {
        this.remoteFSApi = api;
        this.drive = drive;
        this.log = log;
        
        showVersions();

        hook = new ShutdownHook( drive, false );
        vfsHandler = new VfsHandler(remoteFSApi);
    }

    final void showVersions()
    {
        int version = Dokan.getVersion();
        log.info("Dokan V" + version);
        int driverVersion = Dokan.getDriverVersion();
        log.info("DokanDriver V" + driverVersion);
    }

    String get_cr_dispo_txt( int cr )
    {
        switch( cr)
        {
            case CREATE_ALWAYS: return "create always";
            case CREATE_NEW: return "create new";
            case OPEN_ALWAYS: return "open always";
            case OPEN_EXISTING: return "open existing";
            case TRUNCATE_EXISTING: return "truncate existing";
        }
        return "Unknown creationDisposition";
    }

    @Override
    public boolean mount()
    {
        // Sicherheitshalber denselben Mount entfernen
        Dokan.removeMountPoint(drive);
        
        //this.mountedVolume = mountedVolume;
        DokanOptions dokanOptions = new DokanOptions();
        //dokanOptions.driveLetter = driveLetter;
        dokanOptions.mountPoint = drive;
        dokanOptions.threadCount = writeThreads;

        dokanOptions.optionsMode |= DokanOptions.DOKAN_OPTION_REMOVABLE;
//        dokanOptions.optionsMode |= DokanOptions.DOKAN_OPTION_DEBUG;
//        dokanOptions.optionsMode |= DokanOptions.DOKAN_OPTION_STDERR;
        //dokanOptions.optionsMode = DokanOptions.DOKAN_OPTION_ALT_STREAM;
/*
        dokanOptions.optionsMode |= DokanOptions.DOKAN_OPTION_DEBUG;
        dokanOptions.optionsMode |= DokanOptions.DOKAN_OPTION_STDERR;*/


        String mount_name = remoteFSApi.getName();
        log.info("Mounting " + mount_name + " to " + this.drive);
        int result = Dokan.mount(dokanOptions, this);
        
        if (result == 0)
        {
            Runtime.getRuntime().addShutdownHook(hook);            
        }

        return result == 0 ? true : false;
        
    }

    public static boolean unmount( String drive )
    {
        return Dokan.removeMountPoint(drive);
    }
    
    @Override
    public boolean unmount()
    {
        log("[unmount] disconnecting");
        remoteFSApi.disconnect();
        vfsHandler.closeAll();

        log("[unmount] removeMountPoint " + drive);
        boolean ret = Dokan.removeMountPoint(drive);

        if (ret)
            Runtime.getRuntime().removeShutdownHook(hook);

        return ret;
    }


    @Override
    public long onCreateFile( String fileName, int desiredAccess, int shareMode,
            int creationDisposition, int flagsAndAttributes, DokanFileInfo arg5 )
            throws DokanOperationException
    {
        String path = WinFileUtilities.win_to_sys_path(fileName);
        

        log("[onCreateFile] " + fileName + " <" + get_cr_dispo_txt(creationDisposition) + "> " + arg5.toString());

        if (creationDisposition == CREATE_NEW)
        {
            creationDisposition = CREATE_NEW;
        }
        
        IVfsFsEntry entry = null;
        
        // Ask for existing node only it can exist
        entry = resolveNode(fileName);
//        if (creationDisposition != CREATE_NEW)
//        {
//            
//        }        

        if (entry != null)
        {
            switch (creationDisposition)
            {
                case CREATE_NEW:
                            throw new DokanOperationException(ERROR_ALREADY_EXISTS);
                case OPEN_ALWAYS:

                case OPEN_EXISTING:
                {                                                 
                    if (entry.isDirectory())
                    {
                        try
                        {
                            long handle = vfsHandler.openEntry(entry);                            
                            return handle; //getNextHandle();
                        }
                        catch (IOException | SQLException | PoolReadOnlyException | PathResolveException iOException)
                        {
                            throw new DokanOperationException(ERROR_ACCESS_DENIED);
                        }
//                       log.error( "CreateFile with Dir called");
//                       throw new DokanOperationException(ERROR_ACCESS_DENIED);
                    }

                    long handle;
                    try
                    {
                        if (creationDisposition == OPEN_ALWAYS)
                        {                            
                            handle = vfsHandler.openEntryForWrite( entry );
                        }
                        else                        
                        {
                            handle = vfsHandler.openEntry( entry );
                        }
                    }
                    catch (IOException | SQLException | PoolReadOnlyException | PathResolveException exception)
                    {
                        log.debug("access denied: " + path, exception);
                        throw new DokanOperationException(WinError.ERROR_ACCESS_DENIED);
                    }
                    
                        
                    return handle;
                }
                case CREATE_ALWAYS:
                case TRUNCATE_EXISTING:
                {
                    flushDirListCache();
                    if (entry.isDirectory())
                    {
                       log.error( "CreateFile with Dir called");
                       throw new DokanOperationException(ERROR_ACCESS_DENIED);
                    }
                    try
                    {
                        //long fileHandle = getNextHandle();
                        long handle = vfsHandler.openEntryForWrite( entry );
                        if (handle <= 0)
                                throw new DokanOperationException(WinError.ERROR_ACCESS_DENIED);
                        
                        if (creationDisposition == TRUNCATE_EXISTING)
                            entry.truncate(handle, 0);

                        //closeFileChannel(mf.getNode().getIdx());
                        return handle;
                    }

                    catch (IOException | SQLException | PoolReadOnlyException | PathResolveException | DokanOperationException e)
                    {
                        log.error("unable to clear file "  + entry.getPath(), e);
                        throw new DokanOperationException( WinError.ERROR_ACCESS_DENIED);
                    }
                }
            }
        }
        else
        {
            flushDirListCache();            
            switch (creationDisposition)
            {
                case CREATE_NEW:
                case CREATE_ALWAYS:
                case OPEN_ALWAYS:
                    try
                    {
                        log.debug("creating " + path);
                        entry = vfsHandler.createFileEntry( path, 0 );

                        if (entry == null)
                            throw new DokanOperationException(WinError.ERROR_ACCESS_DENIED);

                        long handle = vfsHandler.openEntryForWrite( entry );
                        if (handle <= 0)
                            throw new DokanOperationException(WinError.ERROR_ACCESS_DENIED);
                        return handle;
                    }
                    catch (IOException | SQLException | PathResolveException | PoolReadOnlyException | DokanOperationException e)
                    {
                        log.error("unable to create file " + path, e);
                        throw new DokanOperationException(WinError.ERROR_ACCESS_DENIED);
                    }                    
                case OPEN_EXISTING:
                case TRUNCATE_EXISTING:
                {
                    log.debug("not found: " + path);
                    throw new DokanOperationException(ERROR_FILE_NOT_FOUND);
                }
            }
        }
        throw new DokanOperationException(1);
    }

    @Override
    public long onOpenDirectory( String winpathName, DokanFileInfo arg1 )
            throws DokanOperationException
    {
        log("[onOpenDirectory] " + winpathName + " " + arg1.toString());
        
        IVfsFsEntry entry = resolveNodeDirMustExist(winpathName);

        try
        {
            long handle = vfsHandler.openEntry( entry );                
            return handle; //getNextHandle();
        }
        catch (IOException | SQLException | PoolReadOnlyException | PathResolveException iOException)
        {
            throw new DokanOperationException(ERROR_ACCESS_DENIED);
        }
    }

    @Override
    public void onCreateDirectory( String winPathName, DokanFileInfo file )
            throws DokanOperationException
    {
        log("[onCreateDirectory] " + winPathName + " " + file.toString());        

        IVfsFsEntry entry = resolveNode(winPathName);
        if (entry != null)
        {
            throw new DokanOperationException(WinError.ERROR_ALREADY_EXISTS);
        }
        try
        {
            String pathName = WinFileUtilities.win_to_sys_path(winPathName);
            vfsHandler.mkdir( pathName, 0 );                     
        }
        catch (PoolReadOnlyException exc)
        {
            log.error( "PoolReadOnlyException in onCreateDirectory: " + exc.getMessage() );
            throw new DokanOperationException(WinError.ERROR_ACCESS_DENIED);
        }
        catch (IOException | SQLException | PathResolveException  iOException)
        {
            log.error( "Exception in onCreateDirectory: " + iOException.getMessage() );
            throw new DokanOperationException(WinError.ERROR_ACCESS_DENIED);
        }
    }


    @Override
    public void onCloseFile( String winPath, DokanFileInfo arg1 )
            throws DokanOperationException
    {
        log("[onClose] " + winPath + " " +  arg1.toString());

        // Kein Gültiger Handle -> Open is failed
        if (arg1.handle == 0)
            return;
        
        OpenVfsFsEntry f = resolveNodeOpenedFile(winPath, arg1);
        vfsHandler.closeEntry( f );
        
        if (f.isDeleteOnClose())
        {
            log("[isDeleteOnClose] " + winPath );
            try
            {
                if (!vfsHandler.unlink( f.getEntry() ))
                {
                    log.debug("unable to delete file/folder " + winPath);
                    throw new DokanOperationException(WinError.ERROR_WRITE_FAULT);
                }
            }
            catch (PoolReadOnlyException poolReadOnlyException)
            {
                log.warn("unable to delete file/folder, read only fs " + winPath);
                throw new DokanOperationException(ERROR_SHARING_VIOLATION);
            }                
            catch (Exception exc)
            {
                log.warn("unable to delete file/folder: " + winPath + ": " + exc.toString());
                throw new DokanOperationException(ERROR_WRITE_FAULT);
            }                
        }             
    }

    @Override
    public int onReadFile( String fileName, ByteBuffer buf, long offset, DokanFileInfo arg3 ) throws DokanOperationException
    {
        log("[onReadFile] " + buf.capacity() + " at " + offset + " from " + fileName +  " " + arg3.toString());        
        
        OpenVfsFsEntry entry = resolveNodeOpenedFile(fileName, arg3);
        try
        {            
            // Catch empty read
            if (offset == entry.getEntry().getSize())
            {
                 log("[onReadFile] read " + 0);
                 return 0;
            }
             
            // Catch wrong read pos
            if (offset > entry.getEntry().getSize())
            {
                log("read offset is too large: " + offset + " filelen:" + entry.getEntry().getSize());            
                return -1;
            }


            byte[] b = entry.read( offset, buf.capacity() );
            if (b == null)
            {                
                log.error("unable to read " + buf.capacity() + " byte at pos " + offset + " from " + fileName);
                throw new DokanOperationException(ERROR_READ_FAULT);                
            }

            log("[onReadFile] read " + b.length);
            buf.put(b, 0, b.length);            
            return b.length;
        }
        catch (IOException | SQLException | DokanOperationException | PoolReadOnlyException |PathResolveException  e)
        {
            log.error("unable to read file " + fileName, e);
            throw new DokanOperationException(ERROR_READ_FAULT);
        }        
    }

    @Override
    public int onWriteFile( String fileName, ByteBuffer buf, long offset, DokanFileInfo arg3 ) throws DokanOperationException
    {
        log("[onWriteFile] " + buf.capacity() + " at " + offset + " from " + fileName +  " " + arg3.toString());
        
        byte[] b = new byte[buf.capacity()];
        buf.get(b);
        OpenVfsFsEntry entry = resolveNodeOpenedFile(fileName, arg3);

        try
        {            
            entry.write(offset, b.length, b);            
            log("wrote " + b.length + " at " + offset);            
        }
        catch (IOException | SQLException | PoolReadOnlyException | PathResolveException e)
        {
            log.error("unable to write to file" + fileName, e);
            throw new DokanOperationException(ERROR_WRITE_FAULT);
        }
        return b.length;
    }

    @Override
    public void onSetEndOfFile( String fileName, long length, DokanFileInfo arg2 )
            throws DokanOperationException
    {
        log("[onSetEndOfFile] len " + length + " " + fileName  + " " + arg2.toString());
       
        OpenVfsFsEntry entry = resolveNodeOpenedFile(fileName, arg2);
        try
        {
            entry.truncate(length);            
        }
        catch (IOException | SQLException | PoolReadOnlyException |PathResolveException e)
        {
            log.error("Unable to set length  of " + fileName + " to " + length, e);
            throw new DokanOperationException(ERROR_WRITE_FAULT);
        }       
    }
        
    @Override
    public void onFlushFileBuffers( String fileName, DokanFileInfo arg1 )
            throws DokanOperationException
    {                
        OpenVfsFsEntry entry = resolveNodeOpenedFile(fileName, arg1);
        try
        {
            entry.flush();            
        }
        catch (Exception e)
        {
            log.error("Unable to flush " + fileName, e );
            throw new DokanOperationException(ERROR_WRITE_FAULT);
        }
    }


    @Override
    public ByHandleFileInformation onGetFileInformation( String fileName, DokanFileInfo arg1 ) throws DokanOperationException
    {
        log.debug("[onGetFileInformation] " + fileName + " " + arg1.toString());
        ByHandleFileInformation bhfi;

        if (fileName.equals("\\"))
        {
            bhfi = new ByHandleFileInformation(FILE_ATTRIBUTE_NORMAL
                    | FILE_ATTRIBUTE_DIRECTORY, rootCreateTime, rootCreateTime,
                    rootLastWrite, volumeSerialNumber, 0, 1, 1 );
//                    remoteFSApi.getTotalBlocks() * remoteFSApi.getBlockSize(), 1, 1);
        }
        else
        {
            IVfsFsEntry entry = dirListCacheLookUp(fileName);
            if (entry == null)
            {
                OpenVfsFsEntry oentry = resolveNodeOpenedFile( fileName, arg1);
                entry = oentry.getEntry();
            }
          
            long atime = FileTime.toWinFileTime( entry.getAtimeMs() );
            long ctime = FileTime.toWinFileTime(entry.getCtimeMs() );
            long mtime = FileTime.toWinFileTime(entry.getMtimeMs() );
            long fileLength = entry.getSize();
            int fileAttribute = FILE_ATTRIBUTE_NORMAL;
            if (entry.isDirectory())
                fileAttribute |= FILE_ATTRIBUTE_DIRECTORY;

            bhfi = new ByHandleFileInformation(fileAttribute, ctime, atime, mtime,
                    volumeSerialNumber, fileLength, 1, entry.getGUID());
        }

        log.debug(bhfi.toString());

        return bhfi;
    }
    
    @Override
    public void onCleanup( String winPath, DokanFileInfo arg1 )
            throws DokanOperationException
    {
        log("[onCleanup] " + winPath  + " " + arg1.toString());
   
        // Kein Gültiger Handle -> Open is failed
        if (arg1.handle == 0)
            return;
        
        //OpenVfsFsEntry oentry = resolveNodeOpenedFile(winPath, arg1);
        
        try
        {
//            vfsHandler.closeEntry( entry );
        }
        catch (Exception iOException)
        {
            log.warn("unable to cleanup file/folder: " + winPath + ": " + iOException.toString());
            throw new DokanOperationException( ERROR_WRITE_FAULT );
        }                    
    }
    
    

    class DirListCacheEntry
    {
        long ts;
        List<IVfsFsEntry> data;
        Win32FindData[] wdata;

        public DirListCacheEntry( List<IVfsFsEntry> data )
        {
            this.ts = System.currentTimeMillis();
            this.data = data;
        }
        
        boolean isExpired()
        {
            return ((System.currentTimeMillis() - ts) > 15000);
        }

        public void setWdata( Win32FindData[] wdata )
        {
            this.wdata = wdata;
        }

        public Win32FindData[] getWdata()
        {
            return wdata;
        }
        
    }
            
    Map<String,DirListCacheEntry> dirCache = new ConcurrentHashMap<>();
    
    void addDirListCache( String path,List<IVfsFsEntry> data)
    {
        for (Map.Entry<String, DirListCacheEntry> entry : dirCache.entrySet())
        {
            String string = entry.getKey();
            DirListCacheEntry dirListCacheEntry = entry.getValue();
        
            if (dirListCacheEntry.isExpired())
                dirCache.remove(string);
        }
        dirCache.put(path, new DirListCacheEntry(data));
    }
    void flushDirListCache()
    {
        dirCache.clear();
    }
    IVfsFsEntry dirListCacheLookUp( String filePath)
    {
        int idx = filePath.lastIndexOf('\\');
        // Root?
        if (idx == filePath.length())
            return null;
        
        String parentPath = filePath.substring(0, idx);
        
        String fileName = filePath.substring(idx + 1);
        
        DirListCacheEntry dlce = dirCache.get(parentPath);
        if (dlce != null)
        {
            for (int i = 0; i < dlce.data.size(); i++)
            {
                IVfsFsEntry entry = dlce.data.get(i);
                if (entry.getNode().getName().equals(fileName))
                    return entry;
                
            }            
        }
        return null;        
    }
    

    @Override
    public Win32FindData[] onFindFiles( String pathName, DokanFileInfo arg1 ) throws DokanOperationException
    {
        log("[onFindFiles] " + pathName  + " " + arg1.toString());                        
        IVfsFsEntry entry = resolveNodeDirMustExist( pathName);        
        try
        {
            Win32FindData[] cache_files;

            IVfsDir dir = (IVfsDir)entry;  
            List<IVfsFsEntry> list;
            DirListCacheEntry dlce = dirCache.get(pathName);
            if (dlce != null && !dlce.isExpired())
            {
                return dlce.getWdata();
            }
            else
            {
                if (dir.isRealized())
                    dir.unrealize();
                list = dir.listChildren();
                addDirListCache(pathName, list);
                dlce = dirCache.get(pathName);
            }
            List<Win32FindData>cacheFileList = new ArrayList<>();

            for (int i = 0; i < list.size(); i++)
            {
                RemoteFSElem cf = list.get(i).getNode();

                final String lName = String.copyValueOf(WinFileUtilities.sys_name_to_win_name( cf.getName() ).toCharArray());
                final String sName = String.copyValueOf(Utils.toShortName(lName).toCharArray());

                int fileAttribute = FILE_ATTRIBUTE_NORMAL;
                if (cf.isDirectory())
                    fileAttribute |= FILE_ATTRIBUTE_DIRECTORY;

                Win32FindData d = new Win32FindData(fileAttribute,
                        FileTime.toWinFileTime( cf.getCtimeMs()),
                        FileTime.toWinFileTime( cf.getAtimeMs()),
                        FileTime.toWinFileTime( cf.getMtimeMs()),
                        cf.getDataSize(), 0, 0, lName, sName);

                cacheFileList.add(d);

                //log("[onFindFiles] " + d.toString());
            }
            // Add . and ..
            int fileAttribute = FILE_ATTRIBUTE_NORMAL | FILE_ATTRIBUTE_DIRECTORY;
            RemoteFSElem cf =  entry.getNode();
            Win32FindData d = new Win32FindData(fileAttribute,
                        FileTime.toWinFileTime( cf.getCtimeMs()),
                        FileTime.toWinFileTime( cf.getAtimeMs()),
                        FileTime.toWinFileTime( cf.getMtimeMs()),
                        cf.getDataSize(), 0, 0, ".", ".");

            cacheFileList.add(d);

            // TODO: THIS SHOULD BE PARENT!!!
            if (!entry.getPath().equals( "/"))
            {
                d = new Win32FindData(fileAttribute,
                FileTime.toWinFileTime( cf.getCtimeMs()),
                FileTime.toWinFileTime( cf.getAtimeMs()),
                FileTime.toWinFileTime( cf.getMtimeMs()),
                cf.getDataSize(), 0, 0, "..", "..");
                cacheFileList.add(d);
            }
            cache_files = cacheFileList.toArray(new Win32FindData[0]);
            dlce.setWdata(cache_files);
            
            return cache_files;
        }
        catch (IOException | SQLException e)
        {
            log.error("unable to list files for " + pathName, e);
            throw new DokanOperationException(WinError.ERROR_DIRECTORY);
        }
    }

    @Override
    public Win32FindData[] onFindFilesWithPattern( String arg0, String arg1, DokanFileInfo arg2 ) throws DokanOperationException
    {
        log("[onFindFilesWithPattern] " + arg0  + " " + arg1  + " " + arg2.toString());
        System.out.println("TODO: onFindFilesWithPattern");

        return null;
    }

    @Override
    public void onSetFileAttributes( String fileName, int fileAttributes, DokanFileInfo arg2 ) throws DokanOperationException
    {
         log("[onSetFileAttributes] " + fileName + " " + arg2.toString() + " attr: " + fileAttributes);
         System.out.println("TODO: onSetFileAttributes");
		/*
         * MemFileInfo fi = fileInfoMap.get(fileName); if (fi == null) throw new
         * DokanOperationException(ERROR_FILE_NOT_FOUND); fi.fileAttribute =
         * fileAttributes;
         */
    }

    @Override
    public void onSetFileTime( String fileName, long ctime, long atime, long mtime, DokanFileInfo arg4 ) throws DokanOperationException
    {
        log("[onSetFileTime] " + fileName + " " + arg4.toString() + " C: " + ctime + " A: " + atime + " M: " + mtime);
        if (ctime == 0 && atime == 0 && mtime == 0)
        {
            log("[onSetFileTime] skipped");
            return;
        }
        OpenVfsFsEntry entry =  resolveNodeOpenedFile( fileName, arg4);
        
        try
        {            
            entry.setMsTimes(FileTime.toJavaTime(ctime), FileTime.toJavaTime(atime), FileTime.toJavaTime(mtime));
        }
        catch (IOException | SQLException | PoolReadOnlyException | PathResolveException exception)
        {
            log.warn("unable to onSetFileTime file " + fileName);
            throw new DokanOperationException(ERROR_FILE_NOT_FOUND);
        }
    }

    @Override
    public void onDeleteFile( String fileName, DokanFileInfo arg1 ) throws DokanOperationException
    {
        log("[onDeleteFile] " + fileName);
        OpenVfsFsEntry oentry = resolveNodeOpenedFile(fileName, arg1);
         
//        IVfsFsEntry entry =  resolveNodeFileMustExist( fileName );
        // You should not delete file on DeleteFile or DeleteDirectory.
	// When DeleteFile or DeleteDirectory, you must check whether
	// you can delete the file or not, and return 0 (when you can delete it)
	// or appropriate error codes such as -ERROR_DIR_NOT_EMPTY,
	// -ERROR_SHARING_VIOLATION.
	// When you return 0 (ERROR_SUCCESS), you get Cleanup with
	// FileInfo->DeleteOnClose set TRUE and you have to delete the
	// file in Close.           
        try
        {
            if (oentry.isReadOnly())
            {
                log("[onDeleteFile] " + fileName + " failed with read only");
                throw new DokanOperationException(ERROR_SHARING_VIOLATION);                
            }
        }
        catch (IOException | SQLException | PoolReadOnlyException | PathResolveException iOException)
        {
            log("[onDeleteFile] " + fileName + " failed with " + iOException.getMessage());
            throw new DokanOperationException(ERROR_ACCESS_DENIED);
        }
        
        log("[onDeleteFile] " + fileName + " succeeded");
        oentry.setDeleteOnClose( true );
    }

    @Override
    public void onDeleteDirectory( String path, DokanFileInfo arg1 ) throws DokanOperationException
    {
        log("[onDeleteDirectory] " + path);
        // You should not delete file on DeleteFile or DeleteDirectory.
	// When DeleteFile or DeleteDirectory, you must check whether
	// you can delete the file or not, and return 0 (when you can delete it)
	// or appropriate error codes such as -ERROR_DIR_NOT_EMPTY,
	// -ERROR_SHARING_VIOLATION.
	// When you return 0 (ERROR_SUCCESS), you get Cleanup with
	// FileInfo->DeleteOnClose set TRUE and you have to delete the
	// file in Close.      
        
        OpenVfsFsEntry oentry = resolveNodeOpenedFile(path, arg1);

        try
        {
            if (oentry.isReadOnly())
            {
                log("[onDeleteDirectory] " + path + " failed with read only");
                throw new DokanOperationException(ERROR_SHARING_VIOLATION);                
            }

            List<RemoteFSElem> list = remoteFSApi.get_child_nodes(oentry.getEntry().getNode());
            if (!list.isEmpty())
            {
                log("[onDeleteDirectory] " + path + " failed with not empty");
                throw new DokanOperationException(WinError.ERROR_DIR_NOT_EMPTY);
            }
        }
        catch (SQLException | IOException | PoolReadOnlyException | PathResolveException sQLException)
        {
            log("[onDeleteDirectory] " + path + " failed with " + sQLException.getMessage());
            throw new DokanOperationException(ERROR_ACCESS_DENIED);
        }
        log("[onDeleteDirectory] " + path + " succeeded");
        oentry.setDeleteOnClose( true );        
    }

    @Override
    public void onMoveFile( String winFrom, String winTo, boolean replaceExisiting, DokanFileInfo arg3 ) throws DokanOperationException
    {
        flushDirListCache();
        log("==> [onMoveFile] " + winFrom + " -> " + winTo + ", replaceExisiting = " + replaceExisiting);

        try
        {
           String from = WinFileUtilities.win_to_sys_path(winFrom);
           String to = WinFileUtilities.win_to_sys_path(winTo);
           vfsHandler.moveNode( from, to );
        }
        catch (IOException | SQLException | PoolReadOnlyException | PathResolveException e)
        {
            log.error("unable to move file " + winFrom + " to " + winTo, e);
            throw new DokanOperationException(ERROR_FILE_EXISTS);
        }
        log("<== [onMoveFile]");
    }

    @Override
    public void onLockFile( String fileName, long arg1, long arg2, DokanFileInfo arg3 ) throws DokanOperationException
    {
        log("[onLockFile] " + fileName);
    }

    @Override
    public void onUnlockFile( String fileName, long arg1, long arg2, DokanFileInfo arg3 ) throws DokanOperationException
    {
        log("[onUnlockFile] " + fileName);
    }

    @Override
    public DokanDiskFreeSpace onGetDiskFreeSpace( DokanFileInfo arg0 ) throws DokanOperationException
    {
        log("[onGetDiskFreeSpace]" + " " + arg0.toString());
        DokanDiskFreeSpace free = new DokanDiskFreeSpace();
        free.freeBytesAvailable = (remoteFSApi.getTotalBlocks() - remoteFSApi.getUsedBlocks()) * remoteFSApi.getBlockSize();
        free.totalNumberOfBytes = remoteFSApi.getTotalBlocks() * remoteFSApi.getBlockSize();
        free.totalNumberOfFreeBytes = free.freeBytesAvailable;
        return free;
    }

    @Override
    public DokanVolumeInformation onGetVolumeInformation( String arg0, DokanFileInfo arg1 ) throws DokanOperationException
    {
        log("[onGetVolumeInformation] " + arg0 + " " + arg1.toString());
        DokanVolumeInformation info = new DokanVolumeInformation();
        info.fileSystemFlags = SUPPORTED_FLAGS;
        info.volumeName = "VSM Storage";
        info.fileSystemName = "VSMFS";
        info.volumeSerialNumber = volumeSerialNumber;
        info.maximumComponentLength = 1024;
        return info;
    }

    @Override
    public void onUnmount( DokanFileInfo arg0 ) throws DokanOperationException
    {
        log("[onUnmount]");
        Dokan.removeMountPoint(drive);

        vfsHandler.closeAll();
    }
    
    
    
    private IVfsFsEntry resolveNode( String winpath ) throws DokanOperationException
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
    private OpenVfsFsEntry resolveNodeOpenedFile( String winpath, DokanFileInfo arg1) throws DokanOperationException
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
            throw new DokanOperationException(ERROR_FILE_NOT_FOUND);
        }      
    }  
    private IVfsFsEntry resolveNodeDirMustExist( String winpath ) throws DokanOperationException
    {
        IVfsFsEntry entry = resolveNode( winpath );
        if (entry == null)
        {
            log("[resolveNodeDir] " + winpath + " failed with not found");
            throw new DokanOperationException(ERROR_PATH_NOT_FOUND);
        }
        return entry;
    }  
    /*
    private IVfsFsEntry resolveNodeByHandleMustExist( long handleNo) throws DokanOperationException
    {
        IVfsFsEntry entry = vfsHandler.getEntryByHandle( handleNo);
        if (entry == null)
        {
            log("[resolveNodeByHandle] " + handleNo + " failed with not found");
            throw new DokanOperationException(WinError.ERROR_FILE_NOT_FOUND);
        }
        return entry;
    }*/

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

    /*
    @Override
    public DokanFileSecurity onGetFileSecurity( String filename, DokanFileInfo fileInfo ) throws DokanOperationException
    {
    log.debug("onGetFileSecurity: Not supported yet.");
    return null;
    }
    @Override
    public void onSetFileSecurity( String filename, DokanFileSecurity secInfo, DokanFileInfo fileInfo ) throws DokanOperationException
    {
    log.debug("onSetFileSecurity: Not supported yet.");
    }
     * */
        
}
