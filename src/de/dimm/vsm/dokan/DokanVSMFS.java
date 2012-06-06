/*
The MIT License

Copyright (C) 2008 Yu Kobayashi http://yukoba.accelart.jp/

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
 */
package de.dimm.vsm.dokan;

import de.dimm.vsm.Exceptions.PoolReadOnlyException;
import de.dimm.vsm.net.interfaces.FileHandle;
import de.dimm.vsm.Utilities.WinFileUtilities;
import de.dimm.vsm.VSMFSLogger;
import de.dimm.vsm.fsengine.FileHandleEntry;
import de.dimm.vsm.fsengine.HandleResolver;
import de.dimm.vsm.fsutils.FSENode;
import de.dimm.vsm.fsutils.RemoteStoragePoolHandler;
import de.dimm.vsm.fsutils.ShutdownHook;
import de.dimm.vsm.fsutils.Utils;
import de.dimm.vsm.fsutils.VSMFS;
import de.dimm.vsm.net.RemoteFSElem;
import de.dimm.vsm.records.FileSystemElemNode;
import java.sql.SQLException;
import net.decasdev.dokan.DokanOptions;
;
import static net.decasdev.dokan.CreationDisposition.CREATE_ALWAYS;
import static net.decasdev.dokan.CreationDisposition.CREATE_NEW;
import static net.decasdev.dokan.CreationDisposition.OPEN_ALWAYS;
import static net.decasdev.dokan.CreationDisposition.OPEN_EXISTING;
import static net.decasdev.dokan.CreationDisposition.TRUNCATE_EXISTING;
import static net.decasdev.dokan.FileAttribute.FILE_ATTRIBUTE_DIRECTORY;
import static net.decasdev.dokan.FileAttribute.FILE_ATTRIBUTE_NORMAL;
import static net.decasdev.dokan.WinError.ERROR_ALREADY_EXISTS;
import static net.decasdev.dokan.WinError.ERROR_FILE_EXISTS;
import static net.decasdev.dokan.WinError.ERROR_FILE_NOT_FOUND;
import static net.decasdev.dokan.WinError.ERROR_PATH_NOT_FOUND;
import static net.decasdev.dokan.WinError.ERROR_READ_FAULT;
import static net.decasdev.dokan.WinError.ERROR_WRITE_FAULT;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

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




public class DokanVSMFS implements DokanOperations, VSMFS
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
    String drive ="S:\\";
    RemoteStoragePoolHandler remoteFSApi;

    HandleResolver handleResolver;


    private Logger log = VSMFSLogger.getLog();

    static void logs( String msg )
    {
        System.out.println("WinVSMFS: " + msg);
    }

    public DokanVSMFS( RemoteStoragePoolHandler api, String drive, Logger log )
    {
        this.remoteFSApi = api;
        this.drive = drive;
        this.log = log;

        showVersions();

        hook = new ShutdownHook( drive, false );
        Runtime.getRuntime().addShutdownHook(hook);

        handleResolver = new HandleResolver(log);


    }

    final void showVersions()
    {
        int version = Dokan.getVersion();
        System.out.println("Dokan V" + version);
        int driverVersion = Dokan.getDriverVersion();
        System.out.println("DokanDriver V" + driverVersion);
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
        unmount();

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

        return result == 0 ? true : false;
        
    }

    public static boolean unmount( String drive )
    {
        return Dokan.removeMountPoint(drive);
    }
    
    @Override
    public boolean unmount()
    {
        remoteFSApi.disconnect();

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
        
        FSENode mf = resolveNode(fileName);


        if (mf != null)
        {
            switch (creationDisposition)
            {
                case CREATE_NEW:
                    throw new DokanOperationException(ERROR_ALREADY_EXISTS);
                case OPEN_ALWAYS:

                case OPEN_EXISTING:
                {
                    if (mf.isDirectory())
                    {
                        arg5.isDirectory = true;
                        long handle = open_DirHandle(fileName, mf);
                        if (handle == 0)
                            throw new DokanOperationException(ERROR_FILE_NOT_FOUND);
                        return handle;
                    }

                    long handle = open_FileHandle(mf,  creationDisposition == OPEN_ALWAYS ? true: false);
                    if (handle == 0)
                        throw new DokanOperationException(WinError.ERROR_ACCESS_DENIED);
                    return handle;
                }
                case CREATE_ALWAYS:
                case TRUNCATE_EXISTING:
                {
                    if (mf.isDirectory())
                    {
                        arg5.isDirectory = true;
                        long handle = open_DirHandle(fileName, mf);
                        return handle;
                    }
                    try
                    {
                        //long fileHandle = getNextHandle();
                        long handle = open_FileHandle(mf,  true);
                        FileHandle fh = handleResolver.get_handle_by_handleNo(handle);
                        fh.truncateFile(0);

                        //closeFileChannel(mf.getNode().getIdx());
                        return handle;
                    }

                    catch (Exception e)
                    {
                        log.error("unable to clear file "  + mf.get_path(), e);
                        throw new DokanOperationException( WinError.ERROR_ACCESS_DENIED);
                    }
                }
            }
        }
        else
        {

            switch (creationDisposition)
            {

                case CREATE_NEW:
                case CREATE_ALWAYS:
                case OPEN_ALWAYS:
                    try
                    {
                        log.debug("creating " + path);
                        RemoteFSElem node = remoteFSApi.create_fse_node(path, FileSystemElemNode.FT_FILE);
                        if (node == null)
                            throw new DokanOperationException(WinError.ERROR_ACCESS_DENIED);

                        mf = new FSENode(node, remoteFSApi );
                        long handle = open_FileHandle(mf,  true);
                        if (handle <= 0)
                            throw new DokanOperationException(WinError.ERROR_ACCESS_DENIED);
                        return handle;
                    }
                    catch (Exception e)
                    {
                        log.error("unable to create file " + path, e);
                        throw new DokanOperationException(WinError.ERROR_ACCESS_DENIED);
                    }                    
                case OPEN_EXISTING:
                case TRUNCATE_EXISTING:
                    throw new DokanOperationException(ERROR_FILE_NOT_FOUND);
            }
        }
        throw new DokanOperationException(1);
    }

    @Override
    public long onOpenDirectory( String winpathName, DokanFileInfo arg1 )
            throws DokanOperationException
    {
        log("[onOpenDirectory] " + winpathName + " " + arg1.toString());
        
        FSENode f = resolveNode(winpathName);

        if (f != null)
        {
            long handle = open_DirHandle(winpathName, f);            
            return handle; //getNextHandle();
        }
        else
        {
            throw new DokanOperationException(ERROR_PATH_NOT_FOUND);
        }
    }

    @Override
    public void onCreateDirectory( String pathName, DokanFileInfo file )
            throws DokanOperationException
    {
        log("[onCreateDirectory] " + pathName + " " + file.toString());        

        FSENode f = resolveNode(pathName);
        if (f != null)
        {
            f = null;
            throw new DokanOperationException(WinError.ERROR_ALREADY_EXISTS);
        }
        try
        {
            pathName = WinFileUtilities.win_to_sys_path(pathName);
            remoteFSApi.mkdir(pathName);
        }
        catch (Exception iOException)
        {
            log.error( "Exception in onCreateDirectory: " + iOException.getMessage() );
            throw new DokanOperationException(WinError.ERROR_PATH_NOT_FOUND);
        }
    }

    @Override
    public void onCleanup( String arg0, DokanFileInfo arg2 )
            throws DokanOperationException
    {
    }

    @Override
    public void onCloseFile( String path, DokanFileInfo arg1 )
            throws DokanOperationException
    {
        log("[onClose] " + path + " " +  arg1.toString());

        try
        {
            handleResolver.close_Handle(arg1.handle);
        }
        catch (IOException iOException)
        {
            throw new DokanOperationException( ERROR_WRITE_FAULT );
        }
        
    }

    @Override
    public int onReadFile( String fileName, ByteBuffer buf, long offset, DokanFileInfo arg3 ) throws DokanOperationException
    {
        log("[onReadFile] " + fileName +  " " + arg3.toString());
        
        
        try
        {

            FileHandleEntry entry = handleResolver.get_FileHandleEntry(arg3.handle);
            if (entry == null)
                throw new IOException("No file handle found" );

            FileHandle ch = entry.getFh();

            byte[] b = ch.read( buf.capacity(), offset);
            if (b == null)
            {                
                // SHOULD
                if (entry.getNode().get_size() > offset)
                {
                    log.error("unable to read " + buf.capacity() + " byte at pos " + offset + " from " + fileName);
                    throw new DokanOperationException(ERROR_READ_FAULT);
                }
                log.warn("DokanNodeDevice.onReadFile: Reading after EOF.");
                //throw new DokanOperationException(WinError.ERROR_HANDLE_EOF);

                return 0;
            }

            buf.put(b, 0, b.length);

            log("read  " + b.length + " at " + offset);

            return b.length;
        }
        catch (Exception e)
        {
            log.error("unable to read file " + fileName, e);
            throw new DokanOperationException(ERROR_READ_FAULT);
        }
    }

    @Override
    public int onWriteFile( String fileName, ByteBuffer buf, long offset, DokanFileInfo arg3 ) throws DokanOperationException
    {
        log("[onWriteFile] " + fileName  + " " + arg3.toString());
        

        byte[] b = new byte[buf.capacity()];
        buf.get(b);

        try
        {
            FileHandle ch = handleResolver.get_FileHandleEntry(arg3.handle).getFh();
            ch.writeFile(b, b.length, offset);
            log("wrote " + b.length + " at " + offset);
        }
        catch (Exception e)
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
        log("[onSetEndOfFile] " + fileName  + " " + arg2.toString());
       

        try
        {
            FileHandle ch = handleResolver.get_FileHandleEntry( arg2.handle).getFh();
            ch.truncateFile(length);
        }
        catch (Exception e)
        {
            log.error("Unable to set length  of " + fileName + " to " + length);
            throw new DokanOperationException(ERROR_WRITE_FAULT);
        }
    }

    @Override
    public void onFlushFileBuffers( String fileName, DokanFileInfo arg1 )
            throws DokanOperationException
    {
        
        try
        {
            FileHandle ch = handleResolver.get_FileHandleEntry( arg1.handle).getFh();
            ch.force(true);
        }
        catch (IOException e)
        {
            log.error("unable to sync file " + fileName, e);
            throw new DokanOperationException(ERROR_WRITE_FAULT);
        }

    }


    //static long finfo_idx = 2;
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
            FSENode mf = handleResolver.get_fsenode_by_handleNo( arg1.handle );
           
            if (mf == null)
            {
                throw new DokanOperationException(ERROR_FILE_NOT_FOUND);
            }

            int mode = mf.get_mode();
            long atime = FileTime.toWinFileTime( mf.get_last_accessed() );
            long ctime = FileTime.toWinFileTime(mf.get_creation_date() );
            long mtime = FileTime.toWinFileTime(mf.get_last_modified() );
            long fileLength = mf.get_size();
            int fileAttribute = FILE_ATTRIBUTE_NORMAL;
            if (mf.isDirectory())
                fileAttribute |= FILE_ATTRIBUTE_DIRECTORY;

            bhfi = new ByHandleFileInformation(fileAttribute, ctime, atime, mtime,
                    volumeSerialNumber, fileLength, 1, mf.get_GUID());
        }

        log.debug(bhfi.toString());

        return bhfi;
    }

    HashMap<String,Win32FindData[]> dir_cache = new HashMap<String, Win32FindData[]>();
    

    @Override
    public Win32FindData[] onFindFiles( String pathName, DokanFileInfo arg1 ) throws DokanOperationException
    {
        log("[onFindFiles] " + pathName  + " " + arg1.toString());
        FSENode f = null;


        Win32FindData[] cache_files = null; //dir_cache.get(pathName);

        if (cache_files != null)
            return cache_files;


        try
        {
            ArrayList<Win32FindData> files = new ArrayList<Win32FindData>();
            
            f =  handleResolver.get_fsenode_by_handleNo( arg1.handle );

            //f = resolveNode(pathName);

            if (f != null)
            {
                List<RemoteFSElem> list = remoteFSApi.get_child_nodes( f.getNode() );

                for (int i = 0; i < list.size(); i++)
                {
                    RemoteFSElem cf = list.get(i);

                    String lName = WinFileUtilities.sys_name_to_win_name( cf.getName() );
                    String sName = Utils.toShortName(lName);
                    
                    int fileAttribute = FILE_ATTRIBUTE_NORMAL;
                    if (cf.isDirectory())
                        fileAttribute |= FILE_ATTRIBUTE_DIRECTORY;

                    Win32FindData d = new Win32FindData(fileAttribute,
                            FileTime.toWinFileTime( cf.getCtimeMs()),
                            FileTime.toWinFileTime( cf.getAtimeMs()),
                            FileTime.toWinFileTime( cf.getMtimeMs()),
                            cf.getDataSize(), 0, 0, lName, sName);

                    files.add(d);
                }
            }
            //log("[onFindFiles] " + files);

            cache_files = files.toArray(new Win32FindData[0]);
            //dir_cache.put(pathName, cache_files);
            return cache_files;
        }
        catch (Exception e)
        {
            log.error("unable to list files for " + pathName, e);
            throw new DokanOperationException(WinError.ERROR_DIRECTORY);
        }
        finally
        {
            f = null;
        }
    }

    @Override
    public Win32FindData[] onFindFilesWithPattern( String arg0, String arg1, DokanFileInfo arg2 ) throws DokanOperationException
    {
        return null;
    }

    @Override
    public void onSetFileAttributes( String fileName, int fileAttributes, DokanFileInfo arg2 ) throws DokanOperationException
    {
         log("[onSetFileAttributes] " + fileName + " " + arg2.toString());
		/*
         * MemFileInfo fi = fileInfoMap.get(fileName); if (fi == null) throw new
         * DokanOperationException(ERROR_FILE_NOT_FOUND); fi.fileAttribute =
         * fileAttributes;
         */
    }

    @Override
    public void onSetFileTime( String fileName, long creationTime, long atime, long mtime, DokanFileInfo arg4 ) throws DokanOperationException
    {
        log("[onSetFileTime] " + fileName + " A: " + atime + " M: " + mtime);
        FSENode mf = this.resolveNode(fileName);

        try
        {
            System.out.println("TODO: Setfiletimes");
           // mf.set_ms_times(FileTime.toJavaTime(creationTime), FileTime.toJavaTime(atime), FileTime.toJavaTime(mtime));
        }
        catch (Exception exception)
        {
            log.warn("unable to onSetFileTime file " + fileName);
            throw new DokanOperationException(ERROR_FILE_NOT_FOUND);
        }

    }

    @Override
    public void onDeleteFile( String fileName, DokanFileInfo arg1 ) throws DokanOperationException
    {
        log("[onDeleteFile] " + fileName);
// You should not delete file on DeleteFile or DeleteDirectory.
	// When DeleteFile or DeleteDirectory, you must check whether
	// you can delete the file or not, and return 0 (when you can delete it)
	// or appropriate error codes such as -ERROR_DIR_NOT_EMPTY,
	// -ERROR_SHARING_VIOLATION.
	// When you return 0 (ERROR_SUCCESS), you get Cleanup with
	// FileInfo->DeleteOnClose set TRUE and you have to delete the
	// file in Close.

        String name = WinFileUtilities.win_to_sys_path(fileName);
        try
        {
            if (!remoteFSApi.remove_fse_node(name))
            {
                log.warn("unable to delete file " + fileName);
                throw new DokanOperationException(ERROR_FILE_NOT_FOUND);
            }
        }
        catch (Exception poolReadOnlyException)
        {
            log.warn("unable to delete file, read only fs " + fileName);
            throw new DokanOperationException(ERROR_WRITE_FAULT);
        }

        //
    }

    @Override
    public void onDeleteDirectory( String path, DokanFileInfo arg1 ) throws DokanOperationException
    {
        log("[onDeleteDirectory] " + path);
        

        String name = WinFileUtilities.win_to_sys_path(path);
        try
        {
            if (!remoteFSApi.remove_fse_node(name))
            {
                log.debug("unable to delete folder " + path);
                throw new DokanOperationException(WinError.ERROR_DIR_NOT_EMPTY);
            }
        }
        catch (Exception poolReadOnlyException)
        {
            log.warn("unable to delete file, read only fs " + path);
            throw new DokanOperationException(ERROR_WRITE_FAULT);
        }
    }

    @Override
    public void onMoveFile( String from, String to, boolean replaceExisiting, DokanFileInfo arg3 ) throws DokanOperationException
    {
        log("==> [onMoveFile] " + from + " -> " + to + ", replaceExisiting = " + replaceExisiting);

        try
        {
            to = WinFileUtilities.win_to_sys_path(to);
            from = WinFileUtilities.win_to_sys_path( from );

           this.remoteFSApi.move_fse_node(  from, to );
        }
        catch (Exception e)
        {
            log.error("unable to move file " + from + " to " + to, e);
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

        handleResolver.closeAll();
    }


    
    private long open_FileHandle( FSENode mf,  boolean forWrite  ) throws DokanOperationException
    {               
        try
        {
            FileHandle handle = mf.open_file_handle( /*create*/forWrite);

            // WE USE THE FILEHANDLE FROM SERVER
            long handleNo = handleResolver.put_FileHandle(mf, handle);

            log.debug("->open_FileHandle " + mf.get_name() + ": " + handleNo);
            return handleNo;
        }
        catch (Exception e)
        {
            log.error("Exception in open_FileHandle: " + mf.get_path(), e);

            if (forWrite)
                throw new DokanOperationException(WinError.ERROR_ACCESS_DENIED);
            else
                throw new DokanOperationException(WinError.ERROR_FILE_NOT_FOUND);
        }
    }

    private long open_DirHandle( String win_path, FSENode mf ) throws DokanOperationException
    {
        try
        {
            long handleNo = handleResolver.put_DirHandle( mf );
            log.debug("->open_DirHandle " + win_path + ": " + handleNo);

            // ARTIFICIAL INDEX, NOT USED IN CLOSE AND ON... FUNCTIONS
            // DOKAN DOESNT DISTINGUISH BETWEEN DIRS AND FILES, WE DO
            return  handleNo;
        }
        catch (Exception e)
        {
            log.error("Exception in open_FileHandle: " + mf.get_path(), e);
        }
        throw new DokanOperationException(WinError.ERROR_GEN_FAILURE);
    }
    
    
    private FSENode resolveNode( String winpath ) throws DokanOperationException
    {
        String path = WinFileUtilities.win_to_sys_path(winpath);

        FSENode n = handleResolver.getNodeCache(winpath);
        if (n != null)
        {
            return n;
        }
        log.debug("Resolved path " + winpath + " to " + path);


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
        FSENode node = new FSENode(fse, remoteFSApi );

        handleResolver.addNodeCache(node, winpath);

        return node;
    }


    private void log( String string )
    {
         log.debug(string);
    }

    public void setShutdownHook( ShutdownHook hook )
    {
        this.hook = hook;
    }

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