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
import de.dimm.vsm.fsutils.IVSMFS;
import de.dimm.vsm.fsutils.VirtualFSENode;
import de.dimm.vsm.fsutils.VirtualFsFilemanager;
import de.dimm.vsm.fsutils.VirtualRemoteFileHandle;
import de.dimm.vsm.net.RemoteFSElem;
import de.dimm.vsm.records.FileSystemElemNode;
import de.dimm.vsm.vfs.IBufferedEventProcessor;
import de.dimm.vsm.vfs.IVfsEventProcessor;
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




public class DokanVSMFS implements DokanOperations, IVSMFS
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
    IBufferedEventProcessor processor;


    private Logger log = VSMFSLogger.getLog();

    static void logs( String msg )
    {
        System.out.println("WinVSMFS: " + msg);
    }
    private boolean useVirtualFS = true;
    

//    public static void test( )
//    {
//
//        DokanVSMFS d = new DokanVSMFS(null, null, null);
//
//        long start = System.currentTimeMillis();
//        int cnt = 0;
//        cnt = Dokan.testinit(d);
//        for (int i = 0; i < 100000; i++)
//        {
//            cnt = Dokan.teststring("blah");
//            if (i % 10000 == 0)
//            {
//                if (i == 0)
//                    continue;
//
//                long now = System.currentTimeMillis();
//                long diff = now - start;
//                if (diff == 0)
//                    diff = 1;
//
//                int n = (int)((10000 * 1000) / diff);
//                System.out.println( n +"/s");
//                start = now;
//            }
//        }
//    }

    public DokanVSMFS( RemoteStoragePoolHandler api,  IBufferedEventProcessor processor, String drive, Logger log )
    {
//        if (api == null)
//            test = true;
        this.remoteFSApi = api;
        this.drive = drive;
        this.log = log;
        this.processor = processor;

        showVersions();

        hook = new ShutdownHook( drive, false );
        handleResolver = new HandleResolver(log);
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
        log("[unmount] shutting down FS-Processor");
        processor.shutdown();
        log("[unmount] disconnecting");
        remoteFSApi.disconnect();

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
        
        FSENode mf = null;
        
        // Ask for existing node only it can exist
        if (creationDisposition != CREATE_NEW)
        {
            mf = resolveNode(fileName);
        }        

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
                        if (creationDisposition == TRUNCATE_EXISTING)
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
                        if (!useVirtualFS)
                        {
                            RemoteFSElem node = remoteFSApi.create_fse_node(path, FileSystemElemNode.FT_FILE);

                            if (node == null)
                                throw new DokanOperationException(WinError.ERROR_ACCESS_DENIED);

                            mf = resolveNode(fileName);
                            if (mf == null)
                                throw new DokanOperationException(WinError.ERROR_ACCESS_DENIED);

                            long handle = open_FileHandle(mf,  true);
                            if (handle <= 0)
                                throw new DokanOperationException(WinError.ERROR_ACCESS_DENIED);
                            return handle;
                        }
                        else
                        {
                            long now = System.currentTimeMillis();
                            RemoteFSElem node = new RemoteFSElem(path, FileSystemElemNode.FT_FILE, now, now, now, 0, 0);
                            node.setVirtualFS(true);
                            mf = new VirtualFSENode(node, this.remoteFSApi, processor);
                                                        
                            long handle = open_FileHandle(mf, true);
                            if (handle <= 0)
                                throw new DokanOperationException(WinError.ERROR_ACCESS_DENIED);
                            return handle;
                        }
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
    public void onCreateDirectory( String winPathName, DokanFileInfo file )
            throws DokanOperationException
    {
        log("[onCreateDirectory] " + winPathName + " " + file.toString());        

        FSENode f = resolveNode(winPathName);
        if (f != null)
        {
            f = null;
            throw new DokanOperationException(WinError.ERROR_ALREADY_EXISTS);
        }
        try
        {
            String pathName = WinFileUtilities.win_to_sys_path(winPathName);
            remoteFSApi.mkdir(pathName);            
            
        }
        catch (Exception iOException)
        {
            log.error( "Exception in onCreateDirectory: " + iOException.getMessage() );
            throw new DokanOperationException(WinError.ERROR_PATH_NOT_FOUND);
        }
    }


    @Override
    public void onCloseFile( String winPath, DokanFileInfo arg1 )
            throws DokanOperationException
    {
        log("[onClose] " + winPath + " " +  arg1.toString());

        FSENode f =  handleResolver.get_fsenode_by_handleNo( arg1.handle );
        if (f == null)
            throw new DokanOperationException(WinError.ERROR_FILE_NOT_FOUND);
        
        try
        {
            if (handleResolver.isforWrite(arg1.handle))
            {
                handleResolver.close_Handle(arg1.handle);                
                handleResolver.clearNodeCache(winPath);
            }
            else
            {
                handleResolver.close_Handle(arg1.handle);
            }
            
            if (f.isDeleteOnClose())
            {
                log("[isDeleteOnClose] " + winPath );
                try
                {
                    String name = WinFileUtilities.win_to_sys_path(winPath);
                    
                    if (!remoteFSApi.remove_fse_node(name))
                    {
                        log.debug("unable to delete file/folder " + winPath);
                        throw new DokanOperationException(WinError.ERROR_WRITE_FAULT);
                    }
                    handleResolver.clearNodeCache(winPath);
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
        catch (IOException iOException)
        {
            log.warn("unable to delete file/folder: " + winPath + ": " + iOException.toString());
            throw new DokanOperationException( ERROR_WRITE_FAULT );
        }        
        
    }

    @Override
    public int onReadFile( String fileName, ByteBuffer buf, long offset, DokanFileInfo arg3 ) throws DokanOperationException
    {
        log("[onReadFile] " + buf.capacity() + " at " + offset + " from " + fileName +  " " + arg3.toString());
        
        
        try
        {

            FileHandleEntry entry = handleResolver.get_FileHandleEntry(arg3.handle);
            if (entry == null)
                throw new IOException("No file handle found" );

            // Catch wrong read pos
            if (offset >= entry.getNode().get_size())
            {
                log("read offset is too large: " + offset + " filelen:" + entry.getNode().get_size());            
                return -1;
            }
            
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
        log("[onWriteFile] " + buf.capacity() + " at " + offset + " from " + fileName +  " " + arg3.toString());
        
        

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
        log("[onSetEndOfFile] len " + length + " " + fileName  + " " + arg2.toString());
       

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
            FileHandleEntry fhe = handleResolver.get_FileHandleEntry( arg1.handle);
            FileHandle ch = fhe.getFh();
            ch.force(true);
            handleResolver.clearNodeCache(fileName);
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
            FSENode mf = null;
            if (arg1.handle == 0)
            {
                mf = resolveNode(fileName);
            }
            else
            {
                mf = handleResolver.get_fsenode_by_handleNo( arg1.handle );
            }
           
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
    @Override
    public void onCleanup( String winPath, DokanFileInfo arg1 )
            throws DokanOperationException
    {
        log("[onCleanup] " + winPath  + " " + arg1.toString());
   
        FSENode f =  handleResolver.get_fsenode_by_handleNo( arg1.handle );
        if (f == null)
            throw new DokanOperationException(WinError.ERROR_FILE_NOT_FOUND);
        
        try
        {
            if (handleResolver.isforWrite(arg1.handle))
            {
                handleResolver.cleanup_Handle(arg1.handle);                
                handleResolver.clearNodeCache(winPath);
            }
            else
            {
                handleResolver.cleanup_Handle(arg1.handle);
            }
        }
        catch (IOException iOException)
        {
            log.warn("unable to cleanup file/folder: " + winPath + ": " + iOException.toString());
            throw new DokanOperationException( ERROR_WRITE_FAULT );
        }                    
    }

    //HashMap<String,Win32FindData[]> dir_cache = new HashMap<String, Win32FindData[]>();

//    List<Win32FindData> wfdBuffer = new ArrayList<Win32FindData>();
//
//    Win32FindData[] cache_files = null;

    @Override
    public Win32FindData[] onFindFiles( String pathName, DokanFileInfo arg1 ) throws DokanOperationException
    {
        log("[onFindFiles] " + pathName  + " " + arg1.toString());
        FSENode f = null;
        
        try
        {
            f =  handleResolver.get_fsenode_by_handleNo( arg1.handle );
            Win32FindData[] cache_files;

            if (f == null)
            {
                cache_files = new Win32FindData[0];
            }
            else
            {
                List<RemoteFSElem> list = remoteFSApi.get_child_nodes( f.getNode() );
                
                List<RemoteFSElem> vfsList = VirtualFsFilemanager.getSingleton().get_child_nodes(WinFileUtilities.win_to_sys_path(pathName));
                list.addAll(vfsList);

                List<Win32FindData>cacheFileList = new ArrayList<>();
                

                for (int i = 0; i < list.size(); i++)
                {
                    RemoteFSElem cf = list.get(i);

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
                    
                    log("[onFindFiles] " + d.toString());
                }
                // Add . and ..
                int fileAttribute = FILE_ATTRIBUTE_NORMAL | FILE_ATTRIBUTE_DIRECTORY;
                RemoteFSElem cf =  f.getNode();
                Win32FindData d = new Win32FindData(fileAttribute,
                            FileTime.toWinFileTime( cf.getCtimeMs()),
                            FileTime.toWinFileTime( cf.getAtimeMs()),
                            FileTime.toWinFileTime( cf.getMtimeMs()),
                            cf.getDataSize(), 0, 0, ".", ".");

                cacheFileList.add(d);
                
                // TODO: THIS SHOULD BE PARENT!!!
                d = new Win32FindData(fileAttribute,
                FileTime.toWinFileTime( cf.getCtimeMs()),
                FileTime.toWinFileTime( cf.getAtimeMs()),
                FileTime.toWinFileTime( cf.getMtimeMs()),
                cf.getDataSize(), 0, 0, ".", ".");

                cacheFileList.add(d);
                
                
                
                cache_files = cacheFileList.toArray(new Win32FindData[0]);
            }
            //log("[onFindFiles] " + files);

  //          cache_files = files.toArray(new Win32FindData[0]);
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
        FSENode mf =  handleResolver.get_fsenode_by_handleNo( arg4.handle );
        if (mf == null)
        {
            mf = this.resolveNode(fileName);
        }

        try
        {            
            mf.set_ms_times(FileTime.toJavaTime(ctime), FileTime.toJavaTime(atime), FileTime.toJavaTime(mtime));
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
        FSENode f =  handleResolver.get_fsenode_by_handleNo( arg1.handle );

        // You should not delete file on DeleteFile or DeleteDirectory.
	// When DeleteFile or DeleteDirectory, you must check whether
	// you can delete the file or not, and return 0 (when you can delete it)
	// or appropriate error codes such as -ERROR_DIR_NOT_EMPTY,
	// -ERROR_SHARING_VIOLATION.
	// When you return 0 (ERROR_SUCCESS), you get Cleanup with
	// FileInfo->DeleteOnClose set TRUE and you have to delete the
	// file in Close.           
        if (f == null)
        {
            log("[onDeleteFile] " + fileName + " failed with not found");
            throw new DokanOperationException(ERROR_FILE_NOT_FOUND);
        }
        try
        {
            if (remoteFSApi.isReadOnly( arg1.handle))
            {
                log("[onDeleteFile] " + fileName + " failed with read only");
                throw new DokanOperationException(ERROR_SHARING_VIOLATION);                
            }
        }
        catch (IOException iOException)
        {
            log("[onDeleteFile] " + fileName + " failed with " + iOException.getMessage());
            throw new DokanOperationException(ERROR_ACCESS_DENIED);
        }
        catch (SQLException iOException)
        {
            log("[onDeleteFile] " + fileName + " failed with " + iOException.getMessage());
            throw new DokanOperationException(ERROR_ACCESS_DENIED);
        }
        
        log("[onDeleteFile] " + fileName + " succeeded");
        f.setDeleteOnClose( true );
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
        
        FSENode f =  handleResolver.get_fsenode_by_handleNo( arg1.handle );

        if (f == null)
        {
            throw new DokanOperationException(ERROR_PATH_NOT_FOUND);
        }
        else
        {
            try
            {
                if (remoteFSApi.isReadOnly( f.getNode().getIdx()))
                {
                    log("[onDeleteDirectory] " + path + " failed with read only");
                    throw new DokanOperationException(ERROR_SHARING_VIOLATION);                
                }
               
                List<RemoteFSElem> list = remoteFSApi.get_child_nodes(f.getNode());
                if (!list.isEmpty())
                {
                    log("[onDeleteDirectory] " + path + " failed with not empty");
                    throw new DokanOperationException(WinError.ERROR_DIR_NOT_EMPTY);
                }
            }
            catch (SQLException sQLException)
            {
                log("[onDeleteDirectory] " + path + " failed with " + sQLException.getMessage());
                throw new DokanOperationException(ERROR_ACCESS_DENIED);
            }
            catch (IOException ex)
            {
                log("[onDeleteDirectory] " + path + " failed with " + ex.getMessage());
                throw new DokanOperationException(ERROR_ACCESS_DENIED);
            }
        }
        log("[onDeleteDirectory] " + path + " succeeded");
        f.setDeleteOnClose( true );        
    }

    @Override
    public void onMoveFile( String winFrom, String winTo, boolean replaceExisiting, DokanFileInfo arg3 ) throws DokanOperationException
    {
        log("==> [onMoveFile] " + winFrom + " -> " + winTo + ", replaceExisiting = " + replaceExisiting);

        try
        {
           String to = WinFileUtilities.win_to_sys_path(winTo);
           String from = WinFileUtilities.win_to_sys_path( winFrom );

           this.remoteFSApi.move_fse_node(  from, to );
           
           handleResolver.clearNodeCache(winFrom);
        }
        catch (Exception e)
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

        handleResolver.closeAll();
    }
    
    private long open_FileHandle( FSENode mf,  boolean forWrite  ) throws DokanOperationException
    {               
        try
        {
            long handleNo;
            FileHandle handle = mf.open_file_handle( /*create*/forWrite);
            if (handle instanceof VirtualRemoteFileHandle )
            {
                // WE USE INTERNAL FILEHANDLE
                VirtualRemoteFileHandle vHandle = (VirtualRemoteFileHandle)handle;
                handleNo = handleResolver.put_VirtualFileHandle(vHandle, mf);
            }
            else
            {
                // WE USE THE FILEHANDLE FROM SERVER
                handleNo = handleResolver.put_FileHandle(mf, handle, forWrite);
            }
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
        log.debug("Resolved path " + winpath + " to " + path);

        FSENode n = handleResolver.getNodeCache(winpath);
        if (n != null)
        {
            log.debug("Node in cache");
            return n;
        }


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

        log.debug("Node in DB");
        
        return node;
    }


    private void log( String string )
    {
        //logs( string);
         log.debug(string);
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
