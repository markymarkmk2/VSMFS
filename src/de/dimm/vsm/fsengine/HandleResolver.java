/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.fsengine;

import de.dimm.vsm.fsutils.FSENode;
import de.dimm.vsm.fsutils.VirtualFSFile;
import de.dimm.vsm.fsutils.VirtualRemoteFileHandle;
import de.dimm.vsm.net.interfaces.FileHandle;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.Logger;
import org.catacombae.jfuse.types.fuse26.FUSEFileInfo;

/**
 *
 * @author Administrator
 */
public class HandleResolver
{
    // We need a unique FileHandle for Files / Dirs / Virtual Files
    public static final long DIROFFSET = 4000000001L;  // 4000000001L open Files on Server allowed
    public static final long VIRTUALFSOFFSET = 8000000002L; // 4000000001L open Directories on Clientallowed
    

    final Map<Long,VirtualFileHandleEntry> virtual_file_handles = new ConcurrentHashMap<>();
    final Map<Long,FileHandleEntry> file_handles = new ConcurrentHashMap<>();
    final Map<Long,DirHandleEntry> dir_handles = new ConcurrentHashMap<>();
    


    ConcurrentHashMap<String,FSENode> nodeMap = new ConcurrentHashMap<>();    

    private Logger log;

    public HandleResolver( Logger log )
    {
        this.log = log;
    }


    public void closeAll()
    {
        Iterator<FileHandleEntry> iter = file_handles.values().iterator();
        while (iter.hasNext())
        {
            try
            {
                iter.next().close();
            }
            catch (IOException e)
            {
            }
        }
        Iterator<VirtualFileHandleEntry> viter = virtual_file_handles.values().iterator();
        while (viter.hasNext())
        {
            try
            {
                viter.next().close();
            }
            catch (IOException e)
            {
            }
        }
        Iterator<DirHandleEntry> diter = dir_handles.values().iterator();
        while (diter.hasNext())
        {
            diter.next().close();            
        }
    }

    public boolean isFileHandle( long handleNo )
    {
        return handleNo < DIROFFSET;
    }
    public boolean isDirHandle( long handleNo )
    {
        return (handleNo >= DIROFFSET && handleNo < VIRTUALFSOFFSET);
    }
    public boolean isVirtualFileHandle( long handleNo )
    {
        return (handleNo >= VIRTUALFSOFFSET);
    }
    
    public boolean isforWrite( long handleNo )
    {
        try
        {
            if (isFileHandle(handleNo))
            {
                FileHandleEntry fe = get_FileHandleEntry(handleNo);
            
                return fe.isForWrite();
            }
            if (isVirtualFileHandle(handleNo))
            {
                
            }
        }
        catch (Exception e)
        {
            log.error("Exception in isforWrite: ", e );
        }
        
        return false;
    }

    public VirtualFileHandleEntry get_VirtualFileHandleEntry( long handle ) throws IOException
    {
        return virtual_file_handles.get( handle );
    }

    public FileHandleEntry get_FileHandleEntry( long handle ) throws IOException
    {
        if (isVirtualFileHandle(handle))
            return virtual_file_handles.get(handle);
        
        return file_handles.get( handle );
    }
    public FileHandleEntry remove_FileHandleEntry( long handle ) throws IOException
    {
        if (isVirtualFileHandle(handle))
        {
            FileHandleEntry ret = virtual_file_handles.remove(handle);
            log.debug("number of vfs file handles is " + virtual_file_handles.size());
            return ret;
        }
        
        FileHandleEntry ret = file_handles.remove( handle );
        log.debug("number of file handles is " + file_handles.size());
        return ret;
    }

    public DirHandleEntry get_DirHandleEntry( long handle ) throws IOException
    {
        return dir_handles.get( handle );
    }
    public DirHandleEntry remove_DirHandleEntry( long handle ) throws IOException
    {
        return dir_handles.remove( handle );
    }

    public long put_FileHandle( FSENode mf, FileHandle handle, boolean forWrite ) throws IOException
    {
        try
        {
            long handleNo = mf.getNode().getFileHandle();
            FileHandleEntry entry = new FileHandleEntry(handle, mf, forWrite);
            file_handles.put(handleNo, entry);

            return handleNo;
        }
        catch (Exception e)
        {
            log.error("Exception in put_FileHandle: ", e );
            throw new IOException(e);
        }
        finally
        {
            log.debug("number of file handles is " + file_handles.size());
        }
    }
    
    public long put_DirHandle( FSENode mf ) throws IOException
    {
        try
        {            
            // ARTIFICIAL INDEX, NOT USED IN CLOSE AND ON... FUNCTIONS
            // DOKAN DOESNT DISTINGUISH BETWEEN DIRS AND FILES, WE DO
            long handleNo = getNextFreeDirHandleNo();
            DirHandleEntry entry = new DirHandleEntry( mf, handleNo);            
            dir_handles.put(handleNo, entry);

            return handleNo;
        }
        catch (Exception e)
        {
            log.error("Exception in put_DirHandle: ", e );
            throw new IOException(e);
        }
        finally
        {
            log.debug("number of dir handles is " + dir_handles.size());
        }
    }    
    
    public long put_VirtualFileHandle( VirtualRemoteFileHandle file, FSENode mf ) throws IOException
    {
        try
        {            
            // ARTIFICIAL INDEX, NOT USED IN CLOSE AND ON... FUNCTIONS
            // DOKAN DOESNT DISTINGUISH BETWEEN DIRS AND FILES, WE DO
            long handleNo = getNextFreeVirtualFileHandleNo();
            VirtualFileHandleEntry entry = new VirtualFileHandleEntry( file, mf, handleNo);            
            virtual_file_handles.put(handleNo, entry);

            return handleNo;
        }
        catch (Exception e)
        {
            log.error("Exception in put_VirtualFileHandle: ", e );
            throw new IOException(e);
        }
        finally
        {
            log.debug("number of virtual handles is " + virtual_file_handles.size());
        }
    }
    
    long getNextFreeDirHandleNo()
    {
        long maxId = DIROFFSET;
        Collection<DirHandleEntry> col = dir_handles.values();
        for (DirHandleEntry dirHandleEntry : col)
        {
            if (maxId < dirHandleEntry.getHandle())
            {
                maxId = dirHandleEntry.getHandle();
            }            
        }
        return maxId + 1;        
    }

    long getNextFreeVirtualFileHandleNo()
    {
        long maxId = VIRTUALFSOFFSET;
        Collection<VirtualFileHandleEntry> col = virtual_file_handles.values();
        for (VirtualFileHandleEntry entry : col)
        {
            if (maxId < entry.getHandle())
            {
                maxId = entry.getHandle();
            }            
        }
        return maxId + 1;        
    }

    public void cleanup_FileHandle(  long handleNo ) throws IOException
    {
        try
        {
            FileHandleEntry entry = get_FileHandleEntry(handleNo);
            if (entry != null)
            {
                FileHandle fh = entry.fh;
                fh.close();
                if (entry.isForWrite())
                    removeNodeEntry( entry.node );                
                log.debug(" ->cleanup FileChannel " + handleNo);
            }
            else
            {
                log.debug(" ->FileHandle not found " + handleNo);
            }
            
        }
        catch (Exception e)
        {
            log.error("Exception in close_FileHandle: ", e );
            throw new IOException(e);
        }
        finally
        {
            
        }
    }


    public void close_FileHandle(  long handleNo ) throws IOException
    {
        try
        {
            FileHandleEntry entry = remove_FileHandleEntry(handleNo);
            if (entry != null)
            {
                           
                log.debug(" ->closed FileChannel " + handleNo);
            }
            else
            {
                log.debug(" ->FileHandle not found " + handleNo);
            }
            
        }
        catch (Exception e)
        {
            log.error("Exception in close_FileHandle: ", e );
            throw new IOException(e);
        }
       
    }
    public void cleanup_DirHandle(  long handleNo ) throws IOException
    {
       
    }
    public void close_DirHandle(  long handleNo ) throws IOException
    {
        try
        {
            DirHandleEntry entry = dir_handles.remove(handleNo);
            if (entry != null)
            {
                //removeNodeEntry( entry.node );
                log.debug(" ->closed DirHandle " + entry.toString());
            }
            else
            {
                log.debug(" ->DirHandle not found " + handleNo);
            }
        }
        catch (Exception e)
        {
            log.error("Exception in close_DirHandle: " , e );
            throw new IOException(e);
        }
        finally
        {
            log.debug("number of dir handles is " + dir_handles.size());
        }
    }
    public void cleanup_Handle(  long handleNo ) throws IOException
    {
        if (isDirHandle(handleNo))
            cleanup_DirHandle(handleNo);
        else
            cleanup_FileHandle(handleNo);
    }
    public void close_Handle(  long handleNo ) throws IOException
    {
        if (isDirHandle(handleNo))
            close_DirHandle(handleNo);
        else
            close_FileHandle(handleNo);
    }
    public FileHandle get_handle_by_info( FUSEFileInfo fh )
    {
        try
        {
            FileHandleEntry fhe = get_FileHandleEntry(fh.fh);
            return fhe.fh;
        }
        catch (Exception f)
        {
            return null;
        }
    }
    
    public FileHandle get_handle_by_handleNo( long handleNo ) throws IOException
    {
        try
        {
            FileHandleEntry fhe = get_FileHandleEntry(handleNo);
            return fhe.fh;
        }
        catch (Exception f)
        {
            return null;
        }

    }
    public FSENode get_fsenode_by_handleNo( long handleNo )
    {
        try
        {
            if (isDirHandle(handleNo))
            {
                DirHandleEntry fhe = get_DirHandleEntry(handleNo);
                return fhe.getNode();
            }
            else
            {
                FileHandleEntry fhe = get_FileHandleEntry(handleNo);
                return fhe.getNode();
            }
        }
        catch (Exception f)
        {
            log.error("cannot resolve handleNo " + handleNo, f );
            return null;
        }
    }

    // TODO CLEANUP
    public void addNodeCache( FSENode node, String path )
    {
        log.debug("added to cache " + path );
        nodeMap.put(path, node);
    }
    public FSENode getNodeCache(String path )
    {
        FSENode node = nodeMap.get(path);
        if (node != null)
            log.debug("got from cache " + path );
        
        return node;
    }
    public FSENode clearNodeCache(String path )
    {
        log.debug("Cleared cache for " + path );
        return nodeMap.remove(path);
    }

    private void removeNodeEntry( FSENode node )
    {
        for (Map.Entry<String, FSENode> entry : nodeMap.entrySet())
        {
            String path = entry.getKey();
            FSENode _node = entry.getValue();
            if (node.equals( _node)) 
            {
                nodeMap.remove(path);
                break;
            }            
        }        
    }
}
