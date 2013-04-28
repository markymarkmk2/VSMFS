/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.fsengine;

import de.dimm.vsm.fsutils.FSENode;
import de.dimm.vsm.net.interfaces.FileHandle;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.log4j.Logger;
import org.catacombae.jfuse.types.fuse26.FUSEFileInfo;

/**
 *
 * @author Administrator
 */
public class HandleResolver
{
    public static final long DIROFFSET = 4000000001L;
    long newDirHandleIdx = 1;

    final HashMap<Long,FileHandleEntry> file_handles = new HashMap<Long,FileHandleEntry>();
    final HashMap<Long,DirHandleEntry> dir_handles = new HashMap<Long,DirHandleEntry>();
    private ReentrantLock channelLock = new ReentrantLock();


    ConcurrentHashMap<String,FSENode> nodeMap = new ConcurrentHashMap<String, FSENode>();

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
                iter.next().fh.close();
            }
            catch (IOException e)
            {
            }
        }
    }

    public boolean isDirHandle( long handleNo )
    {
        return (handleNo >= DIROFFSET);
    }

    public FileHandleEntry get_FileHandleEntry( long handle ) throws IOException
    {
        try
        {
            channelLock.lock();

            return file_handles.get( handle );
        }
        catch (Exception e)
        {
            log.error("Exception in get_FileHandleEntry: " + e.getMessage() );
            e.printStackTrace();
            throw new IOException(e);
        }
        finally
        {
            channelLock.unlock();
        }

    }

    public DirHandleEntry get_DirHandleEntry( long handle ) throws IOException
    {
        try
        {
            channelLock.lock();

            return dir_handles.get( handle );
        }
        catch (Exception e)
        {
            log.error("Exception in get_DirHandleEntry: " + e.getMessage() );
            e.printStackTrace();
            throw new IOException(e);
        }
        finally
        {
            channelLock.unlock();
        }

    }

    public long put_FileHandle( FSENode mf, FileHandle handle ) throws IOException
    {
        try
        {
            channelLock.lock();

            long handleNo = mf.getNode().getFileHandle();
            FileHandleEntry entry = new FileHandleEntry(handle, mf);
            file_handles.put(handleNo, entry);

            return handleNo;
        }
        catch (Exception e)
        {
            log.error("Exception in get_FileHandle: " + e.getMessage() );
            e.printStackTrace();
            throw new IOException(e);
        }
        finally
        {
            log.debug("number of file handles is " + file_handles.size());
            channelLock.unlock();
        }
    }
    public long put_DirHandle( FSENode mf ) throws IOException
    {
        try
        {
            channelLock.lock();
            
            // ARTIFICIAL INDEX, NOT USED IN CLOSE AND ON... FUNCTIONS
            // DOKAN DOESNT DISTINGUISH BETWEEN DIRS AND FILES, WE DO

            long handleNo = DIROFFSET + newDirHandleIdx++;
            DirHandleEntry entry = new DirHandleEntry( mf);
            dir_handles.put(handleNo, entry);

            return handleNo;
        }
        catch (Exception e)
        {
            log.error("Exception in get_FileHandle: " + e.getMessage() );
            e.printStackTrace();
            throw new IOException(e);
        }
        finally
        {
            log.debug("number of file handles is " + file_handles.size());
            channelLock.unlock();
        }
    }



    public void close_FileHandle(  long handleNo ) throws IOException
    {
        try
        {
            channelLock.lock();
            FileHandleEntry entry = file_handles.remove(handleNo);
            if (entry != null)
            {
                FileHandle fh = entry.fh;
                fh.close();
                log.debug(" ->closed FileChannel " + handleNo);
            }
        }
        catch (Exception e)
        {
            log.error("Exception in close_FileHandle: " + e.getMessage() );
            e.printStackTrace();
            throw new IOException(e);
        }
        finally
        {
            log.debug("number of file handles is " + file_handles.size());
            channelLock.unlock();
        }
    }
    public void close_DirHandle(  long handleNo ) throws IOException
    {
        try
        {
            channelLock.lock();
            DirHandleEntry entry = dir_handles.remove(handleNo);
            if (entry != null)
            {
                log.debug(" ->closed DirHandle " + entry.toString());
            }
        }
        catch (Exception e)
        {
            log.error("Exception in close_FileHandle: " + e.getMessage() );
            e.printStackTrace();
            throw new IOException(e);
        }
        finally
        {
            log.debug("number of file handles is " + file_handles.size());
            channelLock.unlock();
        }
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
        catch (IOException f)
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
        catch (IOException f)
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
            f.printStackTrace();
            log.error("cannot resolve handleNo " + handleNo );
            return null;
        }
    }

    // TODO CLEANUP
    public void addNodeCache( FSENode node, String path )
    {
        nodeMap.put(path, node);
    }
    public FSENode getNodeCache(String path )
    {
        return nodeMap.get(path);
    }
    public FSENode clearNodeCache(String path )
    {
        return nodeMap.remove(path);
    }
}
