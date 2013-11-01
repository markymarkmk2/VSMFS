/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.vfs;

import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.Exceptions.PoolReadOnlyException;
import de.dimm.vsm.VSMFSLogger;
import de.dimm.vsm.fsutils.RemoteStoragePoolHandler;
import de.dimm.vsm.net.RemoteFSElem;
import de.dimm.vsm.records.FileSystemElemNode;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author root
 */
public class VfsHandler implements IVfsHandler
{
    RemoteStoragePoolHandler remoteFSApi;
    IVfsDir rootDir;
    private static final long DIR_HANDLE_OFFSET = 0x40000000;
    
    //Map<String,IVfsFsEntry> entryMap;
    List<IVfsFsEntry> openList;
    List<IVfsFsEntry> openDirs;

    public VfsHandler( RemoteStoragePoolHandler remoteFSApi) throws SQLException
    {
        this.remoteFSApi = remoteFSApi;
        RemoteFSElem elem = remoteFSApi.resolve_node( "/");
        
        this.rootDir = new VfsDir( null, "/", elem, remoteFSApi, this);
        //entryMap = new HashMap<>();
        openList = new ArrayList<>();
        openDirs = new ArrayList<>();
        //addToEntryMap(rootDir);
    }
    
    
   


    @Override
    public IVfsFsEntry getEntry( String path ) throws IOException, SQLException
    {
        if (path.equals( "/"))
            return rootDir;
        
        IVfsFsEntry ret = null;
        IVfsFsEntry act = rootDir;
        String[] pathArr = path.split( "/");
        for (int i = 0; i < pathArr.length; i++)
        {
            String dpath = pathArr[i];
            if (dpath.isEmpty())
                continue;
            
            if (act.isDirectory())
            {
                if (act instanceof IVfsDir)
                {
                    IVfsDir cdir = (IVfsDir)act;
                    act = cdir.getChild(dpath);
                    // Last entry in Pathlist?
                    if (i == pathArr.length - 1)
                        ret = act;
                }
                else
                {
                    throw new IOException("FileisDir");
                }
            }
            else
            {
                break;
            }
        }
        return ret; //getFromEntryMap(path);
    }

    @Override
    public IVfsDir getParent( String path )  throws IOException, SQLException
    {
        if (path.equals( "/"))
            return null;
        int lastSlashIdx = path.lastIndexOf( "/");
        if (lastSlashIdx == 0)
            return rootDir;
        
        String parentPath = path.substring( 0, lastSlashIdx );
        IVfsFsEntry ret =  getEntry( parentPath );
        if (ret instanceof IVfsDir)
            return (IVfsDir)ret;
        
        return null;
    }

    @Override
    public void closeAll()
    {
        for (IVfsFsEntry entry : openList)
        {
            try
            {
                entry.close();
            }
            catch (Exception iOException)
            {
                VSMFSLogger.getLog().error( "Objekt war schon geschlossen:"  + entry.toString(), iOException);
            }
        }
        openList.clear();
        openDirs.clear();
    }

    @Override
    public void closeEntry(IVfsFsEntry entry)
    {
        if (entry.isFile()) 
        {
            if (!openList.remove( entry))
                VSMFSLogger.getLog().error( "Objekt war nicht in Liste :"  + entry.toString());
            try
            {
                entry.close();
            }
            catch (IOException iOException)
            {
                VSMFSLogger.getLog().error( "Objekt war schon geschlossen:"  + entry.toString(), iOException);
            }
        }
        else
        {
            if (!openDirs.remove( entry))
                VSMFSLogger.getLog().error( "Objekt war nicht in Liste :"  + entry.toString());
        }
    }

    @Override
    public IVfsFsEntry createFileEntry( String pathStr, int posixMode ) throws IOException, SQLException, PathResolveException, PoolReadOnlyException
    {
        IVfsDir parent = getParent(pathStr);
        if (parent == null)
            throw new IOException( "No Parent Node fpound for " + pathStr);
        
        RemoteFSElem elem = remoteFSApi.create_fse_node( pathStr, FileSystemElemNode.FT_FILE);
        IVfsFsEntry entry =  VfsFile.createFile( parent, pathStr, elem, remoteFSApi );
        parent.addChild(entry);
        
        //entryMap.put( pathStr, entry );
        
        VSMFSLogger.getLog().debug("createFileEntry " + entry.toString());
        return entry;
    }

    @Override
    public long openEntryForWrite( IVfsFsEntry entry ) throws IOException, SQLException, PoolReadOnlyException, PathResolveException
    {
        long handleNo = remoteFSApi.open_file_handle_no( entry.getNode(), true);
        entry.setHandleNo(handleNo);
        openList.add( entry );
        VSMFSLogger.getLog().debug("openEntryForWrite " + handleNo + " " + entry.toString());
        return handleNo;
    }

    @Override
    public void mkdir( String pathStr, int posixMode ) throws IOException, SQLException, PathResolveException, PoolReadOnlyException
    {
        IVfsDir parent = getParent(pathStr);
        if (parent == null)
            throw new IOException( "No Parent Node fpound for " + pathStr);

        RemoteFSElem elem = remoteFSApi.create_fse_node( pathStr, FileSystemElemNode.FT_DIR);
        IVfsFsEntry entry =  new VfsDir( parent, pathStr, elem, remoteFSApi, this );
        parent.addChild(entry);
        
        //entryMap.put( pathStr, entry );
        VSMFSLogger.getLog().debug("mkdir " + entry.toString());    }

    @Override
    public long openEntry(IVfsFsEntry entry ) throws IOException, SQLException, PoolReadOnlyException, PathResolveException
    {   
        long handleNo;
        if (entry.isFile())
        {
            handleNo = remoteFSApi.open_file_handle_no( entry.getNode(), false);
            entry.setHandleNo(handleNo);
            openList.add( entry );
        }
        else
        {
            openDirs.add( entry );
            handleNo = DIR_HANDLE_OFFSET + openDirs.size();
            
        }
        VSMFSLogger.getLog().debug("openEntry " + handleNo + " " + entry.toString());
        return handleNo;
    }

    @Override
    public IVfsFsEntry getEntryByHandle( long fh )
    {
        for (IVfsFsEntry entry : openList)
        {
            if (entry.getHandleNo() == fh)
                return entry;
        }
        return null;
    }

    @Override
    public void moveNode( String f, String t ) throws IOException, SQLException, PoolReadOnlyException, PathResolveException
    {
        remoteFSApi.move_fse_node( f, t );
//        IVfsFsEntry entry = getFromEntryMap(f);
//        if (entry != null)
//        {
//            removeFromEntryMap(entry);
//        }
        IVfsDir fparent = getParent(f);
        if (fparent != null)
        {
            fparent.unrealize();
        }
        IVfsDir tparent = getParent(t);
        if (tparent != null )
        {
            tparent.unrealize();
        }
    }

    @Override
    public boolean removeDir( IVfsDir entry ) throws PoolReadOnlyException, SQLException, IOException
    {
        // Check if empty
        if (!entry.isRealized())
        {
            entry.listChildren();
        }
        if (!entry.getChildren().isEmpty())
            return false;
                    
        boolean ret = remoteFSApi.remove_fse_node( entry.getPath());
        if (ret)
        {
            //removeFromEntryMap( entry );
            entry.removeFromParent();        
        }
        return ret;
    }

    @Override
    public boolean unlink( IVfsFsEntry entry )throws PoolReadOnlyException, SQLException, IOException
    {
        boolean ret =  remoteFSApi.remove_fse_node(  entry.getPath() );
        if (ret)
        {
            //removeFromEntryMap( entry );
            entry.removeFromParent();
        }
        return ret;
    }
//
//    private IVfsFsEntry getFromEntryMap( String path )
//    {
//        return entryMap.get( path );
//    }
//    @Override
//    public final void addToEntryMap( IVfsFsEntry entry )
//    {
//        entryMap.put( entry.getPath(), entry);
//    }
//    
//    @Override
//    public final void removeFromEntryMap(IVfsFsEntry entry)
//    {
//        entryMap.remove( entry.getPath() );
//        if (entry.isDirectory())
//        {
//            removeChildrenFromEntryMap((IVfsDir)entry);
//        }                    
//    }
//
//    private void removeChildrenFromEntryMap( IVfsDir entry )
//    {
//        if (entry.isRealized()) 
//        {
//            List<IVfsFsEntry> list = entry.getChildren();
//            for (IVfsFsEntry child : list)
//            {
//                removeFromEntryMap(child);
//            }
//        }
//    }

    
}
