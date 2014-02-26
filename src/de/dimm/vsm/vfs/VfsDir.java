/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.vfs;

import de.dimm.vsm.fsutils.RemoteStoragePoolHandler;
import de.dimm.vsm.net.RemoteFSElem;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author root
 */
public class VfsDir extends VfsFile implements IVfsDir
{
    List<IVfsFsEntry>children;
    IVfsHandler vfsHandler;

    public VfsDir( IVfsDir parent, String path, RemoteFSElem elem, RemoteStoragePoolHandler remoteFSApi, IVfsHandler vfsHandler )
    {
        super( parent, path, elem, remoteFSApi );
        this.vfsHandler = vfsHandler;
    }

    
    @Override
    public List<IVfsFsEntry> listChildren() throws IOException, SQLException
    {
        if (isRealized())
            return children;
        
        
        children = new ArrayList<>();
        List<RemoteFSElem> list = remoteFSApi.get_child_nodes( elem );
        for (RemoteFSElem remoteFSElem : list)
        {
            String parentPath = getPath();
            if (!parentPath.equals( "/"))
                parentPath += "/";
            if (remoteFSElem.isDirectory())
                children.add( new VfsDir( this, parentPath + remoteFSElem.getName(), remoteFSElem, remoteFSApi, vfsHandler) );
            else
                children.add(  VfsFile.createFile( this, parentPath + remoteFSElem.getName(), remoteFSElem, remoteFSApi, vfsHandler.getBlockRunner()) );
                
        }
//        for (IVfsFsEntry entry : children)
//        {
//            vfsHandler.addToEntryMap( entry );
//        }
        return children;
    }

    @Override
    public boolean isRealized()
    {
        return children != null;
    }

    @Override
    public String toString()
    {
        return "Dir " + getPath();
    }

    @Override
    public List<IVfsFsEntry> getChildren()
    {
        return children;
    }

    @Override
    public void addChild( IVfsFsEntry entry )
    {
        if (isRealized())
        {
            children.add( entry );
        }
    }

    @Override
    public boolean removeChild( IVfsFsEntry entry )
    {
        return children.remove( entry);
    }

    @Override
    public void unrealize()
    {
        children.clear();
        children  = null;
    }

    @Override
    public IVfsFsEntry getChild( String dpath ) throws IOException, SQLException
    {
        List<IVfsFsEntry> list = listChildren();
        for (IVfsFsEntry iVfsFsEntry : list)
        {
            if (iVfsFsEntry.getName().equals( dpath))
                return iVfsFsEntry;
        }
        return null;
    }
    
    
}
