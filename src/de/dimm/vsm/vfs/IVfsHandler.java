/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.vfs;

import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.Exceptions.PoolReadOnlyException;
import java.io.IOException;
import java.sql.SQLException;

/**
 *
 * @author root
 */
public interface IVfsHandler
{
    
    IVfsFsEntry getEntry( String path) throws IOException, SQLException;
    IVfsFsEntry getParent( String path) throws IOException, SQLException;

    public void closeAll();
    public void closeEntry(OpenVfsFsEntry entry );

    public IVfsFsEntry createFileEntry( String pathStr, int posixMode )  throws IOException, SQLException, PathResolveException, PoolReadOnlyException;

    public long openEntryForWrite( IVfsFsEntry entry )  throws IOException, SQLException,  PoolReadOnlyException, PathResolveException;

    public void mkdir( String pathStr, int posixMode )  throws IOException, SQLException, PathResolveException, PoolReadOnlyException;

    public long openEntry( IVfsFsEntry entry  ) throws IOException, SQLException, PoolReadOnlyException, PathResolveException;

    public OpenVfsFsEntry getEntryByHandle( long fh );

    public void moveNode( String f, String t ) throws IOException,SQLException, PoolReadOnlyException, PathResolveException;

    public boolean removeDir( IVfsDir entry ) throws IOException,SQLException, PoolReadOnlyException;

    public boolean unlink( IVfsFsEntry entry ) throws IOException,SQLException, PoolReadOnlyException;

//    void addToEntryMap( IVfsFsEntry entry );
//
//    void removeFromEntryMap( IVfsFsEntry entry );
    
    
    
}
