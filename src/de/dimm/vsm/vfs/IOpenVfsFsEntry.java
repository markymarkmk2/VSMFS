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
 * @author Administrator
 */
public interface IOpenVfsFsEntry {

    void flush() throws IOException, SQLException, PoolReadOnlyException, PathResolveException;

    IVfsFsEntry getEntry();

    long getHandleNo();

    boolean isDeleteOnClose();

    boolean isForWrite();

    boolean isReadOnly() throws IOException, SQLException, PoolReadOnlyException, PathResolveException;

    byte[] read( long offset, int capacity ) throws IOException, SQLException, PoolReadOnlyException, PathResolveException;

    void setDeleteOnClose( boolean deleteOnClose );

    void setForWrite( boolean forWrite );

    void setMsTimes( long ctime, long atime, long mtime ) throws IOException, SQLException, PoolReadOnlyException, PathResolveException;

    void truncate( long length ) throws IOException, SQLException, PoolReadOnlyException, PathResolveException;

    void write( long offset, int length, byte[] b ) throws IOException, SQLException, PoolReadOnlyException, PathResolveException;
    
    void close() throws IOException;
    
}
