/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.vfs;

import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.Exceptions.PoolReadOnlyException;
import static de.dimm.vsm.vfs.VfsWriteBlockRunner.blocksize;
import java.io.IOException;
import java.sql.SQLException;

/**
 *
 * @author Administrator
 */
public interface IWriteBlockRunner {

    boolean flushAndWait();

    void write_block( long handleNo, VfsBlock vfsBlock ) throws IOException, SQLException, PoolReadOnlyException, PathResolveException;
    
    public  void close(); 
    
    public VfsBlock getNewFullBlock( long offset, int len );
    public VfsBlock getNewFullBlock( long offset, int len, byte[] data);

    public VfsBlock getNewFullBlock( long offset, int len, int valid_len );
    public VfsBlock getNewFullBlock( long offset, int len, int valid_len, byte[] data);
    public void returnFullBlock( VfsBlock block );
    public String printStatistics();
}
