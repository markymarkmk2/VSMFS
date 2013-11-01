/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.vfs;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;


/**
 *
 * @author root
 */
public interface IVfsDir extends IVfsFsEntry
{
    List<IVfsFsEntry> listChildren() throws IOException, SQLException;
    boolean isRealized();

    public List<IVfsFsEntry> getChildren();

    public void addChild( IVfsFsEntry entry );

    public void unrealize();

    public boolean removeChild( IVfsFsEntry aThis );

    public IVfsFsEntry getChild( String dpath ) throws IOException, SQLException;
    
}
