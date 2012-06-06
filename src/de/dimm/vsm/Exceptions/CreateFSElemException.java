/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.Exceptions;

import de.dimm.vsm.records.AbstractStorageNode;
import de.dimm.vsm.records.FileSystemElemNode;
import java.io.File;
import java.io.IOException;

/**
 *
 * @author mw
 */
public class CreateFSElemException extends IOException
{

    /**
     * Creates a new instance of <code>PathResolveException</code> without detail message.
     */
    public CreateFSElemException() {
    }


    /**
     * Constructs an instance of <code>PathResolveException</code> with the specified detail message.
     * @param msg the detail message.
     */
    public CreateFSElemException(File f, AbstractStorageNode node, String msg )
    {
        super("Cannot create " + f + " in node " + node.getName() + ": " + msg );
    }
    public CreateFSElemException(File f, AbstractStorageNode node )
    {
        super("Cannot create " + f + " in node " + node.getName() );
    }
    public CreateFSElemException(String msg )
    {
        super(msg );
        printStackTrace();
    }
/*
    public CreateFSElemException( File f, FSENodeInterface node, String msg )
    {
        super("Cannot create " + f + " in node " + node.get_name() + ": " + msg );
    }
    public CreateFSElemException( File f, FSENodeInterface node)
    {
        super("Cannot create " + f + " in node " + node.get_name());
    }*/

    public CreateFSElemException( FileSystemElemNode node, Exception e )
    {
        super("Cannot create " + node.getName() + ": " + e.getLocalizedMessage());
    }
}
