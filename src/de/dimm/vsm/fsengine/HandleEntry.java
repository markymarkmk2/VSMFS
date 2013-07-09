/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.fsengine;

import de.dimm.vsm.fsutils.FSENode;
import java.io.IOException;

/**
 *
 * @author Administrator
 */
public abstract class HandleEntry
{
    protected FSENode node;
    protected long handle;

    public HandleEntry(  FSENode node )
    {
        this.node = node;
    }

    @Override
    public String toString()
    {
        return node.get_path();
    }

    public FSENode getNode()
    {
        return node;
    }

    abstract public long getHandle();
    abstract public void close() throws IOException;
    
}
