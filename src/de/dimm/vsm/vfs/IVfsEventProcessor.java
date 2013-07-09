/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.vfs;

import de.dimm.vsm.net.RemoteFSElem;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.List;

/**
 *
 * @author Administrator
 */
public interface IVfsEventProcessor
{

    boolean process( List<RemoteFSElem> elems )  throws IOException;
    
    long startProcess( List<RemoteFSElem> elems )  throws IOException;
    boolean waitProcess( long id, int timout )  throws IOException;
    boolean fetchResult( long id )  throws IOException;
    void abortProcess( long id );
}
