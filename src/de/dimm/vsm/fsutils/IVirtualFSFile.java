/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.fsutils;

import de.dimm.vsm.net.RemoteFSElem;
import java.io.IOException;

/**
 *
 * @author Administrator
 */
public interface IVirtualFSFile
{

    RemoteFSElem getElem();

    void abort();

    boolean closeRead();

    void closeWrite() throws IOException;

    void create();

    boolean delete();

    boolean exists();

    boolean existsBlock( long offset, int size );

    void force( boolean b );

    long length();

    int read( byte[] b, int length, long offset );

    byte[] read( int length, long offset );

    byte[] fetchBlock( long offset, int size) throws IOException;

    void writeFile( byte[] b, int length, long offset ) throws IOException;
    
    void truncateFile( long size );
    
}
