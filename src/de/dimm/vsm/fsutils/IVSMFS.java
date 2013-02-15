/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.fsutils;

/**
 *
 * @author Administrator
 */
public interface IVSMFS
{
    void setShutdownHook(ShutdownHook hook );
    ShutdownHook getShutdownHook();
    public boolean mount();
    public boolean unmount();
}
