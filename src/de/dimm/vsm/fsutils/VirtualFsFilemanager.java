/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.fsutils;

import de.dimm.vsm.VSMFSLogger;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @author Administrator
 */
public class VirtualFsFilemanager
{
    static Map<String,VirtualFSFile> fileList = new ConcurrentHashMap<>();
    
    public static void addFile(String path, VirtualFSFile file)
    {
        fileList.put(path, file);
        VSMFSLogger.getLog().debug("VirtualFsFilemanager add " + path + " size " + fileList.size());
    }
    public static VirtualFSFile getFile(String path)
    {
        return fileList.get(path);
    }
    public static VirtualFSFile removeFile(String path)
    {
        VirtualFSFile ret =  fileList.remove(path);
        VSMFSLogger.getLog().debug("VirtualFsFilemanager rem " + path +" size " + fileList.size());
        return ret;
    }    
    
}
